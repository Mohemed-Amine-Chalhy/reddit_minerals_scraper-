"""Network-free read repositories for bounded web datasets."""

from __future__ import annotations

import math
import re
from collections import Counter, defaultdict
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any, Literal, Protocol

from pydantic import Field

from reddit_minerals.models import ContentKind, MiningStance, Sentiment
from reddit_minerals.web.demo_data import (
    DATASET_GENERATED_AT,
    SYNTHETIC_PROVENANCE,
    synthetic_records,
    synthetic_runs,
)
from reddit_minerals.web.models import (
    ApiModel,
    ConcernMetric,
    DashboardResponse,
    DatasetCounts,
    DatasetDateRange,
    DatasetKind,
    DatasetProvenance,
    DatasetTotals,
    FeatureFlags,
    FilterConfig,
    LabelCount,
    MetaResponse,
    MineralMetric,
    PaginationConfig,
    RecordDetail,
    RecordPage,
    RecordSort,
    RunPage,
    RunStatus,
    RunSummary,
    SnapshotResponse,
    UiConfigResponse,
)


class UnsupportedFilterError(ValueError):
    """A filter value does not exist in the selected bounded dataset."""


class ReadRepository(Protocol):
    """Read operations required by the web adapter."""

    @property
    def minerals(self) -> tuple[str, ...]: ...

    @property
    def source(self) -> DatasetProvenance: ...

    def metadata(self, *, application_version: str) -> MetaResponse: ...

    def dashboard(self, *, mineral: str | None) -> DashboardResponse: ...

    def snapshot(self) -> SnapshotResponse: ...

    def list_records(
        self,
        *,
        page: int,
        page_size: int,
        mineral: str | None,
        kind: ContentKind | None,
        sentiment: Sentiment | None,
        query: str | None,
        sort: RecordSort,
    ) -> RecordPage: ...

    def get_record(self, record_id: str) -> RecordDetail | None: ...

    def list_runs(
        self,
        *,
        page: int,
        page_size: int,
        status: RunStatus | None,
        command: str | None,
    ) -> RunPage: ...

    def ui_config(self) -> UiConfigResponse: ...


class _InMemoryReadRepository:
    """Shared immutable query implementation over a validated bounded dataset."""

    def __init__(
        self,
        *,
        source: DatasetProvenance,
        generated_at: datetime,
        records: Sequence[RecordDetail],
        runs: Sequence[RunSummary],
    ) -> None:
        self._source = source
        self._generated_at = generated_at
        self._records = tuple(records)
        self._runs = tuple(runs)
        self._records_by_id = {record.id: record for record in self._records}
        if len(self._records_by_id) != len(self._records):
            raise ValueError("Web dataset record IDs must be unique")
        self._minerals = tuple(sorted({record.mineral for record in self._records}))
        self._validate_source_contract()

    @property
    def minerals(self) -> tuple[str, ...]:
        return self._minerals

    @property
    def source(self) -> DatasetProvenance:
        return self._source

    def metadata(self, *, application_version: str) -> MetaResponse:
        return MetaResponse(
            mode=self._source.kind,
            synthetic=self._is_synthetic,
            public_sample=self._source.public_sample,
            source=self._source,
            application_name="MineralLens",
            application_version=application_version,
            dataset_label=self._source.dataset_label,
            dataset_description=self._source.dataset_description,
            generated_at=self._generated_at,
            minerals=self.minerals,
            totals=self._totals(self._records),
        )

    def dashboard(self, *, mineral: str | None) -> DashboardResponse:
        normalized_mineral = self._validated_mineral(mineral)
        records = tuple(
            record
            for record in self._records
            if normalized_mineral is None or record.mineral == normalized_mineral
        )
        sentiment_counts = Counter(record.analysis.enrichment.sentiment.value for record in records)
        stance_counts = Counter(record.analysis.enrichment.stance.value for record in records)

        concern_scores: dict[str, list[float]] = defaultdict(list)
        for record in records:
            for concern in record.analysis.enrichment.concerns:
                concern_scores[concern.name].append(concern.score)
        concerns = tuple(
            ConcernMetric(
                name=name,
                average_score=round(sum(scores) / len(scores), 3),
                records=len(scores),
            )
            for name, scores in sorted(
                concern_scores.items(),
                key=lambda item: (-sum(item[1]) / len(item[1]), item[0]),
            )[:8]
        )

        metrics: list[MineralMetric] = []
        for name in sorted({record.mineral for record in records}):
            mineral_records = tuple(record for record in records if record.mineral == name)
            relevance = tuple(
                record.analysis.relevance.confidence
                for record in mineral_records
                if record.analysis.relevance is not None
            )
            reputation = tuple(
                record.analysis.reputation.score
                for record in mineral_records
                if record.analysis.reputation is not None
            )
            metrics.append(
                MineralMetric(
                    mineral=name,
                    records=len(mineral_records),
                    posts=sum(record.kind is ContentKind.POST for record in mineral_records),
                    comments=sum(record.kind is ContentKind.COMMENT for record in mineral_records),
                    average_relevance=(
                        round(sum(relevance) / len(relevance), 1) if relevance else None
                    ),
                    average_reputation=(
                        round(sum(reputation) / len(reputation), 1) if reputation else None
                    ),
                )
            )

        recent = tuple(
            record.summary()
            for record in sorted(
                records, key=lambda item: (item.created_at, item.id), reverse=True
            )[:6]
        )
        return DashboardResponse(
            mode=self._source.kind,
            synthetic=self._is_synthetic,
            public_sample=self._source.public_sample,
            source=self._source,
            selected_mineral=normalized_mineral,
            totals=self._totals(records),
            sentiment_distribution=tuple(
                LabelCount(label=sentiment.value, count=sentiment_counts[sentiment.value])
                for sentiment in Sentiment
                if sentiment_counts[sentiment.value]
            ),
            stance_distribution=tuple(
                LabelCount(label=stance.value, count=stance_counts[stance.value])
                for stance in MiningStance
                if stance_counts[stance.value]
            ),
            top_concerns=concerns,
            mineral_metrics=tuple(metrics),
            recent_records=recent,
        )

    def snapshot(self) -> SnapshotResponse:
        """Return the complete bounded presentation dataset without an N+1 fetch."""

        return SnapshotResponse(
            mode=self._source.kind,
            synthetic=self._is_synthetic,
            public_sample=self._source.public_sample,
            source=self._source,
            generated_at=self._generated_at,
            records=self._records,
        )

    def list_records(
        self,
        *,
        page: int,
        page_size: int,
        mineral: str | None,
        kind: ContentKind | None,
        sentiment: Sentiment | None,
        query: str | None,
        sort: RecordSort,
    ) -> RecordPage:
        normalized_mineral = self._validated_mineral(mineral)
        normalized_query = query.strip().casefold() if query else None
        records = [
            record
            for record in self._records
            if (normalized_mineral is None or record.mineral == normalized_mineral)
            and (kind is None or record.kind is kind)
            and (sentiment is None or record.analysis.enrichment.sentiment is sentiment)
            and (normalized_query is None or self._matches(record, normalized_query))
        ]
        if sort is RecordSort.SCORE:
            records.sort(key=lambda item: (item.score, item.created_at, item.id), reverse=True)
        elif sort is RecordSort.REPUTATION:
            records.sort(
                key=lambda item: (
                    item.analysis.reputation.score if item.analysis.reputation else -1,
                    item.created_at,
                    item.id,
                ),
                reverse=True,
            )
        else:
            records.sort(key=lambda item: (item.created_at, item.id), reverse=True)

        start = (page - 1) * page_size
        selected = records[start : start + page_size]
        return RecordPage(
            mode=self._source.kind,
            synthetic=self._is_synthetic,
            public_sample=self._source.public_sample,
            source=self._source,
            page=page,
            page_size=page_size,
            total=len(records),
            pages=math.ceil(len(records) / page_size) if records else 0,
            items=tuple(record.summary() for record in selected),
        )

    def get_record(self, record_id: str) -> RecordDetail | None:
        return self._records_by_id.get(record_id)

    def list_runs(
        self,
        *,
        page: int,
        page_size: int,
        status: RunStatus | None,
        command: str | None,
    ) -> RunPage:
        normalized_command = command.strip().casefold() if command else None
        available_commands = {run.command.casefold() for run in self._runs}
        if (
            normalized_command is not None
            and available_commands
            and normalized_command not in available_commands
        ):
            raise UnsupportedFilterError("Unknown run command")
        runs = [
            run
            for run in self._runs
            if (status is None or run.status is status)
            and (normalized_command is None or run.command.casefold() == normalized_command)
        ]
        runs.sort(key=lambda item: (item.started_at, item.id), reverse=True)
        start = (page - 1) * page_size
        return RunPage(
            mode=self._source.kind,
            synthetic=self._is_synthetic,
            public_sample=self._source.public_sample,
            source=self._source,
            page=page,
            page_size=page_size,
            total=len(runs),
            pages=math.ceil(len(runs) / page_size) if runs else 0,
            items=tuple(runs[start : start + page_size]),
        )

    def ui_config(self) -> UiConfigResponse:
        return UiConfigResponse(
            mode=self._source.kind,
            synthetic=self._is_synthetic,
            public_sample=self._source.public_sample,
            source=self._source,
            dataset_label=self._source.dataset_label,
            features=FeatureFlags(run_history=bool(self._runs)),
            pagination=PaginationConfig(default_page_size=12, maximum_page_size=50),
            filters=FilterConfig(
                minerals=self.minerals,
                content_kinds=tuple(ContentKind),
                sentiments=tuple(Sentiment),
                run_statuses=tuple(RunStatus),
                record_sorts=tuple(RecordSort),
            ),
        )

    def _validated_mineral(self, mineral: str | None) -> str | None:
        if mineral is None:
            return None
        normalized = " ".join(mineral.casefold().split())
        if normalized not in self._minerals:
            raise UnsupportedFilterError("Unknown mineral")
        return normalized

    def _totals(self, records: Sequence[RecordDetail]) -> DatasetTotals:
        return DatasetTotals(
            minerals=len({record.mineral for record in records}),
            records=len(records),
            posts=sum(record.kind is ContentKind.POST for record in records),
            comments=sum(record.kind is ContentKind.COMMENT for record in records),
            analyses=sum(
                1
                + int(record.analysis.relevance is not None)
                + int(record.analysis.reputation is not None)
                for record in records
            ),
            runs=len(self._runs),
        )

    @staticmethod
    def _matches(record: RecordDetail, query: str) -> bool:
        values = (
            record.id,
            record.mineral,
            record.title or "",
            record.body,
            record.subreddit,
            *record.analysis.enrichment.keywords,
            *record.analysis.enrichment.themes,
        )
        return any(query in value.casefold() for value in values)

    @property
    def _is_synthetic(self) -> bool:
        return self._source.kind is DatasetKind.SYNTHETIC_DEMO

    def _validate_source_contract(self) -> None:
        sample = self._source.sample_counts
        actual = DatasetCounts(
            minerals=len(self._minerals),
            records=len(self._records),
            posts=sum(record.kind is ContentKind.POST for record in self._records),
            comments=sum(record.kind is ContentKind.COMMENT for record in self._records),
        )
        if actual != sample:
            raise ValueError("Loaded records do not match declared sample counts")
        for record in self._records:
            if record.source != self._source:
                raise ValueError("Record provenance does not match repository provenance")
            if record.mode is not self._source.kind:
                raise ValueError("Record mode does not match repository provenance")
            if record.synthetic is not self._is_synthetic:
                raise ValueError("Record source flags do not match repository provenance")
            if self._source.public_sample and record.content_available:
                raise ValueError("Public sample records cannot claim source content is available")
            if record.kind is ContentKind.POST and record.parent_id is not None:
                raise ValueError("Post records cannot declare a parent record")
            if record.kind is ContentKind.COMMENT:
                parent = self._records_by_id.get(record.parent_id or "")
                if parent is None or parent.kind is not ContentKind.POST:
                    raise ValueError("Comment records must reference a loaded post")
                if parent.mineral != record.mineral:
                    raise ValueError("Matched posts and comments must share a mineral")
            if (
                self._source.public_sample
                and re.fullmatch(r"kg-(?:post|comment)-[0-9a-f]{20}", record.id) is None
            ):
                raise ValueError("Public sample IDs must be repository-local hashes")
        if any(run.synthetic is not self._is_synthetic for run in self._runs):
            raise ValueError("Run source flags do not match repository provenance")


class SyntheticReadRepository(_InMemoryReadRepository):
    """Immutable fictional repository retained for isolated replay and tests."""

    def __init__(
        self,
        *,
        records: Sequence[RecordDetail] | None = None,
        runs: Sequence[RunSummary] | None = None,
    ) -> None:
        super().__init__(
            source=SYNTHETIC_PROVENANCE,
            generated_at=DATASET_GENERATED_AT,
            records=records if records is not None else synthetic_records(),
            runs=runs if runs is not None else synthetic_runs(),
        )


class _CanonicalProvenance(ApiModel):
    """Strict on-disk provenance schema for the curated Kaggle sample."""

    kind: Literal["public-research-sample"]
    dataset_label: Annotated[str, Field(min_length=1, max_length=160)]
    dataset_description: Annotated[str, Field(min_length=1, max_length=800)]
    owner_name: Annotated[str, Field(min_length=1, max_length=160)]
    dataset_slug: Annotated[str, Field(pattern=r"^[a-z0-9][a-z0-9-]{1,99}$")]
    dataset_ref: Annotated[str, Field(pattern=r"^[A-Za-z0-9_-]+/[A-Za-z0-9_-]+$")]
    dataset_url: Annotated[str, Field(max_length=2048, pattern=r"^https://")]
    dataset_version: Annotated[int, Field(ge=1)]
    archive_sha256: Annotated[str, Field(pattern=r"^[0-9A-Fa-f]{64}$")]
    license: Annotated[str, Field(min_length=1, max_length=120)]
    published_at: datetime
    source_note: Annotated[str, Field(min_length=1, max_length=600)]
    published_totals: DatasetCounts
    sample_totals: DatasetCounts
    published_date_range: DatasetDateRange
    sample_method: Annotated[str, Field(min_length=1, max_length=600)]
    raw_text_included: Literal[False]
    authors_included: Literal[False]

    def api_source(self) -> DatasetProvenance:
        return DatasetProvenance(
            kind=DatasetKind.PUBLIC_RESEARCH_SAMPLE,
            public_sample=True,
            dataset_label=self.dataset_label,
            dataset_description=self.dataset_description,
            owner_name=self.owner_name,
            dataset_ref=self.dataset_ref,
            source_url=self.dataset_url,
            dataset_version=str(self.dataset_version),
            archive_sha256=self.archive_sha256,
            license=self.license,
            published_at=self.published_at,
            source_note=self.source_note,
            full_counts=self.published_totals,
            sample_counts=self.sample_totals,
            published_date_range=self.published_date_range,
            sample_method=self.sample_method,
            raw_text_included=self.raw_text_included,
            authors_included=self.authors_included,
        )


class _CanonicalSample(ApiModel):
    """Strict local document loaded by the default public repository."""

    schema_version: Literal[1]
    provenance: _CanonicalProvenance
    records: tuple[dict[str, Any], ...]
    runs: tuple[RunSummary, ...] = ()


KAGGLE_SAMPLE_PATH = Path(__file__).with_name("data") / "kaggle_sample.json"


class KaggleSampleReadRepository(_InMemoryReadRepository):
    """Read a checked-in, curated Kaggle sample without network or database access."""

    def __init__(self, *, sample_path: Path | None = None) -> None:
        selected_path = sample_path if sample_path is not None else KAGGLE_SAMPLE_PATH
        document = _CanonicalSample.model_validate_json(selected_path.read_text(encoding="utf-8"))
        source = document.provenance.api_source()
        records: list[RecordDetail] = []
        for raw_record in document.records:
            payload = dict(raw_record)
            payload.setdefault("mode", source.kind)
            payload.setdefault("public_sample", True)
            payload.setdefault("source", source)
            records.append(RecordDetail.model_validate(payload))
        super().__init__(
            source=source,
            generated_at=document.provenance.published_at,
            records=records,
            runs=document.runs,
        )
