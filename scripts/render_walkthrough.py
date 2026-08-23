#!/usr/bin/env python3
"""Build the silent, captioned MineralLens walkthrough from reviewed screenshots.

The script intentionally accepts only the six named desktop captures in
``docs/media``. It never launches a browser, reads a browser profile, or records
the desktop. Every published product frame therefore comes from an image that a
reviewer can inspect before rendering.
"""

from __future__ import annotations

import argparse
import hashlib
import re
import struct
import subprocess
import tempfile
import textwrap
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from PIL import ImageFont

WIDTH = 1920
HEIGHT = 1080
FPS = 30
VIDEO_NAME = "walkthrough-1080p.mp4"
CAPTION_NAME = "walkthrough.en.srt"
THUMBNAIL_NAME = "walkthrough-thumbnail.png"
EXPECTED_DURATION_SECONDS = 71

BACKGROUND = "#061512"
PANEL = "#0d211c"
TEXT = "#f5f3e9"
MUTED = "#a9b7b1"
GOLD = "#f0c45e"
TEAL = "#62d4c2"


@dataclass(frozen=True, slots=True)
class Scene:
    slug: str
    duration_seconds: int
    eyebrow: str
    title: str
    caption: str
    subtitle_caption: str
    screenshot: str | None = None


SCENES: tuple[Scene, ...] = (
    Scene(
        slug="opening",
        duration_seconds=5,
        eyebrow="RESEARCH SYSTEMS EXPLORER",
        title="MineralLens",
        caption="Research tooling developed during a Mines Nancy internship.",
        subtitle_caption=(
            "MineralLens. Research tooling developed during a Mines Nancy internship."
        ),
    ),
    Scene(
        slug="overview",
        duration_seconds=10,
        eyebrow="PUBLIC RESEARCH SAMPLE",
        title="From released metadata to inspectable signals",
        caption=(
            "A read-only interface connects 104 curated records to transparent, "
            "model-derived research signals."
        ),
        subtitle_caption=(
            "A read-only interface connects 104 curated public records to transparent, "
            "model-derived research signals."
        ),
        screenshot="minerallens-overview.png",
    ),
    Scene(
        slug="signals",
        duration_seconds=10,
        eyebrow="AGGREGATED SIGNALS",
        title="Patterns with explicit caveats",
        caption=(
            "Distributions summarize this bounded sample; model-derived concerns are signals, "
            "not verified events."
        ),
        subtitle_caption=(
            "Distributions summarize this bounded sample. Model-derived concerns are signals, "
            "not verified events."
        ),
        screenshot="minerallens-signals.png",
    ),
    Scene(
        slug="explorer",
        duration_seconds=12,
        eyebrow="TRACEABLE RECORDS",
        title="Follow every signal back to its released metadata",
        caption=(
            "Browse labels and provenance while preserving the public dataset boundary: "
            "no original Reddit text or authors."
        ),
        subtitle_caption=(
            "Browse labels and provenance while preserving the public dataset boundary. "
            "Original Reddit text and authors are not included."
        ),
        screenshot="minerallens-explorer.png",
    ),
    Scene(
        slug="pipeline",
        duration_seconds=12,
        eyebrow="SYNTHETIC REPLAY",
        title="Failure behavior you can inspect",
        caption=(
            "A clearly labelled offline replay demonstrates bounded retry and revision-safe "
            "writes without contacting providers."
        ),
        subtitle_caption=(
            "A clearly labelled synthetic replay demonstrates bounded retry and revision-safe "
            "writes without contacting providers."
        ),
        screenshot="minerallens-pipeline.png",
    ),
    Scene(
        slug="engineering",
        duration_seconds=9,
        eyebrow="ENGINEERING CASE STUDY",
        title="Production boundaries for research software",
        caption=(
            "Typed contracts, deterministic fixtures, tests, and resumable stages make the "
            "system inspectable from edge to core."
        ),
        subtitle_caption=(
            "Typed contracts, deterministic fixtures, tests, and resumable stages make the "
            "system inspectable from edge to core."
        ),
        screenshot="minerallens-engineering.png",
    ),
    Scene(
        slug="quality",
        duration_seconds=8,
        eyebrow="EVIDENCE-BACKED ENGINEERING",
        title="Every claim points to implementation evidence",
        caption=(
            "Tests, cross-platform CI, strict typing, and source-linked decisions make quality "
            "visible."
        ),
        subtitle_caption=(
            "Tests, cross-platform continuous integration, strict typing, and source-linked "
            "decisions make quality visible."
        ),
        screenshot="minerallens-quality.png",
    ),
    Scene(
        slug="closing",
        duration_seconds=5,
        eyebrow="RESEARCH CONTEXT",
        title="Built for reproducible research.",
        caption=(
            "Associated manuscript in advanced pre-publication review, according to the "
            "project owner."
        ),
        subtitle_caption=(
            "The associated manuscript is in advanced pre-publication review, according to "
            "the project owner."
        ),
    ),
)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    repository_root = Path(__file__).resolve().parents[1]
    default_media = repository_root / "docs" / "media"
    parser = argparse.ArgumentParser(
        description="Render the reviewed MineralLens screenshots as a captioned 1080p MP4.",
    )
    parser.add_argument(
        "--media-dir",
        type=Path,
        default=default_media,
        help="Directory containing the six reviewed screenshot inputs.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=default_media,
        help="Directory for the MP4, SRT, and thumbnail.",
    )
    parser.add_argument(
        "--ffmpeg",
        type=Path,
        help="Optional full FFmpeg executable; defaults to imageio-ffmpeg's locked binary.",
    )
    parser.add_argument(
        "--confirm-reviewed",
        action="store_true",
        help=(
            "Confirm the six input screenshots were visually reviewed for accurate source "
            "labels and absence of private/local data. Required to render."
        ),
    )
    parser.add_argument(
        "--check-only",
        action="store_true",
        help="Validate inputs and FFmpeg capabilities without creating artifacts.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Replace existing walkthrough artifacts after the new outputs pass validation.",
    )
    return parser.parse_args(argv)


def _load_media_modules() -> tuple[Any, Any, Any, Any]:
    try:
        from PIL import Image, ImageDraw, ImageFont, ImageOps
    except ImportError as exc:  # pragma: no cover - depends on the caller's environment
        raise SystemExit("Media dependencies are missing. Run: uv sync --group media") from exc
    return Image, ImageDraw, ImageFont, ImageOps


def _resolve_ffmpeg(explicit: Path | None) -> Path:
    if explicit is not None:
        candidate = explicit.expanduser().resolve()
    else:
        try:
            import imageio_ffmpeg
        except ImportError as exc:  # pragma: no cover - depends on the caller's environment
            raise SystemExit("Media dependencies are missing. Run: uv sync --group media") from exc
        candidate = Path(imageio_ffmpeg.get_ffmpeg_exe()).resolve()
    if not candidate.is_file():
        raise SystemExit(f"FFmpeg executable not found: {candidate}")
    return candidate


def _run(
    command: Sequence[str], *, allow_failure: bool = False
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        list(command),
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if result.returncode != 0 and not allow_failure:
        rendered = " ".join(command)
        raise RuntimeError(f"Command failed ({result.returncode}): {rendered}\n{result.stderr}")
    return result


def _validate_ffmpeg(ffmpeg: Path) -> str:
    version = _run((str(ffmpeg), "-hide_banner", "-version")).stdout.splitlines()[0]
    encoder_result = _run((str(ffmpeg), "-hide_banner", "-encoders"))
    encoders = f"{encoder_result.stdout}\n{encoder_result.stderr}"
    if "libx264" not in encoders:
        raise SystemExit(
            "The selected FFmpeg lacks libx264. Use `uv sync --group media` and omit "
            "--ffmpeg, or pass a full FFmpeg build."
        )
    return version


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest().upper()


def _validated_screenshots(media_dir: Path) -> dict[str, Path]:
    Image, _, _, _ = _load_media_modules()
    root = media_dir.expanduser().resolve()
    if not root.is_dir():
        raise SystemExit(f"Media directory does not exist: {root}")

    screenshots: dict[str, Path] = {}
    for scene in SCENES:
        if scene.screenshot is None:
            continue
        candidate = (root / scene.screenshot).resolve()
        if candidate.parent != root or candidate.name != scene.screenshot:
            raise SystemExit(f"Screenshot escaped the media directory: {scene.screenshot}")
        if not candidate.is_file() or candidate.is_symlink():
            raise SystemExit(f"Missing regular screenshot: {candidate}")
        with Image.open(candidate) as image:
            image.verify()
        with Image.open(candidate) as image:
            width, height = image.size
            if width < 1280 or height < 720:
                raise SystemExit(f"Screenshot is below 1280x720: {candidate} ({width}x{height})")
            ratio = width / height
            if not 1.72 <= ratio <= 1.82:
                raise SystemExit(f"Screenshot is not a desktop 16:9 capture: {candidate}")
        screenshots[scene.screenshot] = candidate
    return screenshots


def _find_font(ImageFont: Any, *, bold: bool, size: int) -> ImageFont.FreeTypeFont:
    windows = Path("C:/Windows/Fonts")
    candidates = (
        windows / ("seguisb.ttf" if bold else "segoeui.ttf"),
        Path("/System/Library/Fonts/SFNS.ttf"),
        Path(
            "/usr/share/fonts/truetype/dejavu/"
            + ("DejaVuSans-Bold.ttf" if bold else "DejaVuSans.ttf")
        ),
        Path(
            "/usr/share/fonts/truetype/liberation2/"
            + ("LiberationSans-Bold.ttf" if bold else "LiberationSans-Regular.ttf")
        ),
    )
    for candidate in candidates:
        if candidate.is_file():
            try:
                return ImageFont.truetype(str(candidate), size=size)
            except OSError:
                continue
    raise SystemExit("No supported TrueType font found for walkthrough rendering.")


def _wrap(draw: Any, text: str, font: Any, *, max_width: int) -> list[str]:
    words = text.split()
    lines: list[str] = []
    current: list[str] = []
    for word in words:
        proposed = " ".join((*current, word))
        if current and draw.textlength(proposed, font=font) > max_width:
            lines.append(" ".join(current))
            current = [word]
        else:
            current.append(word)
    if current:
        lines.append(" ".join(current))
    return lines


def _draw_multiline(
    draw: Any,
    position: tuple[int, int],
    text: str,
    font: Any,
    *,
    fill: str,
    max_width: int,
    spacing: int,
) -> int:
    x, y = position
    lines = _wrap(draw, text, font, max_width=max_width)
    for line in lines:
        draw.text((x, y), line, font=font, fill=fill)
        box = draw.textbbox((x, y), line, font=font)
        y += box[3] - box[1] + spacing
    return y


def _draw_brand_mark(draw: Any, *, center: tuple[int, int], scale: int) -> None:
    x, y = center
    draw.rounded_rectangle(
        (x - scale, y - scale, x + scale, y + scale),
        radius=scale // 3,
        fill=PANEL,
        outline="#24443a",
        width=max(2, scale // 18),
    )
    radius = scale // 3
    draw.polygon(
        ((x, y - radius), (x + radius, y), (x, y + radius), (x - radius, y)),
        fill=GOLD,
    )
    draw.polygon(
        (
            (x - radius, y),
            (x, y + radius),
            (x - radius // 2, y + radius * 2),
            (x - radius * 2, y + radius // 2),
        ),
        fill=TEAL,
    )


def _draw_grid(draw: Any) -> None:
    for x in range(0, WIDTH, 96):
        draw.line((x, 0, x, HEIGHT), fill="#0b241d", width=1)
    for y in range(0, HEIGHT, 96):
        draw.line((0, y, WIDTH, y), fill="#0b241d", width=1)


def _render_card(scene: Scene, destination: Path) -> None:
    Image, ImageDraw, ImageFont, _ = _load_media_modules()
    canvas = Image.new("RGB", (WIDTH, HEIGHT), BACKGROUND)
    draw = ImageDraw.Draw(canvas)
    _draw_grid(draw)
    draw.ellipse((1220, 120, 1840, 740), outline="#173d32", width=3)
    draw.ellipse((1300, 200, 1760, 660), outline="#245044", width=2)
    _draw_brand_mark(draw, center=(1530, 430), scale=120)

    eyebrow_font = _find_font(ImageFont, bold=True, size=27)
    title_font = _find_font(ImageFont, bold=True, size=88 if scene.slug == "opening" else 72)
    body_font = _find_font(ImageFont, bold=False, size=38)
    footer_font = _find_font(ImageFont, bold=False, size=25)
    draw.text((180, 215), scene.eyebrow, font=eyebrow_font, fill=GOLD)
    title_bottom = _draw_multiline(
        draw,
        (180, 290),
        scene.title,
        title_font,
        fill=TEXT,
        max_width=980,
        spacing=12,
    )
    _draw_multiline(
        draw,
        (180, title_bottom + 42),
        scene.caption,
        body_font,
        fill=MUTED,
        max_width=1050,
        spacing=14,
    )
    draw.line((180, 920, 1740, 920), fill="#24443a", width=2)
    draw.text(
        (180, 955),
        "github.com/Mohemed-Amine-Chalhy/reddit_minerals_scraper-",
        font=footer_font,
        fill=TEAL,
    )
    canvas.save(destination, format="PNG", optimize=True)


def _render_screenshot_scene(scene: Scene, source: Path, destination: Path) -> None:
    Image, ImageDraw, ImageFont, ImageOps = _load_media_modules()
    with Image.open(source) as original:
        canvas = ImageOps.fit(
            original.convert("RGB"),
            (WIDTH, HEIGHT),
            method=Image.Resampling.LANCZOS,
            centering=(0.5, 0.5),
        )
    overlay = Image.new("RGBA", (WIDTH, HEIGHT), (0, 0, 0, 0))
    overlay_draw = ImageDraw.Draw(overlay)
    for row in range(180):
        alpha = int(230 * (row / 179) ** 1.5)
        y = 570 + row
        overlay_draw.line((0, y, WIDTH, y), fill=(4, 18, 15, alpha))
    overlay_draw.rectangle((0, 750, WIDTH, HEIGHT), fill=(4, 18, 15, 230))
    overlay_draw.rectangle((0, 0, WIDTH, 8), fill=GOLD)
    canvas = Image.alpha_composite(canvas.convert("RGBA"), overlay)
    draw = ImageDraw.Draw(canvas)
    eyebrow_font = _find_font(ImageFont, bold=True, size=25)
    title_font = _find_font(ImageFont, bold=True, size=47)
    body_font = _find_font(ImageFont, bold=False, size=33)
    draw.rounded_rectangle((88, 765, 540, 818), radius=24, fill="#10251fde")
    draw.text((116, 779), scene.eyebrow, font=eyebrow_font, fill=GOLD)
    draw.text((96, 838), scene.title, font=title_font, fill=TEXT)
    _draw_multiline(
        draw,
        (96, 914),
        scene.caption,
        body_font,
        fill="#d7dfdb",
        max_width=1640,
        spacing=10,
    )
    canvas.convert("RGB").save(destination, format="PNG", optimize=True)


def _render_thumbnail(source: Path, destination: Path) -> None:
    Image, ImageDraw, ImageFont, ImageOps = _load_media_modules()
    size = (1280, 720)
    with Image.open(source) as original:
        canvas = ImageOps.fit(
            original.convert("RGB"),
            size,
            method=Image.Resampling.LANCZOS,
            centering=(0.5, 0.5),
        ).convert("RGBA")
    overlay = Image.new("RGBA", size, (0, 0, 0, 0))
    overlay_draw = ImageDraw.Draw(overlay)
    for column in range(860):
        alpha = int(228 * (1 - column / 860) ** 1.2)
        overlay_draw.line((column, 0, column, size[1]), fill=(4, 18, 15, alpha))
    canvas = Image.alpha_composite(canvas, overlay)
    draw = ImageDraw.Draw(canvas)
    eyebrow_font = _find_font(ImageFont, bold=True, size=24)
    title_font = _find_font(ImageFont, bold=True, size=66)
    body_font = _find_font(ImageFont, bold=False, size=29)
    draw.rounded_rectangle(
        (48, 130, 760, 510),
        radius=34,
        fill=(4, 18, 15, 238),
        outline="#24443a",
        width=2,
    )
    draw.text((72, 170), "MINERALLENS", font=eyebrow_font, fill=GOLD)
    _draw_multiline(
        draw,
        (72, 220),
        "Research systems\nexplorer",
        title_font,
        fill=TEXT,
        max_width=650,
        spacing=6,
    )
    draw.text((74, 420), "71-second product walkthrough", font=body_font, fill="#d7dfdb")
    canvas.convert("RGB").save(destination, format="PNG", optimize=True)


def _format_srt_time(milliseconds: int) -> str:
    hours, remainder = divmod(milliseconds, 3_600_000)
    minutes, remainder = divmod(remainder, 60_000)
    seconds, millis = divmod(remainder, 1_000)
    return f"{hours:02}:{minutes:02}:{seconds:02},{millis:03}"


def _write_srt(destination: Path) -> None:
    entries: list[str] = []
    cursor_ms = 0
    for index, scene in enumerate(SCENES, start=1):
        start = cursor_ms + 400
        cursor_ms += scene.duration_seconds * 1_000
        end = cursor_ms - 400
        caption = "\n".join(
            textwrap.wrap(
                scene.subtitle_caption,
                width=76,
                break_long_words=False,
                break_on_hyphens=False,
            )
        )
        entries.append(
            f"{index}\n{_format_srt_time(start)} --> {_format_srt_time(end)}\n{caption}\n"
        )
    destination.write_text("\n".join(entries), encoding="utf-8", newline="\n")


def _render_segment(ffmpeg: Path, image: Path, scene: Scene, destination: Path) -> None:
    frame_count = scene.duration_seconds * FPS
    fade_duration = 0.35
    fade_out = scene.duration_seconds - fade_duration
    filter_graph = (
        "zoompan="
        "z='min(zoom+0.000035,1.018)':"
        "x='iw/2-(iw/zoom/2)':"
        "y='ih/2-(ih/zoom/2)':"
        f"d={frame_count}:s={WIDTH}x{HEIGHT}:fps={FPS},"
        f"fade=t=in:st=0:d={fade_duration},"
        f"fade=t=out:st={fade_out}:d={fade_duration},"
        "format=yuv420p"
    )
    _run(
        (
            str(ffmpeg),
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-loop",
            "1",
            "-framerate",
            str(FPS),
            "-i",
            str(image),
            "-vf",
            filter_graph,
            "-frames:v",
            str(frame_count),
            "-an",
            "-c:v",
            "libx264",
            "-preset",
            "slow",
            "-crf",
            "19",
            "-pix_fmt",
            "yuv420p",
            "-r",
            str(FPS),
            "-g",
            str(FPS * 2),
            "-movflags",
            "+faststart",
            str(destination),
        )
    )


def _concat_escape(path: Path) -> str:
    return path.resolve().as_posix().replace("'", "'\\''")


def _assemble(ffmpeg: Path, segments: Iterable[Path], destination: Path) -> None:
    segment_list = destination.with_suffix(".concat.txt")
    segment_list.write_text(
        "".join(f"file '{_concat_escape(segment)}'\n" for segment in segments),
        encoding="utf-8",
        newline="\n",
    )
    _run(
        (
            str(ffmpeg),
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            str(segment_list),
            "-c",
            "copy",
            "-movflags",
            "+faststart",
            str(destination),
        )
    )


def _mp4_atom_offsets(path: Path) -> dict[bytes, int]:
    offsets: dict[bytes, int] = {}
    with path.open("rb") as stream:
        file_size = path.stat().st_size
        position = 0
        while position + 8 <= file_size:
            stream.seek(position)
            header = stream.read(8)
            size, kind = struct.unpack(">I4s", header)
            header_size = 8
            if size == 1:
                extended = stream.read(8)
                if len(extended) != 8:
                    break
                size = struct.unpack(">Q", extended)[0]
                header_size = 16
            elif size == 0:
                size = file_size - position
            if size < header_size:
                break
            offsets.setdefault(kind, position)
            position += size
    return offsets


def _validate_video(ffmpeg: Path, video: Path) -> dict[str, str]:
    probe = _run((str(ffmpeg), "-hide_banner", "-i", str(video)), allow_failure=True)
    output = f"{probe.stdout}\n{probe.stderr}"
    duration_match = re.search(r"Duration: (\d+):(\d+):(\d+(?:\.\d+)?)", output)
    if duration_match is None:
        raise RuntimeError("FFmpeg could not report the output duration")
    hours, minutes, seconds = duration_match.groups()
    duration = int(hours) * 3600 + int(minutes) * 60 + float(seconds)
    if abs(duration - EXPECTED_DURATION_SECONDS) > 0.2:
        raise RuntimeError(f"Unexpected duration: {duration:.3f} seconds")
    if "Video: h264" not in output or f"{WIDTH}x{HEIGHT}" not in output:
        raise RuntimeError("Output is not 1920x1080 H.264")
    if re.search(r"\b30(?:\.0+)? fps\b", output) is None:
        raise RuntimeError("Output is not 30 fps")
    if "Audio:" in output:
        raise RuntimeError("Silent walkthrough unexpectedly contains an audio stream")
    atoms = _mp4_atom_offsets(video)
    if b"moov" not in atoms or b"mdat" not in atoms or atoms[b"moov"] > atoms[b"mdat"]:
        raise RuntimeError("MP4 is not faststart optimized")
    return {
        "codec": "h264",
        "resolution": f"{WIDTH}x{HEIGHT}",
        "fps": str(FPS),
        "duration": f"{duration:.2f}s",
        "faststart": "yes",
        "audio": "none",
    }


def _publish(staged: Path, output: Path, *, force: bool) -> None:
    if output.exists() and not force:
        raise SystemExit(f"Refusing to replace existing artifact without --force: {output}")
    try:
        staged.replace(output)
        return
    except PermissionError:
        if not force or not output.exists():
            raise

    # Some Windows filesystems deny replace-over-existing even when neither file
    # is open. Rotate the reviewed artifact out of the way, then restore it if
    # publishing the already-validated staged file fails.
    backup = output.with_name(f".{output.name}.replace-backup")
    if backup.exists():
        raise RuntimeError(f"Refusing to overwrite stale replacement backup: {backup}")
    output.replace(backup)
    try:
        staged.replace(output)
    except Exception:
        backup.replace(output)
        raise
    backup.unlink()


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    media_dir = args.media_dir.expanduser().resolve()
    output_dir = args.output_dir.expanduser().resolve()
    screenshots = _validated_screenshots(media_dir)
    ffmpeg = _resolve_ffmpeg(args.ffmpeg)
    ffmpeg_version = _validate_ffmpeg(ffmpeg)

    print(ffmpeg_version)
    for name, path in sorted(screenshots.items()):
        print(f"INPUT {name} SHA256={_sha256(path)}")
    if args.check_only:
        print("Input and encoder checks passed; no artifacts were created.")
        return 0
    if not args.confirm_reviewed:
        raise SystemExit(
            "Rendering requires --confirm-reviewed after visually checking every screenshot "
            "for accurate provenance labels and absence of private/local data."
        )

    output_dir.mkdir(parents=True, exist_ok=True)
    outputs = (
        output_dir / VIDEO_NAME,
        output_dir / CAPTION_NAME,
        output_dir / THUMBNAIL_NAME,
    )
    existing = tuple(path for path in outputs if path.exists())
    if existing and not args.force:
        joined = ", ".join(str(path) for path in existing)
        raise SystemExit(f"Refusing to replace existing artifacts without --force: {joined}")

    with tempfile.TemporaryDirectory(prefix=".walkthrough-build-", dir=output_dir) as raw_temp:
        temporary = Path(raw_temp)
        segments: list[Path] = []
        for scene in SCENES:
            still = temporary / f"{scene.slug}.png"
            if scene.screenshot is None:
                _render_card(scene, still)
            else:
                _render_screenshot_scene(scene, screenshots[scene.screenshot], still)
            segment = temporary / f"{scene.slug}.mp4"
            _render_segment(ffmpeg, still, scene, segment)
            segments.append(segment)

        staged_video = temporary / VIDEO_NAME
        staged_captions = temporary / CAPTION_NAME
        staged_thumbnail = temporary / THUMBNAIL_NAME
        _assemble(ffmpeg, segments, staged_video)
        _write_srt(staged_captions)
        _render_thumbnail(screenshots["minerallens-overview.png"], staged_thumbnail)
        details = _validate_video(ffmpeg, staged_video)

        _publish(staged_video, outputs[0], force=args.force)
        _publish(staged_captions, outputs[1], force=args.force)
        _publish(staged_thumbnail, outputs[2], force=args.force)

    print(
        "OUTPUT "
        + " ".join(f"{key}={value}" for key, value in details.items())
        + f" bytes={outputs[0].stat().st_size}"
    )
    for output in outputs:
        print(f"ARTIFACT {output} SHA256={_sha256(output)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
