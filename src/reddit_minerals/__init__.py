"""Production-oriented Reddit minerals collection and analysis package."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("reddit-minerals-scraper")
except PackageNotFoundError:  # pragma: no cover - source tree without installation
    __version__ = "0.0.0"

__all__ = ["__version__"]
