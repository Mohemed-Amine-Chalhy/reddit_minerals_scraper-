"""Allow ``python -m reddit_minerals`` execution."""

from reddit_minerals.cli import main

if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
