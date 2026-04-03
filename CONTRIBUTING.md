# Contributing

Thanks for helping improve OpenComputePrices.

## Development Setup

1. Use Python 3.9 or newer.
2. Create a virtual environment.
3. Install optional browser dependencies with `pip install -r requirements.txt` if you plan to work on Playwright collectors.

## Local Checks

Run these before opening a pull request:

```bash
PYTHONPYCACHEPREFIX=/tmp/pycache python -m compileall backfill_skypilot.py collect.py collectors rebuild_archive.py schema.py summary.py unify.py
python -m unittest discover -s tests -v
```

## Collector Guidelines

- Keep collectors stdlib-only unless browser rendering is strictly necessary.
- Normalize rows through `BaseCollector.make_row()`.
- Prefer explicit parsing over brittle regexes when a structured response exists.
- Handle partial upstream failures locally and let unexpected failures raise.

## Pull Requests

- Describe the provider or workflow behavior changed.
- Include sample output or a short verification note for new collectors.
- Update `README.md` when setup, schema, or source coverage changes.
