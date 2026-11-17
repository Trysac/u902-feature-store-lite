# feature-store-lite

[![PyPI version](https://img.shields.io/pypi/v/trysac-feature-store-lite)](https://pypi.org/project/trysac-feature-store-lite/)
[![License: MIT](https://img.shields.io/pypi/l/trysac-feature-store-lite)](LICENSE)
[![CI](https://github.com/Trysac/u902-feature-store-lite/actions/workflows/ci.yml/badge.svg)](https://github.com/Trysac/u902-feature-store-lite/actions/workflows/ci.yml)

Offline and online feature store on SQLite with point-in-time correct joins that never leak future values into training data

## Problem

Describe in two or three sentences the concrete problem this utility solves, who hits it, and what
they do today without it. Replace this paragraph when the first real feature lands.

## Install

```sh
pip install trysac-feature-store-lite
# or
uv add trysac-feature-store-lite
```

Requires Python 3.10 or newer. No runtime dependencies.

## Usage

```python
import feature_store_lite

# normalize() trims surrounding whitespace and lower-cases the text.
print(feature_store_lite.normalize("  Hello World "))  # -> "hello world"

# Blank input is rejected explicitly instead of returning an empty string.
try:
    feature_store_lite.normalize("   ")
except ValueError as exc:
    print(f"rejected: {exc}")
```

## Documentation

- [Architecture](docs/ARCHITECTURE.md) and [decisions](docs/decisions/)
- [Testing](docs/TESTING.md) and [security](docs/SECURITY.md)
- [Project record](docs/PROJECT.md) and [stack](docs/STACK.md)
- [Changelog](CHANGELOG.md) and [contributing guide](CONTRIBUTING.md)

## License

[MIT](LICENSE) (c) 2026 Miguel Gomez
