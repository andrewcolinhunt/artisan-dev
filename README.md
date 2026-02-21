# Your Package

A short description of what this project does.

---

## Quick Start

**Prerequisites:** Python 3.12+, [Pixi](https://pixi.sh)

```bash
# Install Pixi (if needed)
curl -fsSL https://pixi.sh/install.sh | bash

# Clone and install
git clone https://github.com/your-org/your-repo.git
cd your-repo
pixi install

# Verify
pixi run python -c "import your_package; print('Installed successfully')"
```

---

## Development

```bash
pixi run -e dev test    # Run tests
pixi run -e dev fmt     # Format and lint
```

---

## License

BSD 3-Clause. See [LICENSE](LICENSE) for details.
