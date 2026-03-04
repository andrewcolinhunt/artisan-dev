# Installation

## Prerequisites

- **Platforms:** Linux (x86_64) or macOS (Apple Silicon)
- Python 3.12+
- [Pixi](https://pixi.sh) package manager

## Quick Install

```bash
# Install Pixi (if not already installed)
curl -fsSL https://pixi.sh/install.sh | sh

# Clone the repository
git clone https://github.com/dexterity-systems/artisan.git
cd artisan

# Install all dependencies
pixi install

# Verify (restart your terminal first if pixi was just installed)
pixi run python -c "import artisan; print('Installation OK')"
```

## Start the Prefect Server

Artisan uses [Prefect](https://www.prefect.io/) to orchestrate pipeline
execution. A Prefect server must be running before you run any
pipeline.

```bash
# Start the server in the background
pixi run prefect-start
```

The server writes a discovery file so Artisan can find it automatically.

To stop the server when you're done:

```bash
pixi run prefect-stop
```

You can also point to an existing Prefect server instead of starting a local one:

```bash
export ARTISAN_PREFECT_SERVER=http://<host>:<port>/api
```

## IDE Setup (VSCode)

### Python Interpreter

Set the Pixi environment as your VSCode Python interpreter:

```bash
pixi run which python
# Example output: /home/user/artisan/.pixi/envs/default/bin/python
```

In VSCode: `Ctrl+Shift+P` → "Python: Select Interpreter" → paste the path above.

### Jupyter Kernel

Register the Pixi environment as a Jupyter kernel so notebooks use the correct
packages:

```bash
pixi run install-kernel
```

In VSCode: open a `.ipynb` file → click "Select Kernel" → choose **Artisan**.

## Next Steps

- [Your First Pipeline](first-pipeline.md) — Build and run a complete pipeline
  in ~15 minutes
- [Orientation](orientation.md) — A quick map of the key abstractions before
  diving deeper
