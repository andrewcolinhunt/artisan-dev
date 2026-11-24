# Artisan Modal

`artisan-modal` provides `ModalRunner` for running Artisan operations and
composites on Modal. The remote worker runs the full operation lifecycle.
Command operations declare their tool dependencies separately from the worker
environment.

Install the matching versions of Artisan and this provider. To install both
from the same source checkout, run this from the repository root:

```sh
python -m pip install . ./packages/artisan-modal
```

Configure a Modal account, an S3-compatible shared store, and a locked Pixi
project for the worker software. Use `ModalRunner.prepare()` to prepare the
worker before submitting the operation or composite through `PipelineManager`.
For examples, see [Run Remote Workers](https://github.com/dexterity-systems/artisan/blob/main/docs/how-to-guides/running-remote-workers.md).
