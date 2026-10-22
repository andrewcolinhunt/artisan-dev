# Artisan

Artisan is a Python framework for composable, observable computational workflows.

Build workflows from reusable operations around Python code and external tools.
Researchers and AI agents can compose tested integrations into new analyses and
extend them when needed. Artisan records execution history and the relationships
each operation declares between individual results, so you can inspect how an
output was produced and use its lineage in later computations.

Artisan grew out of computational protein design. Its core is domain-agnostic,
so applications can supply their own tools and data types. Start with the
[orientation](getting-started/orientation.md) to see how operations, artifacts,
and provenance work together.

::::{grid} 2
:::{grid-item-card} Getting Started
:link: getting-started/index.md

Install Artisan and run your first pipeline.
:::

:::{grid-item-card} Tutorials
:link: tutorials/index.md

Run complete examples in interactive notebooks.
:::

:::{grid-item-card} Concepts
:link: concepts/index.md

Understand operations, orchestration, storage, and provenance.
:::

:::{grid-item-card} How-to Guides
:link: how-to-guides/index.md

Complete specific tasks once you know the basics.
:::

:::{grid-item-card} Reference
:link: reference/index.md

Find public APIs, inspect their current contracts, and look up terminology.
:::

:::{grid-item-card} Contributing
:link: contributing/index.md

Coding conventions, tooling decisions, and documentation guidelines.
:::
::::
