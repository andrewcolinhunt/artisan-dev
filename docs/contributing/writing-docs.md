# Writing Documentation

Write documentation that helps someone use Artisan correctly. Keep explanations
and working examples in the site. Keep exact API details beside the code that
implements them.

## Choose the page's purpose

Use the reader's task to decide what belongs on a page:

| Page type | Purpose | Include |
| --- | --- | --- |
| Tutorial | Learn by running a small workflow | Prerequisites, runnable notebook cells, observations, and next steps |
| How-to guide | Complete a specific task | A working example, necessary choices, and a way to verify the result |
| Concept | Understand behavior and tradeoffs | A concrete example, the relevant guarantees, and their limits |
| Reference | Look up a fact | A short index, terminology, or information derived from the public API |

These distinctions follow [Diátaxis](https://diataxis.fr/). Use them to keep a
page focused. A short explanation within a recipe is useful when it helps the
reader make the next decision; link to the concept page for more detail.

Use only the sections a page needs. A short guide does not need a summary that
repeats its opening or a table of pitfalls with no relevant entries.

## Keep one authoritative description

- **Signatures, types, defaults, return values, and exceptions** belong in
  function and class docstrings, type annotations, and schema fields. Link to
  [Python API lookup](../reference/python-api.md) for inspection and source access.
  Do not copy complete signatures or field catalogs into guides.
- **Workflow behavior** belongs in one main guide or concept page. Explain
  choices such as cache reuse, run selection, and cancellation there; link from
  other pages instead of repeating the explanation.
- **Examples** should demonstrate the task with the fewest relevant options.
  Keep a complete example intact. Avoid repeating a shortened implementation
  that differs from the working version.
- **Implementation details** belong in source comments or contributor guidance
  when a contributor needs them. User docs should describe the behavior that
  affects a user's decisions.

A small table comparing choices can be useful. Include it because the reader
needs to make that choice, not because a model has fields that could be listed.
An internal module move should not require rewriting a user-facing concept.

Automatic reference generation still depends on accurate docstrings. If API
reference is generated, build it from the same revision as the rest of the site
and select supported public interfaces. Do not publish every internal helper or
maintain a second set of descriptions in the generator.

## Write directly

Use plain, matter-of-fact language and address the reader as “you.” Lead with
what the page helps them do or understand. Explain an unfamiliar term when it
first matters, and use the same term throughout the docs.

Describe observable behavior. For example, say “The failed attempt remains
available for inspection” instead of claiming that the architecture makes all
failure modes impossible. State relevant limits alongside the behavior they
qualify.

Avoid promotional claims, rhetorical introductions, filler, and claims that a
task is easy. Prefer short sentences, but keep the context needed to understand
why a setting or step matters.

## Write examples that prove the point

Use Artisan's built-in example operations and generic data. Import from the
supported public packages listed in [Python API lookup](../reference/python-api.md).
Do not teach a private import as a user workflow.

A runnable example needs its imports, input data, and setup. If a fragment
continues an earlier example, say so. For snippets showing a user's own operation,
state what they must supply; do not label an undefined operation a minimal
working example.

Use deterministic data when the expected outcome depends on identity, caching,
ordering, or exact counts. Show the result that matters:

- A cache example checks whether the step executed or reused prior work.
- A batching example checks which inputs each output came from.
- An export example selects the intended run and verifies the exported data.

Operation-authoring examples should work with multiple inputs per execution
unit when they teach per-artifact dispatch. Keep sample files small and use
isolated output directories. Explain cleanup that deletes a tutorial's previous
run. State account, credential, and deployment prerequisites for cloud examples
before the first command that needs them.

Tutorials are notebooks. Keep their committed outputs empty; the notebook tests
execute cells. Include assertions for outcomes that would otherwise be checked
only by reading a printed table. A passing cell alone does not prove that its
explanation is correct.

## Format and navigation

Write pages in MyST Markdown and tutorials in Jupyter notebooks. Use lowercase
kebab-case filenames, a title-case page title, and sentence-case subheadings.
Use tables for comparisons and diagrams when they make a relationship clearer.

Register every content page in `docs/myst.yml`. Add links where a reader needs
background or a next action. Link directly to the relevant page or section; do
not add reciprocal links solely to satisfy a template.

Use relative Markdown links within the site:

```markdown
[Building a pipeline](../how-to-guides/building-a-pipeline.md)
[First pipeline](../tutorials/01-getting-started/01-first-pipeline.ipynb)
```

Preserve a referenced heading when revising a page, or update its incoming links.
Use an explicit MyST label when a heading needs a stable target:

```markdown
(cache-reuse)=
## Reuse previous results
```

Use MyST directives for notes that deserve separate attention:

```markdown
::::{note}
State the condition and what the reader should do about it.
::::
```

## Building and previewing docs

From the repository root:

```bash
pixi run --locked -e docs docs-build
pixi run --locked -e docs docs-serve
```

Open `http://localhost:8000`. Check the changed pages, their examples and diagrams,
and navigation to and from them. `docs-build` renders the site; it does not
execute all notebook examples.

For code changes in local tutorials, run:

```bash
pixi run --locked -e dev test-notebook
```

See [development tasks](../getting-started/using-pixi.md#dev-environment) for the
separate resource requirements of local, S3, and Modal tests. Run the relevant
cloud checks when a changed example depends on a live service.

Run the published-example syntax/import check after changing Python snippets:

```bash
pixi run --locked -e dev pytest -q tests/test_published_imports.py
```

For a changed how-to example, execute the complete example in a temporary
working directory and verify its result. Syntax and import checks do not catch
wrong input types, incorrect run selection, or misleading count assertions.

## Review before merging

Check that the page answers its stated question, the examples match the code,
and the changed claims have supporting evidence. Remove repeated API facts and
unnecessary implementation detail. Confirm that links resolve and the rendered
page is readable. Update the canonical explanation when behavior changes, then
check the examples that depend on it.
