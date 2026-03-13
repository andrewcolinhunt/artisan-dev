# Documentation Contributor Guide

This guide establishes conventions for writing and maintaining Artisan framework
documentation. All contributors (human and AI) should follow these rules to keep
docs consistent, well-organized, and domain-agnostic.

---

## Documentation structure (Diataxis)

Artisan docs follow the [Diataxis](https://diataxis.fr/) framework, which
organizes documentation into four quadrants based on two axes: the reader's
**mode** (learning vs. working) and the content's **orientation** (practical vs.
theoretical).

|                            | Learning (acquiring) | Working (applying) |
| -------------------------- | -------------------- | ------------------ |
| **Practical (doing)**      | Tutorials            | How-to Guides      |
| **Theoretical (thinking)** | Concepts             | Reference          |

### Rationale

- **Mixing categories serves nobody well.** A tutorial that stops to explain
  architectural rationale loses the learner. A reference page that weaves in
  step-by-step narrative frustrates someone scanning for a function signature.
- **Different maintenance cadences.** Tutorials need updating when the onboarding
  experience changes. Reference pages need updating when the API changes. Concepts
  pages are the most stable. Separating categories lets each evolve independently.
- **Clear placement rules for new content.** When adding a page, there should be
  exactly one correct quadrant. If it does not fit cleanly, the content should be
  split.

### Implications

- **One page, one category.** Never mix quadrants on a single page.
- **Concepts pages have no API signatures; reference pages have no prose
  rationale.** If you need both, link between pages instead.
- **Tutorials are self-contained.** A reader should be able to follow a tutorial
  without reading anything else first.
- **How-to guides assume competence and link to concepts for background.** They
  do not teach; they direct.

---

## Diataxis boundary rules

| Quadrant       | Purpose                 | Format                                                    | Constraints                                                           | Reader mindset              |
| -------------- | ----------------------- | --------------------------------------------------------- | --------------------------------------------------------------------- | --------------------------- |
| **Tutorials**  | Learning by doing       | Self-contained, step-by-step. Notebooks only.             | No API signatures, no architectural rationale.                        | "I'm new, teach me."       |
| **How-to Guides** | Task-oriented recipes | Prerequisites/Verify sections. Structured steps.          | Assumes competence. Links to concepts for background.                 | "I need to accomplish X."   |
| **Concepts**   | Explanations of *why*   | "Why This Exists" opener. Decision/Rationale/Implications format. | No API signatures. No step-by-step instructions.                     | "I want to understand."     |
| **Reference**  | Factual descriptions    | Tables, signatures, enum values. "See Also" footer.       | No prose rationale. No narrative or teaching.                         | "I need the exact API."     |

---

## Cross-quadrant linking pattern

Every page must link to related pages in other quadrants.

| Source quadrant   | Must link to                                                  |
| ----------------- | ------------------------------------------------------------- |
| **Concepts**      | 1-2 tutorials + the relevant reference page                   |
| **How-to Guides** | Prerequisite concepts page(s) + the relevant reference page   |
| **Reference**     | Corresponding concepts page + relevant how-to guide(s)        |
| **Tutorials**     | Concepts page for deeper understanding + how-to for techniques |

When adding a new page, check that the linked pages also link back.

---

## Writing tone

Respect the reader's time and intelligence. Write as a knowledgeable colleague
explaining something at a whiteboard -- not a legal contract, not a blog post.

**Be direct.** Use active voice. Address the reader as "you." Get to the point.

- Good: "Run the command"
- Avoid: "The command should be run"

**Be confident but honest.** State things plainly. When something is a
limitation or rough edge, say so.

- Good: "This method returns a string"
- Avoid: "This method should return a string"

**Be warm without being chatty.** A brief orienting sentence is welcoming.
Excessive enthusiasm wastes the reader's time.

- Good: "This guide walks you through setting up authentication for your API"
- Avoid: "Awesome! You're going to LOVE this feature!"

**Be task-oriented.** Structure around what the reader is trying to do, not
what the software can do.

**Be consistent in terminology.** Pick a term and stick with it. If you call
it a "workspace" in one paragraph, do not call it a "project" in the next.
Use the terms defined in the [Glossary](../reference/glossary.md).

**Layer detail progressively.** Lead with the most common case, then explain
options, then cover edge cases. A reader who needs the quick answer gets it in
10 seconds; someone with a complex scenario keeps reading.

**Be economical.** Short sentences. Use "use" not "utilize," "start" not
"initialize" (unless "initialize" is the actual API term). Cut filler: "in
order to," "it should be noted that," "as a matter of fact."

**Be neutral about skill level.** Never write "simply," "just," or "easily."
Give clear instructions and let the reader judge difficulty.

---

## File and format conventions

### Markup

All docs use **MyST Markdown** (Jupyter Book 2). Use MyST directives, not RST.

### File formats

| Quadrant       | Format             | Extension |
| -------------- | ------------------ | --------- |
| Tutorials      | Jupyter notebooks  | `.ipynb`  |
| How-to Guides  | Markdown           | `.md`     |
| Concepts       | Markdown           | `.md`     |
| Reference      | Markdown           | `.md`     |
| Contributing   | Markdown           | `.md`     |
| Getting Started | Markdown          | `.md`     |

### Directory layout

New pages go in the directory that matches their Diataxis quadrant:

```
docs/
├── getting-started/         # Installation and orientation
├── tutorials/               # Jupyter notebooks (.ipynb)
│   ├── getting-started/     # First steps
│   ├── pipeline-design/     # Topology patterns
│   ├── execution/           # Caching, batching, error handling, overrides
│   ├── analysis/            # Provenance, filtering, timing
│   └── writing-operations/  # Building custom operations and composites
├── concepts/                # Explanations of design and architecture
├── how-to-guides/           # Task-oriented recipes
├── reference/               # API signatures, glossary, comparisons
└── contributing/            # Project conventions (this page)
```

### File naming

- Use **lowercase kebab-case**: `writing-creator-operations.md`
- Tutorials are numbered with a two-digit prefix: `01-first-pipeline.ipynb`,
  `02-exploring-results.ipynb`
- Every directory has an `index.md` landing page

### Headings

- **Page title**: title case (e.g., "Documentation Contributor Guide")
- **All subheadings**: sentence case (e.g., "Cross-quadrant linking pattern")

### Section separators

Use `---` horizontal rules between major sections. This matches the convention
across all existing pages.

---

## Registering new pages

Every new page must be added to the table of contents in `docs/myst.yml`.

Find the appropriate section under `project.toc` and add a `file:` entry. For
example, to add a new concepts page:

```yaml
# In docs/myst.yml, under project.toc
- title: Concepts
  children:
    - file: concepts/index.md
    - title: System Architecture
      children:
        - file: concepts/architecture-overview.md
        - file: concepts/operations-model.md
        - file: concepts/my-new-concept.md  # <-- add here
```

Tutorials within a sub-section are ordered by their numeric prefix. Place the
new entry in the position that matches its prefix number.

A page that exists on disk but is not listed in `myst.yml` will not appear in
the built site.

---

## MyST Markdown conventions

### Links between pages

Use standard Markdown links with relative paths:

```markdown
[Operations Model](../concepts/operations-model.md)
[First Pipeline tutorial](../tutorials/getting-started/01-first-pipeline.ipynb)
```

Link to a specific heading by appending `#anchor`:

```markdown
[Five layers](../concepts/architecture-overview.md#five-layers)
```

### Label targets

Use MyST label targets for anchors that other pages reference (especially in
the glossary):

```markdown
(glossary-artifact)=
## Artifact

An immutable, content-addressed data node...
```

### Directives

Use colon-fence syntax for MyST directives:

```markdown
::::{note}
This is important context the reader should know.
::::

::::{tip} Optional title
Helpful but non-essential information.
::::
```

For grid layouts (used on landing pages):

```markdown
::::{grid} 2
:::{grid-item-card} Card Title
:link: path/to/page.md

Card description.
:::
::::
```

### Include directives

Pull in external content with the `include` directive:

````markdown
```{include} ../README.md
```
````

---

## Code examples in documentation

Code blocks in docs must be **syntactically correct** and use the **current
API**. Import from the correct package (`artisan` for the framework).

- Show the minimum code needed to illustrate the point
- Include imports so the reader can copy-paste
- Use the example operations (`DataGenerator`, `DataTransformer`,
  `MetricCalculator`) for generic demonstrations -- these live in
  `src/artisan/operations/examples/`
- In how-to guides, provide a **minimal working example** before breaking
  the code into explained steps

When referencing source files, use the path from the project root:

```markdown
**Source:** `src/artisan/schemas/artifact/base.py`
```

---

## Domain decontamination

Artisan is a domain-agnostic framework. All documentation must use generic
examples that do not reference any specific scientific domain, tool, or dataset.

**What to avoid:**

- Domain-specific tool names, file formats, or workflows
- References to specific research fields or application areas
- Hardcoded paths, usernames, or environment-specific values

**What to use instead:**

- The built-in example operations (`DataGenerator`, `DataTransformer`,
  `MetricCalculator`) for demonstrations
- Generic terms like "input data," "output artifacts," "computed metrics"
- Placeholder paths like `~/projects/my-pipeline/`

If a concept requires a concrete example to be understandable, use a
self-contained scenario that any reader can follow regardless of their domain.

---

## Building and previewing docs

Build the documentation site locally to verify your changes render correctly:

```bash
pixi run -e docs docs-build     # Build HTML site
pixi run -e docs docs-serve     # Serve at http://localhost:8000
pixi run -e docs docs-clean     # Remove build artifacts
```

The build output goes to `docs/_build/html/`. Check for:

- Broken links (the build warns about these)
- Correct rendering of tables, code blocks, and directives
- New pages appearing in the sidebar navigation

---

## Templates

### Tutorial

All tutorials are Jupyter notebooks. Follow this cell structure:

```markdown
# Cell 1 (markdown)

# Title

## What you'll learn
- Bullet 1
- Bullet 2
- Bullet 3

**Prerequisites:** [links]
**Estimated time:** X minutes
```

```python
# Cell 2+ (alternating markdown -> code)
# Narrative explains *what* and *why* before each code cell.
# Code cells are short, focused, and produce visible output.
```

```markdown
# Final cell (markdown)

## Summary
Recap of what was covered.

## Next steps
- [Tutorial name](link) -- what it covers
- [Concept name](link) -- deeper understanding
- [How-to name](link) -- related task
```

Guidelines:

- Tutorials must be self-contained. A reader should not need to read other docs
  to follow along.
- Do not explain architecture or design decisions. Link to concepts pages
  instead.
- Do not include API signature tables. Link to reference pages instead.
- Every code cell should produce visible output so the reader can verify
  progress.

---

### How-to guide

```markdown
# Title (verb phrase)

One-line description of what the reader will accomplish.

**Prerequisites:** [links]

---

## Minimal working example

Complete, copy-pasteable code block that works end-to-end.

---

## Step 1: ...

Code block + brief explanation.

## Step 2: ...

...

---

## Common patterns

Named patterns with small code blocks (if applicable).

---

## Common pitfalls

| Problem | Cause | Fix |
|---------|-------|-----|
| ...     | ...   | ... |

---

## Verify

How to confirm success (expected output, a test to run, a state to check).

---

## Cross-references

- [Reference page](link) -- full API
- [Concepts page](link) -- design rationale
- [Related how-to](link) -- related task
```

Guidelines:

- **Title** is a verb phrase (e.g., "Configure Custom Artifact Storage", not
  "Custom Artifact Storage").
- **Prerequisites** links to concepts or other how-to pages the reader should
  have completed.
- **Steps** are numbered and concrete. Each step includes a code block or
  command.
- **Minimal working example** appears before the steps so readers who learn by
  example can copy-paste immediately.
- **Common pitfalls** table is optional but encouraged for operations that have
  non-obvious failure modes.
- **Verify** tells the reader how to confirm the task succeeded.

---

### Concepts page

```markdown
# Title (noun phrase)

Opening paragraph: state *why* this concept matters and what understanding it
enables. Frame around the reader's needs, not the software's features.

One-line summary restating the scope.

---

## Section heading (sentence case)

Explain the design decision or architectural concept. Use the pattern:

1. **What** the mechanism/pattern is (brief)
2. **Why** it exists (the problem it solves or the trade-off it makes)
3. **Implications** for the reader (what this means in practice)

Use comparison tables for contrasting two approaches:

| Aspect   | Approach A | Approach B |
|----------|------------|------------|
| Purpose  | ...        | ...        |
| Trade-off| ...        | ...        |

Use ASCII diagrams for flows or relationships:

    +-----------+     +-----------+
    |  Phase 1  | --> |  Phase 2  |
    +-----------+     +-----------+

---

## Key design decisions (optional)

Summarize the most important decisions as a table or bulleted list when
the page covers multiple related choices.

| Decision | Rationale |
|----------|-----------|
| ...      | ...       |

---

## Cross-references

- [Reference page](link) -- field tables and API signatures
- [Tutorial](link) -- see this concept in action
- [Related concept](link) -- complementary explanation
```

Guidelines:

- **Title** is a noun phrase (e.g., "Operations Model", not "Understanding
  Operations").
- **Opening paragraph** answers "why should I read this?" before diving into
  detail.
- **No API signatures or code blocks showing imports.** Those belong in
  reference pages. Short illustrative pseudocode is acceptable when it clarifies
  a concept.
- **No step-by-step instructions.** Link to how-to guides instead.
- **Explain "why" explicitly.** Use phrases like "Why this matters:",
  "Why this separation exists:", or a "Rationale" subsection.
- **Progressive disclosure:** lead with the high-level mental model, then
  layer in details. A reader who stops after two paragraphs should still
  understand the core idea.

---

### Reference page

```markdown
# Title Reference

One-line description of what this page covers.

**Source:** `src/artisan/path/to/module.py`

---

## Section name

Brief (1-2 sentence) description of this component.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| ...   | ...  | ...     | ...         |

### Methods (if applicable)

    def method_name(self, param: Type) -> ReturnType

One-line description. No rationale or narrative.

---

## Another section

...

---

## Skeleton examples

Minimal, copy-pasteable code showing correct usage. Place at the bottom,
not woven into field tables.

    class Example(BaseClass):
        ...

---

## See also

- [Concepts page](link) -- design rationale
- [How-to guide](link) -- step-by-step usage
- [Related reference](link) -- complementary API
```

Guidelines:

- **Title** ends with "Reference" (e.g., "OperationDefinition Reference",
  "Provenance Schemas Reference").
- **Source path** at the top so readers can jump to the implementation.
- **Field tables** use four columns: Field, Type, Default, Description.
  Every field in the public API must appear.
- **Minimal prose.** Sentences are descriptive ("Returns the artifact ID"),
  not explanatory ("This returns the artifact ID because..."). Save "why"
  for concepts pages.
- **No narrative or teaching.** No "first you need to understand..." or
  design rationale paragraphs.
- **Skeleton examples** at the bottom show correct, minimal usage. They
  complement the field tables but do not replace them.
- **"See also" footer** links to the corresponding concepts page, how-to
  guides, and related reference pages.

---

## Pre-submission checklist

Before finishing a documentation change, verify:

- [ ] Page belongs to exactly one Diataxis category and follows its template
- [ ] Tone follows the rules above (scan for "simply," "just," "easily,"
      passive voice, filler phrases)
- [ ] Terminology is consistent with existing docs and the
      [Glossary](../reference/glossary.md)
- [ ] Code examples are syntactically correct and use current imports
- [ ] Progressive disclosure: common case first, edge cases last
- [ ] Cross-references link to related pages in other quadrants
- [ ] New pages are registered in `docs/myst.yml`
- [ ] Docs build without errors (`pixi run -e docs docs-build`)

---

## Cross-references

- [Coding Conventions](coding-conventions.md) -- Code style, naming, and
  project structure
- [Tooling Decisions](tooling-decisions.md) -- Why Pixi and Prefect
- [Orientation](../getting-started/orientation.md) -- How the docs are
  organized from a reader's perspective
