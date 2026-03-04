# Documentation Contributor Guide

This guide establishes conventions for writing and maintaining Artisan framework
documentation. All contributors (human and AI) should follow these rules to keep
docs consistent, well-organized, and domain-agnostic.

---

## Documentation Structure (Diataxis)

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

## Diataxis Boundary Rules

| Quadrant       | Purpose                 | Format                                                    | Constraints                                                           | Reader mindset              |
| -------------- | ----------------------- | --------------------------------------------------------- | --------------------------------------------------------------------- | --------------------------- |
| **Tutorials**  | Learning by doing       | Self-contained, step-by-step. Notebooks only.             | No API signatures, no architectural rationale.                        | "I'm new, teach me."       |
| **How-to Guides** | Task-oriented recipes | Prerequisites/Verify sections. Structured steps.          | Assumes competence. Links to concepts for background.                 | "I need to accomplish X."   |
| **Concepts**   | Explanations of *why*   | "Why This Exists" opener. Decision/Rationale/Implications format. | No API signatures. No step-by-step instructions.                     | "I want to understand."     |
| **Reference**  | Factual descriptions    | Tables, signatures, enum values. "See Also" footer.       | No prose rationale. No narrative or teaching.                         | "I need the exact API."     |

---

## Cross-Quadrant Linking Pattern

Every page must link to related pages in other quadrants.

| Source quadrant   | Must link to                                                  |
| ----------------- | ------------------------------------------------------------- |
| **Concepts**      | 1-2 tutorials + the relevant reference page                   |
| **How-to Guides** | Prerequisite concepts page(s) + the relevant reference page   |
| **Reference**     | Corresponding concepts page + relevant how-to guide(s)        |
| **Tutorials**     | Concepts page for deeper understanding + how-to for techniques |

When adding a new page, check that the linked pages also link back.

---

## Domain Decontamination

Artisan is a domain-agnostic framework. All Artisan docs must use generic
examples.

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
# Cell 2+ (alternating markdown → code)
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

## Cross-References

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

    ┌───────────┐     ┌───────────┐
    │  Phase 1  │ --> │  Phase 2  │
    └───────────┘     └───────────┘

---

## Key design decisions (optional)

Summarize the most important decisions as a table or bulleted list when
the page covers multiple related choices.

| Decision | Rationale |
|----------|-----------|
| ...      | ...       |

---

## Cross-References

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