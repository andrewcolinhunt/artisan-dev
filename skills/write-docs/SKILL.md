---
name: write-docs
description: Use when the user asks to "write docs", "write documentation", "add a docs page", "create a tutorial", "document a feature", or any task involving writing or editing documentation pages, tutorials, how-to guides, concept pages, or reference pages for the project.
argument-hint: "[path/to/page.md or description of what to document]"
---

# Documentation Writing

Write or edit documentation at `$ARGUMENTS`.

Before writing, read these files:

1. **`docs/contributing/writing-docs.md`** — page structure, Diataxis boundary
   rules, cross-linking requirements, and templates for all four page types
   (tutorial, how-to, concepts, reference). Follow the template for the
   category you are writing.
2. **`CLAUDE.md`** and **`docs/myst.yml`** — project conventions and TOC
   structure.
3. **Existing pages in the same section** — match format, depth, and
   conventions already established.

---

## Writing Tone

### Core principle

Respect the reader's time and intelligence. Write as a knowledgeable colleague
explaining something at a whiteboard — not a legal contract, not a blog post.

### Rules

**Be direct.**
Use active voice. Address the reader as "you." Get to the point.
- Yes: "Run the command"
- No: "The command should be run"

**Be confident but honest.**
State things plainly. When something is a limitation or rough edge, say so.
- Yes: "This method returns a string"
- No: "This method should return a string"
- Yes: "Note: this operation is not atomic — if you need atomicity, see X"

**Be warm without being chatty.**
A brief orienting sentence is welcoming. Excessive enthusiasm wastes the reader's
time.
- Yes: "This guide walks you through setting up authentication for your API"
- No: "Awesome! You're going to LOVE this feature!"

**Be task-oriented.**
Structure around what the reader is trying to do, not what the software can do.

**Be consistent in terminology.**
Pick a term and stick with it. If you call it a "workspace" in one paragraph,
don't call it a "project" in the next.

**Layer detail progressively.**
Lead with the most common case. Then explain options. Then cover edge cases. A
reader who needs the quick answer gets it in 10 seconds; someone with a complex
scenario keeps reading.

**Be economical.**
Short sentences. Simple words. "Use" not "utilize." "Start" not "initialize"
(unless "initialize" is the actual API term). Cut filler: "in order to," "it
should be noted that," "as a matter of fact."

**Be neutral about skill level.**
Never write "simply," "just," or "easily." Give clear instructions and let the
reader judge difficulty.

---

## Format Conventions

- **Markup**: MyST Markdown (Jupyter Book 2). Use MyST directives, not RST.
- **Tutorials**: Jupyter notebooks (`.ipynb`). Code cells should be runnable.
- **Other pages**: Markdown (`.md`).
- **Code examples**: Must be syntactically correct and use current API. Import
  from the correct package (`artisan` for framework, `pipelines` for domain).
- **Headings**: Title case for page title, sentence case for all subheadings.
- **Cross-references**: Use MyST cross-reference syntax to link between pages.

---

## Checklist

Before finishing, verify:

- [ ] Page belongs to exactly one Diataxis category and matches its structure
- [ ] Tone follows the rules above (scan for "simply", "just", "easily",
      passive voice, filler phrases)
- [ ] Terminology is consistent with existing docs and codebase
- [ ] Code examples are syntactically correct and use current imports
- [ ] Progressive disclosure: common case first, edge cases last
- [ ] No unexplained jargon — terms are defined on first use or obvious
- [ ] Page is registered in `docs/myst.yml` TOC if it's a new page
