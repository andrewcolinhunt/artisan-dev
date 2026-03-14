# Getting Started Redesign

## Problem

Real user feedback (Michael Tyka, 2026-03-13) and a doc review revealed that
the current getting-started experience fails at multiple points:

**Installation friction**
- `pixi install` fails on resource-constrained nodes (thread spawn panic) —
  workaround (`RAYON_NUM_THREADS=4`) is undocumented
- Users unfamiliar with Pixi don't know how it compares to venv, how to
  activate it, or whether multiple clones can coexist
- No guidance on where to run things on HPC (head node vs compute node vs
  interactive session)
- Prefect server placement is unclear on shared infrastructure
- Prefect Cloud as an alternative to local server is unmentioned

**First experience is too complex**
- First Pipeline tutorial (`first-pipeline.md`) references `path/to/csv/files`
  in the `IngestData` step — the path doesn't exist, so the first real step
  errors out
- First Pipeline tutorial introduces 6 operations, 3 storage directories,
  merging, filtering, metrics, and output references — all at once
- No "hello world" moment: the simplest runnable example requires
  understanding too many concepts simultaneously
- No expected output shown anywhere — user can't verify they're on track

**Structural issues**
- No gentle on-ramp between "installed successfully" and the complex 7-step
  tutorial — the gap is too wide
- Orientation (mental model) exists but has no natural entry point — the user
  hits a complex tutorial before they've done anything simple
- No mention of `pixi shell` as an alternative to `pixi run`

---

## Design principles

- **Progressive disclosure.** Each page introduces one layer of complexity.
  A user who stops at any point has accomplished something real.
- **Show expected output.** Every command and code block that produces output
  should show what the user will see. This builds confidence and catches
  setup problems early.
- **No dead ends.** Every code snippet must work as-is. No placeholder paths,
  no "insert your data here."
- **Address the real environment.** Users run this on HPC clusters, not just
  laptops. The docs need to acknowledge that without drowning laptop users
  in HPC details.
- **Respect existing users' mental models.** People coming from venv/conda
  need a bridge, not a lecture.

---

## Proposed structure

### Getting Started section

```
docs/getting-started/
├── index.md                  # Landing page (updated)
├── installation.md           # Installation (rewritten)
├── quick-start.md            # NEW — hello world in 5 minutes
├── first-pipeline.md         # First Pipeline (simplified)
├── orientation.md            # Orientation (repositioned)
└── using-claude-code.md      # NEW — AI-assisted development with Claude Code
```

### README changes

The README `Quick Start` section needs minor updates. The `Quick Example` is
already clean (no placeholder paths) — it just needs expected output added.

---

## Page-by-page outline

### `index.md` — Landing page

Reorder to match the new flow:

- **Installation** — Get Artisan running
- **Quick Start** — Run your first pipeline in 5 minutes
- **Your First Pipeline** — Build a multi-step pipeline with data flow
- **Orientation** — The mental model behind the framework
- **Using Claude Code** — AI-assisted development within the repo

### `installation.md` — Installation (rewrite)

**Goal:** Get from zero to `Installation OK` with no ambiguity.

**Sections:**

- **Prerequisites**
  - Platform requirements (same)
  - Brief "What is Pixi?" paragraph: project-scoped environment manager,
    similar to venv but handles Python itself + all native dependencies.
    Each clone gets its own isolated environment. No global pollution.
  - Comparison callout: "If you're used to venv/conda, Pixi is similar but
    project-scoped — `pixi install` in a directory creates an environment
    for that directory only."

- **Install Pixi**
  - `curl` command (same)
  - Restart terminal reminder (same)
  - Troubleshooting tip for restricted environments

- **Clone and install**
  - `git clone` + `cd` + `pixi install`
  - Show expected output (or at least "you should see a progress bar...")
  - Troubleshooting: `RAYON_NUM_THREADS=4 pixi install` for
    resource-constrained nodes (thread spawn error)

- **Verify**
  - `pixi run python -c "import artisan; print('Installation OK')"`
  - Show expected output: `Installation OK`

- **Start the Prefect server**
  - `pixi run prefect-start`
  - Show expected output / what to check (`http://localhost:4200`)
  - `pixi run prefect-stop` to stop
  - Callout: "On HPC clusters" — expandable/tip box:
    - Don't run on the head node
    - Use a persistent interactive session (long-running CPU node, screen/tmux)
    - The server needs to stay running while pipelines execute
  - Callout: "Using Prefect Cloud" — brief mention that you can use Prefect
    Cloud instead of a local server, with `PREFECT_API_URL` env var. Link to
    Prefect docs for setup.

- **Using Pixi day-to-day**
  - `pixi run <command>` — run a single command in the environment
  - `pixi shell` — activate a shell session (like `source .venv/bin/activate`)
  - `pixi run python script.py` — run a script
  - Note: all `pixi` commands are project-scoped. `cd` into the repo first.

- **IDE setup (VSCode)** — keep existing content, same structure

- **Troubleshooting** — expand table:
  - Add: thread spawn error → `RAYON_NUM_THREADS=4 pixi install`
  - Add: "pixi install is slow" → normal on first run, downloads Python +
    all deps
  - Keep existing entries

- **Next steps** — link to Quick Start

### `quick-start.md` — Quick Start (NEW)

**Goal:** Run a working pipeline in under 5 minutes. Two steps, visible
output, minimal concepts.

**Diataxis category:** Getting Started (practical, learning-oriented, but
markdown not notebook since it's a "follow along in your terminal" page).

**Prerequisites:** Installation complete, Prefect server running.

**Sections:**

- **Intro paragraph**
  - "This page gets you from installed to running in 5 minutes. You'll
    create a pipeline that generates data and transforms it."
  - No theory. No architecture. Just do the thing.

- **Create a script**
  - Tell the user to create a file `my_first_pipeline.py`
  - Minimal pipeline: generate → transform → finalize
  - Use only `DataGenerator` and `DataTransformer` (two operations, one
    connection)
  - ~15 lines of code total
  - Inline comments explain each line briefly

- **Run it**
  - `pixi run python my_first_pipeline.py`
  - Show expected terminal output (pipeline log messages, step completion,
    finalize summary)

- **See what happened**
  - The script itself should include inspection calls at the end (after
    `finalize()`), so the user sees output when they run it:
    - `inspect_pipeline(delta_root)` — prints a table with 2 rows
    - `build_macro_graph(delta_root)` — saves a provenance graph image
  - Show expected terminal output for each
  - Brief: "Every artifact is tracked. Every step is recorded. The framework
    did this automatically."

- **What's next**
  - "You've run a pipeline. Here's what to explore:"
  - Link to First Pipeline (more operations, data flow patterns)
  - Link to Orientation (understand the concepts behind what you just did)
  - Link to Exploring Results tutorial (dig into the stored data)

### `first-pipeline.md` — Your First Pipeline (simplify)

**Goal:** Build a pipeline that goes beyond generate-and-transform: compute
metrics, filter by score, and refine the survivors. This is where the user
sees output references, params, and inspection tools working together.

**Changes from current version:**

- **Remove IngestData and Merge steps.**
  - IngestData requires external CSV files that don't exist in the repo.
    Ingestion is covered in the tutorials section.
  - Merge adds a second data stream and a new concept. Covered in Pipeline
    Design tutorials.
  - Pipeline becomes: generate → transform → metrics → filter → refine
    (5 steps, not 7). Zero external dependencies.

- **Add expected output after every step**
  - After each `pipeline.run()`, show what the user should see in their
    terminal
  - After each inspection call, show the expected table/graph

- **Add brief "what just happened" after each step**
  - One sentence explaining the key concept demonstrated
  - Not architecture — just "this step created 5 dataset artifacts from
    nothing" or "filter selected 3 of 5 artifacts based on median > 0.15"

- **Keep all the good parts:**
  - Output references (`pipeline.output`)
  - Params
  - Finalize
  - Inspection (pipeline summary, metrics, provenance graph)
  - Summary table of concepts at the end

### `orientation.md` — Orientation (reposition)

**Changes:**

- Move to 4th position (after first-pipeline, not after installation)
  - At this point the user has *done* things and wants to understand *why*
  - The current content is good but reads better as a "now that you've
    seen it in action, here's the mental model"
- Add a brief intro: "You've built a pipeline and seen artifacts, operations,
  provenance, and storage in action. This page explains the mental model
  behind them."
- Keep the five abstractions section as-is (it's well written)
- Keep the docs organization section

### `using-claude-code.md` — Using Claude Code (NEW)

**Goal:** Get a user from "I've heard of Claude Code" to productively using it
inside the Artisan repo. This is not a Claude Code tutorial — it's about how
Artisan is set up to work *with* Claude Code, and what that unlocks.

**Diataxis category:** Getting Started (practical, learning-oriented).

**Why this page exists:** Artisan ships with a `CLAUDE.md` that teaches Claude
Code the project conventions, and a plugin with framework-aware skills. Most
users won't discover this on their own. An explicit page makes the AI-assisted
workflow a first-class part of the getting-started experience.

**Sections:**

- **What is Claude Code?**
  - One paragraph: AI coding assistant that runs in your terminal, reads your
    codebase, and can edit files, run commands, and write code.
  - Link to official Claude Code docs for installation.
  - "Artisan is set up so Claude Code understands the framework's conventions,
    architecture, and APIs out of the box."

- **Install Claude Code**
  - `npm install -g @anthropic-ai/claude-code` (or link to official install)
  - Verify: `claude --version`

- **Start Claude Code in the repo**
  - `cd artisan && claude`
  - Explain what happens: Claude Code reads `CLAUDE.md` at the repo root,
    which teaches it about Artisan's architecture, testing commands, code
    style, and Git conventions.
  - "You don't need to explain the project from scratch each time — the
    context is built in."

- **Artisan skills**
  - The repo ships with framework-specific skills (slash commands) that
    Claude Code auto-discovers from the `.claude-plugin/` directory.
  - If skills aren't available automatically, install them manually (these
    commands are run inside the Claude Code interactive session, not in a
    regular terminal):
    ```
    /plugin marketplace add
    /plugin install
    ```
  - Brief table of available skills (same as current installation.md):
    `/artisan:write-operation`, `/artisan:write-composite`,
    `/artisan:write-pipeline`, `/artisan:write-docs`

- **What you can do**
  - Walk through concrete examples of real workflows. Not exhaustive — just
    enough to show the value:
    - "Write me an operation that takes CSV files and computes column
      statistics" → Claude scaffolds the class, tests, and docstrings
    - "Build a pipeline that generates data, transforms it, and filters by
      median score" → Claude writes a working pipeline script
    - "Explain how artifact lineage works in this codebase" → Claude reads
      the source and explains
    - "Run the tests and fix any failures" → Claude runs pytest, reads
      errors, and proposes fixes
  - Emphasize: Claude Code knows the `pixi run` commands, the test structure,
    the formatting rules. It follows the same conventions a human contributor
    would.

- **Tips for effective use**
  - Be specific about what you want ("write an operation that..." not "help
    me with operations")
  - Review generated code before committing — Claude proposes, you decide
  - Use skills for scaffolding, then iterate: `/artisan:write-operation` gets
    you 80% there, then refine
  - Claude Code can run tests and format code for you: ask it to run the
    pre-PR checklist

- **What `CLAUDE.md` contains**
  - Brief explanation: it's a project-level instruction file that Claude Code
    reads automatically. It contains environment setup, code style, testing
    commands, Git conventions, and architecture overview.
  - "You can read it yourself at `CLAUDE.md` in the repo root — it's the
    same instructions a new contributor would follow."
  - Note: you can create a personal `CLAUDE.local.md` for your own
    preferences (auto-gitignored).

### README changes

**Quick Start section:**
- Keep the install commands
- Replace the verification step with a note about checking the docs for
  Prefect server setup
- Or: keep `prefect-start` but add a one-line note about what it does

**Quick Example section:**
- The current README example already uses DataGenerator → DataTransformer →
  MetricCalculator → Filter with no placeholder paths — it works as-is
- Add expected output inline (even just `# Pipeline complete: 4 steps`)
- Add "See the full Getting Started guide for more"

---

## What we're NOT changing

- **Tutorials section** stays as-is. The tutorials are comprehensive and well
  structured. The getting-started tutorials (`01-first-pipeline.ipynb`,
  `02-exploring-results.ipynb`) should be updated to match the simplified
  first-pipeline flow, but that's a follow-on task.
- **Orientation content** stays mostly the same — it's good. We're just
  moving it in the reading order.
- **IDE setup** stays in installation.md — it's the right place.
- **Claude Code section in installation.md** gets replaced with a brief
  mention and a link to the dedicated using-claude-code.md page.
  All Claude Code content (install, plugin, usage) lives in one place.

---

## Resolved questions

**Q: Should quick-start.md be a notebook or markdown?**
→ **Markdown page with a runnable script.** The page is `.md` (Getting Started
category), but it walks the user through creating a Python script that they
run with `pixi run python my_first_pipeline.py`. The user gets a runnable
artifact; the docs page stays in the right format. The script could also be
provided as a downloadable file alongside the page.

**Q: Should we add an HPC-specific getting-started page?**
→ **Not in this redesign.** Create an HPC how-to guide as a follow-on task.
Add HPC callouts (tip boxes) to installation.md for now.

**Q: Should we provide sample CSV files for IngestData?**
→ **Remove IngestData from first-pipeline.** Cover ingestion in tutorials.

---

## Implementation order

- Update `installation.md` — Pixi explainer, troubleshooting, HPC callouts
- Create `quick-start.md` — minimal hello-world pipeline script
- Simplify `first-pipeline.md` — remove ingest/merge, add expected output
- Create `using-claude-code.md` — AI-assisted development guide
- Update `index.md` — new reading order
- Update `myst.yml` — register new pages
- Update `orientation.md` — add intro paragraph referencing prior experience
- Update README — add expected output to Quick Example, minor Quick Start tweaks
- Update tutorial notebooks to match (follow-on task)
