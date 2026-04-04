# Analysis: Design Skill Suite — Research & Improvement Recommendations

**Date:** 2026-04-04
**Context:** Research into industry best practices for writing, planning,
reviewing, and implementing design documents — applied to the Claude Code
skill suite at `~/.claude/skills/` (write-design, plan-design,
implement-design, review-design).

---

## Method

Three research agents searched the internet in parallel, each focused on
one skill area. Sources included published engineering practices from
Google, Amazon, Uber, and Stripe; open-source RFC processes (Rust, Python,
React); Architecture Decision Records (ADRs); emerging spec-driven
development tools (GitHub Spec-Kit, Kiro, Tessl); AI coding agent
frameworks (Aider, SWE-Agent, Devin, Copilot Workspace); academic papers;
and engineering blogs.

A fourth agent (implement-design) was lost to connectivity issues. That
skill's research is deferred.

---

## Current State

The four skills form a pipeline:

```
write-design → review-design → plan-design → implement-design
```

| Skill | Purpose | Key sections |
|-------|---------|-------------|
| write-design | Author a design doc | Problem, Prior Art Survey, Design, Scope, Sequencing, Testing, Open Questions, Related Docs |
| review-design | Evaluate readiness | 5 dimensions: Clarity, Internal Consistency, Convention Compliance, Codebase Accuracy, Solution Effectiveness |
| plan-design | Sequence into PRs | Resolve open questions, validate against code, PR-by-PR plan with changes/tests/verification, risk flags |
| implement-design | Execute the plan | Branch, implement file-by-file, commit incrementally, DRY audit, validate, report |

---

## Findings by Skill

### write-design

**Strengths (preserve):**

- **Prior Art Survey** — aligns with Rust RFC 2333's philosophy. Our
  DRY-focused codebase survey is a unique strength; most industry
  templates lack this.
- **Sequencing** — practical and implementation-focused. Rare in other
  templates but very useful for bridging design to implementation.
- **Testing section** — aligns with spec-driven development emphasis on
  testability as a first-class design concern.

**Gaps:**

| Gap | Precedent | Notes |
|-----|-----------|-------|
| No Non-Goals section | Google, Rust RFCs, Angela Zhang template | Prevents scope creep. Non-goals are things that *could* reasonably be goals but are explicitly excluded. Not negated goals ("shouldn't crash") but scoping decisions ("this design does not address multi-tenancy"). |
| No Alternatives Considered | Google (mandatory), Rust RFCs ("Drawbacks" + "Rationale and alternatives") | Forces the author to prove they explored the solution space. Different from Prior Art (which surveys existing code) — this is about *design approaches you rejected for this problem*. |
| No Summary at the top | Google, Rust RFCs | One-paragraph elevator pitch so busy reviewers can decide in 30 seconds whether they need to read the full doc. |
| No cross-cutting concerns prompt | Google (dedicated sections for security, performance, observability) | A lightweight checklist prompt ("Does this change affect performance? Security? Observability?") catches gaps without adding heavyweight sections. |
| No Future Possibilities | Rust RFC 2561 | Parks "while we're at it..." suggestions without bloating the current scope. Prevents scope creep while acknowledging ideas. |
| No tiered doc weight | Uber (lightweight vs heavyweight templates based on blast radius) | The skill uses the same template regardless of change size. A config change doesn't need the same depth as an architectural redesign. |

**Key reference — Spec-Driven Development (SDD):** GitHub's Spec-Kit,
Kiro (AWS), and Tessl are formalizing spec-first approaches with AI
agents. Martin Fowler identifies three maturity levels: spec-first (write
spec then code), spec-anchored (spec lives alongside code for
maintenance), spec-as-source (spec IS the source of truth). Our current
approach is spec-first.

**Key reference — Grant Slatton's analogy:** "A design document is like a
proof in mathematics. The goal of a proof is to convince the reader the
theorem is true. The goal of a design document is to convince the reader
the design is optimal given the situation."

---

### plan-design

**Strengths (preserve):**

- PR-by-PR structure with file changes, tests, verification steps
- Risk flagging before implementation starts
- Design validation against current code (drift detection)
- Always directing to /implement-design at the end

**Gaps:**

| Gap | Precedent | Notes |
|-----|-----------|-------|
| No dependency DAG between PRs | Google CL practices, stacked-diff workflows (Graphite, ghstack) | Flat sequence doesn't express "PR 3 depends on PR 1 but not PR 2." Dependency information enables parallel work and clearer sequencing. |
| No prep-PR auto-identification | Kent Beck: "Make the change easy, then make the easy change" | Mechanical refactors (renames, interface additions, type introductions) should be identified and front-loaded automatically. These are low-risk, easy to review, and reduce downstream complexity. |
| No file contention analysis | Stacked-diff tooling, merge conflict research | When multiple PRs modify the same file, merge conflict risk rises. Should flag explicitly: "Warning: `dispatch.py` is modified in PRs 1, 3, and 5." |
| No checkpoint states | Google's "system is always deployable" principle | After each PR, describe the expected system state: "UnitResult exists and is importable, but nothing creates or consumes it yet. All existing tests pass unchanged." |
| No scope boundaries per PR | PR review best practices | State what each PR does NOT do. "PR 2 adds the schema but does NOT wire it into the dispatch loop." Prevents scope creep during implementation. |
| No blast radius per PR | SEI risk assessment practices | How many existing tests exercise the modified code? Low test coverage on modified code = higher risk. |
| No rollback notes | Incident response literature | For high-risk PRs, note what to do if they need to be reverted. "If PR 4 causes failures, revert it; PRs 1-3 remain valid independently." |

**Key insight from research:** "The gap between design and implementation
is bridged by concrete, ordered, dependency-aware work items with explicit
verification criteria." File identification is the easy part; ordering and
dependency analysis is the hard part.

**Key reference — PR sizing:** Google's internal data (documented in
*Software Engineering at Google*, Winters et al. 2020) suggests review
quality drops sharply above ~200 lines of meaningful change. The "one
reviewer, one sitting" rule: a PR should be reviewable in 30-60 minutes.

---

### review-design

**Strengths (preserve):**

- Five-dimension framework with structured PASS/WARN/FAIL verdicts
- Codebase Accuracy is a strong differentiator — few review processes
  (human or automated) can verify that code references are still valid
- Convention Compliance grounds the review in project-specific standards

**Gaps:**

| Gap | Precedent | Notes |
|-----|-----------|-------|
| No Completeness dimension | Google ASE 2023 paper (141K docs analyzed); Mercari's production readiness checklist | Check for *missing* sections rather than just evaluating content that IS there. A design doc that omits alternatives or testing strategy should be flagged even if everything it does say is correct. |
| No Operational Readiness check | NASA SW-FMEA; multiple industry sources cite migration/rollback/monitoring as the most commonly *missed* sections | Does the design address migration plans, rollback strategy, failure modes, monitoring? Especially important for changes to existing systems. |
| No evidence-based findings | Academic rubric research on LLM evaluation | Requiring direct quotes from the design doc for every finding prevents vague assessments ("the design is somewhat unclear") and increases trust. |
| No confidence levels on verdicts | LLM evaluation research (granularity mismatch findings) | Solution Effectiveness is inherently subjective — LLMs are weaker at evaluating trade-off quality than at checking references. Mark verdicts with confidence: HIGH (verifiable, like Codebase Accuracy) vs MODERATE (judgment-based, like Solution Effectiveness). |
| No systematic code reference extraction | RAG-based consistency checking research | For Codebase Accuracy, extract ALL code references (class names, function names, module paths) from the doc and verify each one, rather than spot-checking. |
| No common anti-pattern detection | Cross-referencing multiple failure-mode sources | Flag known anti-patterns: hand-wavy sections ("we'll handle this later"), copy-paste goals, negated non-goals, missing trade-off analysis. |

**Key reference — Google ASE 2023:** Analysis of 141,652 approved design
documents by 41,030 authors. Built a tool (DAC) that tracks approver
status, decreasing median time-to-approval by 25%. Primary finding: the
value of design review is incorporating organizational experience into a
design, not just error-catching.

**Key reference — Amazon:** Uses 6-page narrative documents read in
silence at the start of meetings (20 min), followed by discussion.
Documents are intentionally NOT pre-read to ensure everyone reads the same
version.

---

## Cross-Cutting Themes

**Theme: Strong on codebase-awareness, weak on exclusions.** The Prior Art
Survey and codebase validation patterns are our differentiator vs industry
templates. But non-goals, alternatives considered, scope boundaries, and
explicit exclusions are missing across all skills. These are the primary
defense against scope creep.

**Theme: Flat PR lists need dependency structure.** The plan-design skill
outputs a flat sequence. A dependency graph (even informal: "depends on PR
N") enables parallel work, clearer sequencing, and safer rollback.

**Theme: Review should check for missing content.** The review skill
evaluates what IS there but doesn't systematically check for what SHOULD
be there. Completeness and operational readiness are the highest-impact
additions.

**Theme: Evidence and confidence improve trust.** Requiring quotes for
review findings and marking confidence levels on verdicts makes the review
output more actionable and less hand-wavy.

---

## Prioritized Recommendations

### Tier 1 — High impact, straightforward

| Skill | Change |
|-------|--------|
| write-design | Add **Non-Goals** section to the template (after Problem) |
| write-design | Add **Alternatives Considered** section (after Design) |
| write-design | Add a one-paragraph **Summary** at the top (before Problem) |
| plan-design | Add **dependency information** between PRs ("depends on PR N") |
| plan-design | Add **checkpoint states** after each PR describing expected system state |
| review-design | Add **Completeness** dimension (check for missing sections) |

### Tier 2 — High impact, more nuanced

| Skill | Change |
|-------|--------|
| plan-design | **Auto-identify prep PRs** — recognize mechanical changes (renames, interface additions) and front-load them |
| plan-design | **File contention warnings** — flag when multiple PRs modify the same file |
| review-design | Add **Operational Readiness** sub-dimension (migration, rollback, monitoring, failure modes) |
| review-design | **Require evidence quotes** for every finding |
| write-design | Add **cross-cutting concerns checklist** (security, performance, observability) |

### Tier 3 — Nice to have

| Skill | Change |
|-------|--------|
| write-design | Tiered doc weight (lightweight vs heavyweight based on scope) |
| write-design | **Future Possibilities** section |
| plan-design | **Scope boundaries** per PR (what it does NOT do) |
| plan-design | **Rollback notes** for high-risk PRs |
| review-design | **Confidence levels** on verdicts (HIGH for verifiable, MODERATE for judgment-based) |

---

## Deferred

- **implement-design research** — lost to connectivity issues. The skill
  is already solid; the biggest wins are in the upstream skills (write,
  plan, review) that feed into it.
- **Spec-Driven Development alignment** — the SDD pattern (GitHub
  Spec-Kit, Kiro, Tessl) is worth a deeper analysis. Our skill suite
  is already spec-first; understanding spec-anchored and spec-as-source
  could inform future evolution.

---

## Sources

- [Design Docs at Google](https://www.industrialempathy.com/posts/design-docs-at-google/) — Malte Ubl
- [Design Doc: A Design Doc](https://www.industrialempathy.com/posts/design-doc-a-design-doc/)
- [Things I Learned at Google: Design Docs](https://ryanmadden.net/things-i-learned-at-google-design-docs/)
- [How to Write a Good Software Design Document](https://www.freecodecamp.org/news/how-to-write-a-good-software-design-document-66fcf019569c/) — Angela Zhang
- [Writing a Good Design Document](https://grantslatton.com/how-to-design-document) — Grant Slatton
- [Engineering Planning with RFCs, Design Documents and ADRs](https://newsletter.pragmaticengineer.com/p/rfcs-and-design-docs) — Pragmatic Engineer
- [Scaling Engineering Teams via RFCs](https://blog.pragmaticengineer.com/scaling-engineering-teams-via-writing-things-down-rfcs/) — Uber
- [Rust RFC Template](https://github.com/rust-lang/rfcs/blob/master/0000-template.md)
- [Rust RFC 2333: Prior Art](https://rust-lang.github.io/rfcs/2333-prior-art.html)
- [Rust RFC 2561: Future Possibilities](https://rust-lang.github.io/rfcs/2561-future-possibilities.html)
- [PEP 1](https://peps.python.org/pep-0001/)
- [React RFC Process](https://legacy.reactjs.org/blog/2017/12/07/introducing-the-react-rfc-process.html)
- [ADR GitHub org](https://adr.github.io/)
- [Martin Fowler on ADRs](https://martinfowler.com/bliki/ArchitectureDecisionRecord.html)
- [GitHub Spec-Kit](https://github.com/github/spec-kit)
- [Spec-Driven Development (GitHub Blog)](https://github.blog/ai-and-ml/generative-ai/spec-driven-development-with-ai-get-started-with-a-new-open-source-toolkit/)
- [Martin Fowler: Understanding SDD Tools](https://martinfowler.com/articles/exploring-gen-ai/sdd-3-tools.html)
- [How to Write a Good Spec for AI Agents](https://addyosmani.com/blog/good-spec/) — Addy Osmani
- [Improving Design Reviews at Google (ASE 2023)](https://dl.acm.org/doi/10.1109/ASE56229.2023.00066)
- [Mercari Production Readiness Checklist](https://github.com/mercari/production-readiness-checklist)
- [Software Engineering at Google](https://abseil.io/resources/swe-book) — Winters, Manshreck, Wright (2020)
- [NASA SW-FMEA](https://swehb.nasa.gov/display/SWEHBVD/8.5+-+SW+Failure+Modes+and+Effects+Analysis/)
