# Burr Research

Research on Apache Burr (incubating), the state machine framework by DAGWorks
(same team behind Hamilton). Version 0.41.0 as of research date.

---

## Core Abstractions

Burr has four fundamental building blocks:

**Action** -- The atomic unit of computation. Every action has two
responsibilities: `run()` (compute a result) and `update()` (modify state based
on that result). Actions declare `reads` (state keys consumed) and `writes`
(state keys produced), enabling the framework to validate the graph at build
time.

Two authoring styles:

```python
# Function-based (terse, common case)
@action(reads=["chat_history"], writes=["response", "chat_history"])
def ai_response(state: State) -> State:
    response = call_llm(state["chat_history"])
    return state.update(response=response).append(chat_history={"role": "assistant", "content": response})

# Class-based (when you need parameterization or inheritance)
class CustomAction(Action):
    @property
    def reads(self) -> list[str]:
        return ["input"]

    def run(self, state: State) -> dict:
        return {"output": transform(state["input"])}

    @property
    def writes(self) -> list[str]:
        return ["output"]

    def update(self, result: dict, state: State) -> State:
        return state.update(**result)
```

Function-based actions can also return a `(result_dict, State)` tuple when the
caller needs to inspect the intermediate result separately from the state
mutation.

There is also `@action.pydantic` which infers reads/writes from typed Pydantic
models and allows in-place mutation of the model within the action body (the
framework handles immutability under the hood).

**State** -- An immutable dict-like object. Every mutation returns a new
`State` instance. The API:

- `state.update(key=value)` -- set/overwrite
- `state.append(key=value)` -- append to a list
- `state.extend(key=values)` -- extend a list
- `state.increment(key=n)` -- increment a numeric value
- `state.wipe(keep=[...])` or `state.wipe(delete=[...])` -- field-level cleanup
- `state["key"]` and `state.get_all()` -- read access

Calling these without capturing the return value is a no-op (a common gotcha).
State must ultimately be JSON-serializable for persistence; custom
serializers/deserializers exist for non-JSON types.

**Transitions** -- Directed edges between actions. Defined as tuples of
`(from_action, to_action)` or `(from_action, to_action, condition)`. Three
condition helpers:

- `when(key=value)` -- true when state has that value
- `expr("count < 10")` -- expression evaluated against state
- `default` -- catch-all, always true

Conditions are evaluated in definition order; first match wins. Conditions can
be negated with `~` (e.g., `~when(safe=False)`). Multiple source actions can be
grouped: `(["action_a", "action_b"], "action_c")`.

**Application** -- The assembled state machine. Built via `ApplicationBuilder`:

```python
app = (
    ApplicationBuilder()
    .with_actions(human_input, ai_response)
    .with_transitions(
        ("human_input", "ai_response"),
        ("ai_response", "human_input"),
    )
    .with_state(chat_history=[])
    .with_entrypoint("human_input")
    .build()
)
```

Minimum requirements: actions, transitions, and an entrypoint.

There is also a `GraphBuilder` that separates the topology definition from
runtime concerns (persistence, tracking, identifiers), enabling one graph
definition shared across multiple Application instances.

---

## Execution Model

Burr is a **state machine**, not a DAG. Actions are nodes, transitions are
edges, and the machine can have cycles (which DAGs cannot). At each step, the
framework evaluates outgoing transitions from the current action, picks the
first matching condition, and moves to the next action.

Four execution APIs, each with sync and async variants:

- **`step()` / `astep()`** -- Execute one action. Returns `(action, result,
  state)`. The most granular control.
- **`iterate()` / `aiterate()`** -- Generator that calls `step()` repeatedly.
  Controlled by `halt_after` (stop after running a named action) and
  `halt_before` (stop before running a named action).
- **`run()` / `arun()`** -- Runs `iterate()` to completion. Returns final
  state.
- **`stream_result()` / `astream_result()`** -- For streaming actions. Returns
  a `StreamingResultContainer` that yields intermediate results and provides a
  `.get()` for the final result/state.

All execution methods accept `inputs={}` for passing external data (user
prompts, API keys, etc.) into actions. Unbound action parameters (not supplied
via `bind()`) become required runtime inputs.

Async support: class-based actions set `is_async = True` and implement `async
def run()`. Function-based actions use `async def` directly. The framework runs
async actions with the `a*` execution methods.

There is no built-in distributed execution. Burr runs in-process. For
distributed scenarios, you wrap Burr applications in whatever serving layer you
prefer (FastAPI, Ray, etc.).

---

## Data/State Model

State is the central data structure. Key design decisions:

**Immutability** -- Every state mutation returns a new `State` object. This is
the core invariant. It enables: safe inspection at any point in time,
persistence without coordination, time-travel debugging, and fork-based replay.

**Reads/writes declarations** -- Actions declare which state keys they read and
write. This serves as documentation, enables validation (the framework can walk
the graph and check that no action reads a key that no prior action writes), and
scopes what each action can see. The framework enforces these declarations at
runtime -- if an action reads a key not in its `reads` list, it will error. (As
of v0.41 there is also AST-based validation that checks the action body against
its declared reads.)

**No global state** -- All data flows through the State object. There is no
ambient context or shared mutable store.

**Typed state (optional)** -- The `@action.pydantic` decorator allows using
Pydantic models for state, giving type safety and IDE support. However, some
features (like parallelism) do not yet work with typed state.

**Result vs State separation** -- Actions produce both a `result` dict
(intermediate output for display/logging) and a `State` update. This
separation means the tracking UI can show action results without them
necessarily persisting in state.

---

## Persistence & Tracking

### State Persistence

Pluggable persisters save/load state keyed by `(app_id, partition_key,
sequence_id)`:

- `app_id` -- unique per application instance (auto-UUID if not set)
- `partition_key` -- optional grouping (e.g., user ID)
- `sequence_id` -- auto-incrementing step counter

Available persisters: **SQLite**, **PostgreSQL**, **Redis**, **MongoDB**,
**S3**, **file-based** (local dev). Async variants exist for PostgreSQL
(`asyncpg`) and SQLite (`aiosqlite`).

Configuration:

```python
persister = SQLLitePersister(db_path="app.db", table_name="burr_state")
persister.initialize()

app = (
    ApplicationBuilder()
    .with_actions(...)
    .with_transitions(...)
    .initialize_from(
        persister,
        resume_at_next_action=True,
        default_state={"chat_history": []},
        default_entrypoint="human_input",
    )
    .with_state_persister(persister)
    .with_identifiers(app_id="my-app", partition_key="user-123")
    .build()
)
```

`initialize_from()` loads the last persisted state if it exists, otherwise uses
defaults. `with_state_persister()` saves state after every action.

**State forking** -- `fork_from_app_id` and `fork_from_sequence_id` let you
create a new application branching from a specific point in a prior run. This
enables time-travel debugging: load a historical state, change inputs, and
replay.

Custom persisters implement `BaseStatePersister` (sync) or
`AsyncBaseStatePersister` (async).

### Tracking & Observability

The tracking system is separate from persistence. Enable it with
`.with_tracker("local", project="my-project")` or by passing a
`LocalTrackingClient` instance.

Data model: **Project** > **Application** > **Steps**. Each step records:
action name, inputs, result, state before/after, timing.

The **Burr UI** (`burr` CLI command, port 7241) provides:

- State machine graph visualization
- Step-by-step execution timeline with waterfall timing view
- State inspection at any historical point
- Action input/output inspection
- OpenTelemetry-compatible spans and traces
- Arbitrary attribute logging (prompts, token counts, etc.)
- Insights dashboard (per-step and per-span metrics)
- Notebook integration (`%burr_ui` magic)
- FastAPI embedding (`mount_burr_ui()`)
- Pre-loaded demo applications for exploration

The tracking client also serves as a state loader for `initialize_from()`,
meaning the tracking data itself can be used for persistence in local dev.

**OpenTelemetry** -- Burr can log spans in OpenTelemetry-compatible format and
attach arbitrary attributes during execution.

**Hooks / Lifecycle** -- Pre and post execution hooks (`PreRunStepHook`,
`PostRunStepHook`) allow injecting custom behavior (logging, metrics,
sync to external systems). Hooks receive action, state, result, sequence_id,
and any exception. Hooks are passed via `.with_hooks()` on the builder.

---

## Composition & Reuse

### Sub-Applications (Recursive Burr)

Burr supports nesting: a parent application's action can build and run a child
Application internally. The pattern:

```python
@action(reads=["params"], writes=["results"])
def run_sub_apps(state: State, __context: ApplicationContext) -> State:
    results = []
    for param_set in state["params"]:
        sub = (
            ApplicationBuilder()
            .with_actions(...)
            .with_transitions(...)
            .with_tracker(__context.tracker.copy())
            .with_spawning_parent(
                __context.app_id, __context.sequence_id, __context.partition_key
            )
            .with_entrypoint(...)
            .with_state(...)
            .build()
        )
        _, _, sub_state = sub.run(halt_after=["final"])
        results.append(sub_state["output"])
    return state.update(results=results)
```

Key points:
- `__context: ApplicationContext` is injected automatically
- `tracker.copy()` prevents state corruption between parent/child
- `with_spawning_parent()` links child to parent for UI visualization
- Persisters are NOT automatically wired through; must be done manually

This is currently the manual/"low-level" approach. The team has stated that
first-class ergonomic constructs for recursive applications are planned.

### Parallelism (Map-Reduce)

Three high-level parallel action types:

- **`MapStates`** -- Same action, multiple state variations (fan-out on state)
- **`MapActions`** -- Different actions, same state (fan-out on actions)
- **`MapActionsAndStates`** -- Cartesian product of actions x states

Each requires implementing `action()`/`actions()`, `state()`/`states()`, and
`reduce()` (the join). Execution uses `ThreadPoolExecutor` by default,
configurable via `.with_parallel_executor()` or per-action `executor()`
override.

`RunnableGraph` wraps a subgraph (built via `GraphBuilder`) so parallel actions
can fan out over multi-step workflows, not just single actions.

Async parallelism is supported. Tracking and persistence cascade from parent to
child by default but can be overridden.

**Limitation**: Parallelism does not currently work with typed (Pydantic) state.

### Graph Reuse

`GraphBuilder` produces a `Graph` object (topology only) that can be shared
across multiple `Application` instances, each with different runtime
configuration (persistence, tracking, identifiers).

### Action Reuse

`bind()` partially applies parameters to actions, enabling reuse with different
configurations:

```python
query_llm.bind(model="gpt-4").with_name("gpt4_query")
query_llm.bind(model="claude").with_name("claude_query")
```

---

## Streaming & Human-in-the-Loop

### Streaming

The `@streaming_action` decorator enables incremental output:

```python
@streaming_action(reads=["prompt"], writes=["response"])
def streaming_chat(state: State) -> Generator:
    full = ""
    for chunk in call_llm_streaming(state["prompt"]):
        full += chunk
        yield {"response": chunk}, None          # intermediate (no state update)
    yield {"response": full}, state.update(response=full)  # final
```

Consumed via:

```python
action, streaming_container = app.stream_result(
    halt_after=["streaming_chat"], inputs={"prompt": "Hello"}
)
for chunk in streaming_container:
    print(chunk)  # incremental
result, final_state = streaming_container.get()  # final
```

State updates and hooks execute only after the iterator is fully consumed.
Non-streaming actions called via `stream_result()` return a container with an
empty iterator and immediate final result. Streaming actions used as
intermediate graph nodes (not halted on) execute as normal non-streaming
actions.

Class-based streaming: inherit `StreamingAction`, implement `stream_run()`
(yields result dicts) and `update()` (handles final state mutation).

### Human-in-the-Loop

The pattern uses `halt_before` to pause execution before actions that need user
input, then resumes by calling `run()` again with new `inputs`:

```python
# First run: pauses before "get_feedback"
action, result, state = app.run(
    halt_before=["get_feedback"],
    halt_after=["final_result"],
    inputs={"prompt": initial_prompt}
)

# User provides feedback, resume:
action, result, state = app.run(
    halt_before=["get_feedback"],
    halt_after=["final_result"],
    inputs={"feedback": user_feedback}
)
```

Combined with persistence, this enables multi-session workflows: pause, persist,
user goes away, comes back, load state, resume. The email assistant example
demonstrates this with FastAPI endpoints for each interaction point.

Action tagging (`tags=["response_to_display"]`) lets you halt on logical
groups via `@tag:response_to_display`.

---

## Integration with Hamilton

Burr and Hamilton are complementary frameworks from the same team (both now
Apache incubating projects):

- **Hamilton** -- Models computation as a DAG of pure functions. Each function
  is a node; dependencies are inferred from parameter names. No cycles, no
  mutable state. Best for: data transformations, feature engineering, ML
  pipelines, ETL.

- **Burr** -- Models computation as a state machine with cycles, mutable
  (immutable-API) state, and conditional transitions. Best for: agents,
  chatbots, interactive workflows, anything with decision loops.

**Origin story**: Burr was originally built as a "harness to handle state
between executions of Hamilton DAGs (because DAGs don't have cycles)."

**Integration pattern**: Use Hamilton dataflows as the internal computation
within Burr actions. The Burr action handles state management and transition
logic; Hamilton handles the pure functional computation within a single action.

```
Burr Application (state machine, cycles, persistence)
  -> Action A (uses Hamilton DAG internally for data transformation)
  -> Action B (uses Hamilton DAG internally for feature computation)
  -> Action C (plain Python, calls an LLM)
```

When to use which:
- Pure data pipeline with no cycles or state: Hamilton alone
- Interactive/stateful application: Burr (optionally with Hamilton inside actions)
- Both: Burr for orchestration, Hamilton for the data-heavy parts

---

## Key Design Decisions

**State machine paradigm** -- Burr's most opinionated choice. Everything is
modeled as explicit states and transitions. This gives visibility, testability,
and persistence "for free" but requires the developer to think in terms of a
state machine graph upfront.

**Immutable state** -- The `State` object cannot be modified in place. This
enables safe inspection, forking, time-travel, and concurrent reads. The
tradeoff is that every action allocates a new State object (the team has
planned optimizations using a linked-list diff approach).

**Reads/writes declarations** -- Actions must explicitly declare their state
dependencies. This is friction by design: it makes data flow visible and
enables static validation.

**Framework-agnostic** -- Burr has zero required dependencies. It does not
dictate how you call LLMs, manage data, or serve your application. Actions are
plain Python functions. This is a deliberate contrast to frameworks like
LangChain that provide their own abstractions for LLMs, prompts, etc.

**Result/state separation** -- The intermediate result dict is separate from
the state update. This means tracking can capture what an action produced
without that data necessarily living in state.

**Builder pattern** -- The `ApplicationBuilder` fluent API is the only way to
construct applications. This enables validation at build time.

**No distributed execution** -- Burr is intentionally in-process. Distributed
execution is delegated to the serving layer (FastAPI, Ray, etc.).

**Tracking as a first-class citizen** -- The UI and telemetry system are built
into the framework, not bolted on. One line (`.with_tracker(...)`) enables
full observability.

---

## Developer Experience

**Learning curve** -- Low to moderate. The core concepts (action, state,
transition, application) are simple and map to familiar programming concepts.
The function-based API is particularly accessible. The main conceptual leap is
thinking about your application as a state machine graph.

**API surface** -- Small. The core module exports `action`, `State`,
`ApplicationBuilder`, `when`, `expr`, `default`, and a handful of base classes.
No complex DSLs or custom query languages.

**Debugging** -- Strong. The Burr UI provides state inspection at every step,
the immutable state model means you can always reconstruct any historical
state, and fork-based time-travel lets you replay from any point.

**Testing** -- Actions are pure functions of `(State, inputs) -> State`, making
unit testing straightforward. No mocking of framework internals needed.

**Type support** -- The `@action.pydantic` decorator provides typed state with
IDE autocompletion. Plain `@action` uses string-keyed dicts (less type-safe but
more flexible).

**Documentation quality** -- Good. The official docs cover concepts, examples,
and API reference. Multiple blog posts provide architectural guidance. The
examples directory on GitHub is extensive (chatbots, RAG, email assistant,
adventure game, simulations, hyperparameter tuning). There is also a
getting-started tutorial with the CLI that ships demo data.

**Pain points reported by users**:
- The reads/writes declaration can feel verbose for simple actions
- Typed state (Pydantic) does not yet work with all features (e.g., parallelism)
- Recursive/sub-application wiring is manual and verbose
- No built-in retry/exception-handling transitions (on the roadmap)
- No TypeScript SDK yet (planned)

---

## Use Cases

Burr is best suited for:

- **AI agents** -- Tool-calling loops, multi-step reasoning, agent-within-agent
- **Chatbots** -- Stateful conversation with persistence across sessions
- **Human-in-the-loop workflows** -- Approval flows, interactive assistants,
  feedback loops
- **RAG applications** -- Multi-turn retrieval with state tracking
- **Simulations** -- Time-series forecasting, game logic
- **ML experiment tracking** -- Hyperparameter tuning as a state machine
- **Multi-step LLM workflows** -- Content generation pipelines with
  conditional branching

Burr is NOT limited to LLM use cases. It works for any application that
benefits from explicit state management, conditional control flow, and
persistence.

---

## Strengths

- **Explicit state machine model** -- Makes control flow visible, testable, and
  debuggable. No hidden state or implicit transitions.
- **Immutable state with time-travel** -- Fork from any historical point for
  debugging or evaluation.
- **Built-in tracking UI** -- One-line setup for full observability, no external
  vendor required.
- **Zero required dependencies** -- The core library has no deps. Extras are
  optional.
- **Framework-agnostic** -- Works with any LLM provider, any data library, any
  serving framework.
- **Persistence is trivial** -- One line for SQLite/Postgres/Redis/MongoDB
  persistence with automatic resume.
- **Streaming is first-class** -- Not an afterthought. Clean API for
  incremental results.
- **Testability** -- Actions are pure functions. Easy to unit test in isolation.
- **Human-in-the-loop is natural** -- `halt_before` + `inputs` + persistence =
  multi-session interactive workflows.

## Weaknesses

- **State machine thinking required** -- Developers must model their
  application as a graph upfront. This is a cognitive shift from imperative
  "just call functions" style.
- **No distributed execution** -- Single-process only. Horizontal scaling
  requires external orchestration.
- **Recursive applications are verbose** -- Sub-application wiring is manual.
  No first-class composition primitive yet.
- **Typed state limitations** -- Pydantic state does not work with parallelism
  and some other features.
- **No built-in retry/error transitions** -- Exception handling halts
  execution. Retry logic must be implemented manually (on the roadmap).
- **Smaller ecosystem** -- ~2K GitHub stars, ~500K total downloads. Much smaller
  community than LangChain/LangGraph.
- **No native DAG support** -- If your workflow is a pure DAG (no cycles, no
  state), Burr adds overhead vs simpler pipeline tools.
- **State copy overhead** -- Every action creates a new State object. For
  large state, this can be expensive (linked-list optimization planned).

---

## Community & Ecosystem

**Status**: Apache Incubating (as of early 2025). Previously BSD-3 licensed
under DAGWorks-Inc/burr, now Apache 2.0 under apache/burr.

**Metrics** (as of April 2026):
- GitHub stars: ~2,000
- PyPI downloads: 500,000+ total
- Discord: 160+ members
- Contributors: 15+
- Known production users: Coinbase, TaskHuman, Provectus, Watto.ai, Peanut
  Robotics

**Core team**: Elijah ben Izzy and Stefan Krawczyk (DAGWorks Inc., same team
as Hamilton).

**Integrations**: Hamilton, Haystack, LangChain (LCEL), OpenTelemetry,
Streamlit, FastAPI, Graphviz. Persistence: SQLite, PostgreSQL, Redis, MongoDB,
S3.

**Documentation**: Official docs at burr.apache.org. Blog at
blog.dagworks.io. Examples directory covers chatbots, RAG, email assistant,
adventure games, simulations, ML training, YouTube-to-social-media, and more.

**Community sentiment**: Positive among users who adopt it. Common praise:
simpler than LangChain, great debugging UI, clean API. Common criticism:
smaller ecosystem, fewer integrations than LangGraph.

---

## Sources

- https://github.com/apache/burr
- https://burr.apache.org/
- https://blog.dagworks.io/p/burr-develop-stateful-ai-applications
- https://blog.dagworks.io/p/building-interactive-agents-with
- https://blog.dagworks.io/p/burr-ui
- https://blog.dagworks.io/p/travel-back-in-time-with-burr
- https://cwiki.apache.org/confluence/display/INCUBATOR/BurrProposal
- https://news.ycombinator.com/item?id=39917364
