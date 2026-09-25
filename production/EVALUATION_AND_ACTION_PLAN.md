# PyStreamFlow — Evaluation & Action Plan

Prepared: 2026-09-11
Scope: full codebase (`pystreamflow/` core engine, ~90 node types, FastAPI server + node-editor UI, MCP server, CLI), plus the existing `tests/` suite.

## How this evaluation was done

Your computer's local shell bridge (`device_bash`) is currently down — Anthropic is tracking a Windows update (Sept 8) that broke it for this session. So instead of editing in place, I staged the whole project into my own sandbox, installed it in a clean virtualenv, and ran the real test suite and a static lint pass, in addition to reading the source. That mattered: several of the bugs below (a hard crash in `config.py`, a session-manager crash, ~30% of the test suite hanging) only showed up once I actually *ran* things — reading the code alone made them look fine.

Because I couldn't get a shell on your machine, I could not run `git init` there. **This project currently has no version control**, which means none of this work has a safety net beyond what I'm giving you here. I've attached `pystreamflow_claude_backup_2026-09-11.zip`, a full snapshot of the project as it stood before I touched anything. Please keep it until you've set up git (see "Immediate next steps" at the end).

Per your direction, this round is **evaluation + plan first**. I made a small number of clearly-scoped, low-risk fixes (listed below, all verified against the test suite) and stopped there — the larger refactor, the wiring/inline-edit UI rework, and the full test rewrite are all laid out as phases for you to greenlight.

---

## 1. Baseline: does it actually work?

I ran `pytest` with coverage, using a per-test timeout (the default run has no timeout, so it just hangs — see 2.2 below).

| | Before my fixes | After my fixes |
|---|---|---|
| Tests collected | 69 (1 file failed to import) | 70 |
| Passing | 47 | 49 |
| Failing/hanging | 23 (33%) | 21 (30%) |
| Line coverage | 38.9% | 39.1% |
| `pip install -e ".[dev]"` then `pytest` | crashes on collection | runs cleanly |

The project's own `pyproject.toml` sets a coverage gate of 50% (`fail_under = 50`) — the suite has never actually met its own bar. The 21 remaining failures aren't flaky; they're deterministic, and they all trace back to one architectural issue (2.2).

---

## 2. Critical bugs (verified, not guessed)

### 2.1 The node-editor's "Trigger Wiring" / "Endpoint Wiring" feature cannot work — FIXED

This is the headline bug, and it's directly the thing you asked me to improve. `pystreamflow/api/ui.html` has two dedicated buttons, "Trigger Wiring" and "Endpoint Wiring", plus purple "attribute" handles on nodes. Dragging a wire while one of these is active tags the edge with `type: 'control'` / `'endpoint'` and sends it to the backend.

But the backend's `Edge` dataclass (`pystreamflow/core/models.py`) never had a `type` field:

```python
@dataclass
class Edge:
    source: str
    target: str
    source_port: str = "out"
    target_port: str = "in"
```

Both `POST /workflows` (`api/server.py`) and workflow-file loading (`core/persistence.py`) do `Edge(**e)` on whatever the UI or a YAML file hands them. I reproduced it directly:

```
>>> Edge(**{'source':'a','target':'b','type':'control', ...})
TypeError: Edge.__init__() got an unexpected keyword argument 'type'
```

So the moment a user drags a trigger or endpoint wire and saves or runs the workflow, the backend throws. The two headline wiring-mode buttons in the UI have never worked end-to-end. **Fixed**: `Edge` now has `type: str = "data"`, defaulting so old workflow files still load. This unblocks the feature at the data-model level; Phase 2/3 below is what makes the *interaction* good, not just non-crashing (see 4.3 for what's still wrong with the UX itself).

### 2.2 Calling a node's `start()` directly deadlocks — not fixed, needs a design decision

`BaseNode.start()` (`core/node.py`) is written as a loop that runs forever — it awaits `self.process()`, and most nodes' `process()` is itself a `while self._running:` loop. That's fine *if* you always schedule it as a background task (`asyncio.create_task(node.start())`), which is what `Engine.run()` does for source nodes and what the `_auto_start` wrapper does for downstream nodes.

But nothing stops you from `await`-ing it directly, and a third of the test suite does exactly that:

```python
node = ScriptedOutputNode('test_script', {'script': 'return data * 2'})
await node.init()
await node.start()        # <-- blocks here forever
pipe = node.inputs['in']  # <-- never reached
```

`process()` for this node is `while self._running and 'in' not in self.inputs: await asyncio.sleep(0.5)` — waiting for a pipe that can only be added *after* `start()` returns. It never will. I confirmed this is a real, deterministic deadlock (not a flaky timing issue) by running the suite twice with a signal-based per-test timeout; the same 20 tests hang every time in `test_new_nodes.py`, `test_nodes_coverage.py`, and `test_trigger_advanced.py`.

This isn't just a test-writing mistake — it's a real API footgun. There's no `node.start_background()` or similar, no documented convention, and the auto-start machinery that papers over it for the *engine's* code path doesn't help anyone calling nodes directly (which the API server's `/nodes` and `/nodes/connect` endpoints do). This needs an actual design decision (Phase 1) about what "starting a node" means and how you interact with one afterward — not a one-line patch.

### 2.3 Engine has no supervision of running nodes

`Engine.run()` only `await`s the nodes it starts directly (source nodes). Everything downstream is auto-started via a bare `asyncio.create_task(self.start())` inside `node.py`'s `_ensure_started`, with the returned task never stored anywhere. Consequences:

- `Engine.run()` can return while most of the graph is still running in detached tasks — there's no way to know when a workflow is actually "done," and no graceful shutdown path.
- If a downstream node's `process()` raises (after exhausting `retries`), the exception has nowhere to go — Python will print "Task exception was never retrieved" to stderr and the node just silently stops. The UI's health LED would presumably still show whatever it last showed.
- Same pattern in `emit()`: `asyncio.create_task(pipe.put(item))` is fire-and-forget with no reference kept, which also means backpressure from a bounded `Pipe` (`maxsize>0`) is invisible to the emitting node — it just schedules a task that could block forever without the node knowing.

### 2.4 `config.py` crashes on import with the pinned dependency version — FIXED

```python
try:
    from pydantic_settings import BaseSettings
except ImportError:
    from pydantic import BaseSettings
```

`pydantic-settings` was never listed in `pyproject.toml`, so the `except` branch always ran — and on pydantic ≥2.13 (what `pip install -e ".[dev]"` gives you today), `pydantic.BaseSettings` doesn't raise `ImportError` when accessed, it raises `pydantic.errors.PydanticImportError` from the module's `__getattr__`. That happens *inside* the except block, so it isn't caught by anything, and it took down `tests/test_misc_coverage.py`'s entire collection. **Fixed**: added `pydantic-settings` as a real dependency, and replaced the broken fallback with a small self-contained shim (reads `Settings` fields from `PSF_*` env vars) so the module still works even if `pydantic-settings` is somehow absent. Verified both paths.

### 2.5 `asyncio.get_event_loop()` used where there's no running loop — FIXED (6 sites)

`core/session_manager.py`'s `Session.__init__` — a plain, synchronous constructor — called `asyncio.get_event_loop().time()`. On Python ≥3.10, calling `get_event_loop()` with no running loop and no loop ever set on the thread raises `RuntimeError: There is no current event loop in thread 'MainThread'`. I reproduced this directly: `SessionManager().create(...)` crashes every time it's called from sync code (which is exactly how `tests/test_coverage.py::TestSessionManager` calls it, and plausibly how a sync FastAPI dependency or the CLI could call it too).

The same pattern showed up in `core/node.py` (`start()` and `health()`), `nodes/clock_node.py`, `nodes/json_input.py`, and `nodes/table.py`. Two different underlying issues, both fixed:
- Where the code wanted *elapsed time* (`node.py`'s uptime tracking, `table.py`'s emit-interval timer, `session_manager.py`'s `created_at`), I swapped in `time.monotonic()`, which needs no event loop and is the correct tool for measuring durations.
- Where the code wanted a *timestamp* for downstream consumers (`clock_node.py`'s `timestamp` field, `json_input.py`'s `ts` field), the old code was also **semantically wrong**, not just fragile: `loop.time()` is seconds-since-the-loop-started, not a real timestamp, so anything downstream reading that field as wall-clock time was getting nonsense. Fixed to `time.time()`.

### 2.6 A node type is invisible to the engine — FIXED

`Engine.run()` hand-maintains a ~90-entry `dict` mapping type name → class, duplicating what `pystreamflow/nodes/__init__.py` already exports via `__all__`. `UserInputNode` is properly exported from the package (and the API server's `/nodes` endpoint, which *does* build its registry dynamically from `__all__`, can create one) — but it was simply missing from the engine's hand-copied dict. Any workflow YAML referencing `UserInputNode` would silently fall back to a no-op `GenericNode` (see `engine.py`'s fallback branch) instead of erroring — the workflow "runs" and quietly does nothing. **Fixed** by adding the missing entry, but see 4.1 for why this class of bug will keep recurring until the registry is unified.

### 2.7 Dead, broken concurrency primitives

`core/stream.py` defines `Fork` and `Merge` helper classes that are never imported or used anywhere (the real `ForkNode`/`MergeNode` in `nodes/` implement their own polling loops instead). `Merge.run()` is also just wrong — `queues.append(asyncio.create_task(d.get()))` calls `.get()` on `d`, which is the *completed Task* from `asyncio.wait`, not the source `Pipe`; `asyncio.Task` has no `.get()` method, so this throws `AttributeError` the instant it would ever run. Not urgent (nothing calls it), but it's the kind of "looks like infrastructure, is actually landmine" code that should be deleted rather than fixed, since fixing it would just be polishing something nobody uses.

### 2.8 Pause/resume isn't pause/resume

`handle_control`'s `pause` action calls `self.stop()` (sets `_running=False`), and `resume` calls `self.start()` again from scratch — which re-runs `init()` and restarts `process()` as a brand-new coroutine. For any node with internal state accumulated during `process()` (counters, buffers, open connections), pause/resume silently resets that state rather than suspending and continuing. The UI has a "Pause" button that wires straight into this.

### 2.9 Unsandboxed `exec()` of user-supplied config

`nodes/scripted_output.py` and `nodes/modifier_script.py` `exec()` a `script` string taken directly from node config with no restrictions. This is a reasonable *feature* (script nodes are supposed to run code), but it's worth being explicit that any workflow YAML — including one imported from a file, a URL, or a shared session — has full code-execution power the moment it's loaded. Not something to "fix" outright (that's the point of the node), but it should be a documented trust boundary, and the `/parse_yaml` and workflow-import endpoints should probably say so.

### 2.10 Widespread silent error-swallowing

A static pass (`ruff`) found 58 "blind except" (`except Exception:` with no re-raise/log), 5 `except: pass`, and 3 bare `except:` across the node implementations — on top of the ones I already ran into by hand (e.g. `BaseNode.emit()` silently drops a `bytes_out` calculation error). Individually mostly harmless; together they mean node failures tend to disappear rather than surface, which will make Phase 1's supervision work (2.3) much less useful unless cleaned up alongside it.

---

## 3. What I fixed just now (verified against the test suite, zero behavior change beyond the bug itself)

| File | Fix |
|---|---|
| `pystreamflow/core/models.py` | Added `Edge.type: str = "data"` — stops the wiring-feature crash (2.1) |
| `pystreamflow/core/config.py` | Replaced the broken `BaseSettings` fallback with a working shim (2.4) |
| `pyproject.toml` | Added `pydantic-settings`, `pytest-asyncio`, `pytest-timeout` as real declared dependencies |
| `pystreamflow/core/engine.py` | Registered the missing `UserInputNode` (2.6) |
| `pystreamflow/core/node.py` | `get_event_loop().time()` → `time.monotonic()` in `start()`/`health()` (2.5) |
| `pystreamflow/core/session_manager.py` | Same fix for `Session.created_at` — was crashing session creation from sync code (2.5) |
| `pystreamflow/nodes/table.py` | Same fix for the emit-interval timer (2.5) |
| `pystreamflow/nodes/clock_node.py` | `timestamp` field: loop time → real wall-clock `time.time()` (2.5) |
| `pystreamflow/nodes/json_input.py` | Same fix for the `ts` field (2.5) |

Net effect: the test suite goes from 23 failing/hanging to 21 (the remaining 21 are all instances of 2.2, which is a design question, not a quick patch), coverage collection no longer errors out, and `pip install -e ".[dev]" && pytest` works without any manual `pip install` step. I have **not** touched the wiring UX, the node lifecycle model, the registry duplication, or the test suite itself — those are the phases below.

---

## 4. Where the duplication and cleanup opportunities are

### 4.1 Three separate node registries that drift

1. `pystreamflow/core/engine.py` — a hand-written ~90-entry dict (source of the `UserInputNode` bug)
2. `pystreamflow/api/server.py`'s `/nodes` endpoint — built dynamically from `nodes.__all__` (correct, but only used there)
3. `pystreamflow/mcp/server.py` — presumably its own copy too (worth checking during Phase 1)

These should collapse to one: build the registry once, dynamically, from `nodes/__init__.py`, and have `engine.py`, `api/server.py`, and `mcp/server.py` all import it. This is the single highest-leverage refactor for "re-use as much code as possible" — it's also what will stop bugs like 2.6 from recurring every time a node type is added.

### 4.2 Near-identical node families

The `numeric_*` family (`numeric_add.py`, `numeric_sub.py`, `numeric_mul.py`, `numeric_div.py`, `numeric_mod.py`, `numeric_pow.py`, `numeric_min.py`, `numeric_max.py` — 8 files, ~20 lines each) and the `text_*` family (`text_upper.py`, `text_lower.py`, `text_trim.py`, `text_strip.py`, `text_title.py`, `text_reverse.py`, `text_split.py`, `text_join.py`, `text_replace.py`, `text_substring.py` — 10 files) are each a copy-pasted single-operation wrapper around a two-input-port or one-input-port pattern. Same for the five classes in `trigger_advanced.py`, which each redefine an identical `_trigger()` method that does nothing but look up `target_node_id` in the node registry and call `handle_control`. All three families are strong candidates for a shared base/mixin (a `BinaryOpNode(op_fn)` and `UnaryOpNode(op_fn)` base, and a `TriggerActionMixin._trigger()`), cutting roughly 20 files down to small declarative registrations.

### 4.3 The wiring/inline-editing UX gaps you asked about specifically

Beyond the crash in 2.1, I read through `ui.html`'s interaction code and found the actual UX is more limited than the two mode-toggle buttons suggest:

- **Edge type is chosen by a global mode toggle, not by what you drag.** Whether a dragged wire becomes "data," "control," or "endpoint" depends on whether the "Trigger Wiring" / "Endpoint Wiring" button was clicked beforehand — not on which handle (attribute vs. IO vs. trigger) you actually grabbed. That's backwards from how this should feel: dragging from an attribute handle should always make an attribute wire.
- **You can't target a specific input port.** `window.onmouseup`'s target-detection only checks distance to each node's *first* input handle position (`hy = n.y + 22`, i.e. always `in0`); the resulting edge is hardcoded to `target_port: 'in0'` regardless of which of a node's several input handles you actually dropped on. Multi-input nodes (Merge, And/Or/Xor, etc.) can't be wired to a specific input this way.
- **Two disconnected places to edit the same node config**: the Inspector panel's raw "Config JSON" textarea, and the expanded on-canvas node's auto-generated key/value form (built by reflecting over whatever keys already exist in `config`). They don't share state live and can drift.
- **No per-node-type field schema.** The auto-form special-cases exactly two key names (`mode`, `action`) with a hardcoded `['auto','manual','off']` dropdown regardless of what the node actually means by them; every other field is a generic text/number/checkbox box inferred from its current JS type. There's no way to see, e.g., that `TriggerThresholdNode.threshold` is an integer count or that `TriggerIfNode.condition` should be a dropdown of `truthy/equals/contains/regex` — you have to already know the node's source code.
- **Attribute handles are decoration.** The purple "attr" handles render from `config.attributes` and can be dragged *from*, but nothing on the backend gives them meaning beyond becoming a same-shaped `Edge` as everything else (and per 2.1, that edge couldn't even save until just now).
- **Trigger targeting bypasses the graph entirely.** Every trigger node (`TriggerIfNode`, `TriggerThresholdNode`, `TriggerDebounceNode`, `TriggerPulseNode`, `TriggerToggleNode`) reaches its target through a raw `target_node_id` string typed into config — not through a drawn edge at all. So even after 2.1's fix, "Trigger Wiring" mode produces a cosmetic `type: 'control'` edge on screen that the trigger nodes themselves don't read; you still have to separately type the target node's random ID into a JSON field to make triggering actually work. This is very likely the root cause of the wiring feature feeling broken/unusable — because functionally, most of it is.

This is the concrete gap between what exists today and what you asked for ("wiring of nodes by attribute, trigger, io" and "inline editing and viewing of node attributes, content, trigger types"). Phases 2–3 below address it directly.

---

## 5. Phased action plan

I've ordered this so each phase is independently shippable and testable, and so the riskier UI/engine work happens after the ground underneath it (lifecycle, registry) is solid.

**Phase 0 — done today.** Baseline established, safety-net backup created, 9 quick-win bugs fixed and verified (section 3).

**Phase 1 — Core correctness (do this before touching the UI).**
Fix the node lifecycle model so "start a node" has one clear, documented meaning (2.2); make `Engine` actually supervise the tasks it spawns — track them, propagate/log exceptions instead of swallowing them, support graceful stop (2.3); fix pause/resume to actually suspend rather than restart (2.8); unify the three node registries into one (4.1); clean up the blind-except patterns that would otherwise hide bugs from Phase 1's new supervision (2.10); delete the dead/broken `Fork`/`Merge` classes in `stream.py` (2.7). This phase touches no user-facing behavior — it's the foundation the wiring feature and the tests both need to be trustworthy.

**Phase 2 — Backend model for attribute/trigger/IO wiring.**
Give trigger targeting a real edge-based path instead of a hand-typed `target_node_id` string — trigger nodes should be able to resolve their target from an actual `control`-type edge in the graph, with the config field kept only as a fallback/override. Extend the API (`/nodes/connect`, `/workflows`) to validate edge `type` against what the source/target node's ports actually support, so a bad wire is rejected with a clear error instead of silently doing nothing. Add a lightweight per-node-type "port schema" (what inputs/outputs/attributes/triggers each node type exposes, and their config field types/enums) that both the backend and the future UI form-builder can read from one place, so node authors declare it once instead of the UI guessing from JS runtime types.

**Phase 3 — Node-editor UI overhaul, within the existing single-file `ui.html`** (per your call: rework the current architecture rather than a full rewrite).
- Make edge type follow the handle you grab, not a global mode toggle: drag from an attribute handle → attribute edge, from a trigger port → control edge, from a normal IO handle → data edge, with the mode buttons becoming an optional override rather than the only way to choose.
- Fix target-port detection to hit-test each of a node's actual input handles, not just the first, so multi-input nodes can be wired precisely.
- Replace the dual JSON-textarea/auto-form editing surfaces with one inline editor per node, driven by the Phase 2 port schema — real dropdowns for enum fields (trigger condition, action, comparison operator), typed inputs for numeric/boolean fields, and a proper code editor widget for script/template content instead of a bare `<textarea>`.
- Surface trigger targets as a pick-a-node-from-the-graph control (backed by the Phase 2 edge-based targeting) instead of asking the user to type a random node ID.
- Keep the existing canvas/pan/zoom/status-LED machinery, which is solid and doesn't need touching.

**Phase 4 — Code-reuse refactor.**
Collapse the `numeric_*` and `text_*` families onto shared base classes (4.2); collapse `trigger_advanced.py`'s repeated `_trigger()` into a mixin; apply the Phase 1 registry unification everywhere it's currently duplicated.

**Phase 5 — Sophisticated test suite + coverage.**
Fix the ~20 tests that deadlock by giving them a proper async-node test fixture (start as a background task, wire pipes, `await` a bounded amount of time, always tear down) rather than `await`-ing `start()` directly — this alone should recover most of the "hanging" 30%. Then add real coverage for the many node types currently at 15–25% (most of `nodes/`), the engine's topological sort/cycle detection, the session manager, and the API/MCP servers (`mcp/server.py` is at 11% today). Add dedicated tests for whatever Phase 2/3 wiring behavior we build, including the crash this evaluation found in 2.1 so it can't silently regress. Target: 50% (the project's own declared, currently-unmet gate) → 75% → 90%+ as each phase lands, with `pytest --cov` wired into the existing GitHub Actions workflow (`.github/workflows/ci.yml`) so it's enforced automatically rather than checked by hand.

**Phase 6 — Hardening pass.**
Make the `exec()`-based script trust boundary explicit (2.9) — at minimum, clear documentation and a UI warning when importing a workflow containing script nodes; optionally a restricted-globals sandbox if untrusted workflow sharing is a real use case for you. Audit the MQTT/socket/subprocess-based nodes for the same class of input-validation gaps.

---

## Immediate next steps

1. **Set up git.** I couldn't do this myself (see the note at the top) — once your computer's shell bridge is back, ask me and I'll initialize the repo and make an initial commit from the current (post-quick-fix) state, or you can run `git init && git add -A && git commit -m "initial commit"` yourself. Until then, keep the attached backup zip.
2. **Review this plan** and tell me which phase(s) to start on, or reorder if you'd rather tackle the UI before the engine work — I'd recommend against that, since Phase 3's per-port hit-testing and schema-driven forms both depend on Phase 1/2 groundwork, but it's your call.
3. I'll pick up wherever you point me and keep working the same way — verify with the real test suite before calling anything done, and report coverage deltas honestly rather than gaming the number (which is, candidly, what several of the existing `test_*coverage*.py` files look like they were written to do — they hit lines without asserting much, and a third of them don't even pass).
