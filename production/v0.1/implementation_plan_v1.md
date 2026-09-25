# PyStreamFlow v1 Implementation Plan

## Project Overview
Python-based modern node graph editor for stream flow routing.

## Goals
- Local web API + node editor
- CLI interface
- On-the-fly rerouting
- YAML configuration
- Daemon mode
- Docker support
- Packages, docs, tutorials

## Architecture
- Core engine: directed acyclic graph with dynamic rerouting
- Streams: async iterators / queues
- Nodes: Input, Output, Modifier, Subgraph
- UI: React/Flask/FastAPI web editor
- Persistence: YAML workflows
- CLI: Typer commands

## Phases

### Phase 1: Foundation
- Project scaffolding, packaging, config
- Core data models: Node, Edge, Graph, Stream
- Stream primitives: Pipe, Queue, Fork, Merge
- YAML load/save

### Phase 2: Node System
- Base Node class with lifecycle
- Input nodes: File, Pipe, Process, Socket, Web, PythonScript, Shell, Generator, Log
- Output nodes: File, Pipe, Process, Socket, Web, Log
- Modifier nodes: Grep, Merge, Fork, Template, Trigger, BoolOps, NumberOps, Tokenizer, JSON extract/export, Script, Process

### Phase 3: Wiring & Execution
- Graph validation and topological sort
- Dynamic rerouting engine
- Async execution runtime
- Subgraph composition

### Phase 4: Interface
- Web editor UI with node wiring
- REST/WebSocket API
- CLI commands: run, validate, daemon, convert
- YAML workflow import/export

### Phase 5: Packaging & Ops
- Docker image
- PyPI package
- Documentation and tutorials
- Tests and CI

## Deliverables v1
- Functional graph editor locally
- 10+ input/output/modifier nodes
- YAML persistence
- CLI run/daemon
- Basic web UI
