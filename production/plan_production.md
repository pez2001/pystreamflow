# PyStreamFlow Production Release Plan

## Vision
Production-ready stream flow node graph with enterprise reliability, observability, and AI integration.

## Release Goals
- Stable API + MCP server for AI agents
- Multi-node web input support with shared server
- Live view of node contents
- Docker + compose production deployment
- CI/CD, tests, docs, tutorials
- Security, auth, rate limiting
- Monitoring and logging

## Scope

### Core Stability
- Hardened stream primitives with backpressure
- Node lifecycle management with health checks
- Persistent workflow versioning
- Error handling and retries

### Features
- Web input nodes, multiple instances
- LLM node with LM Studio connection
- Live view API `/nodes/{id}/last`
- MCP server for AI interaction
- Docker container + compose

### Operations
- Production Dockerfile multi-stage
- Helm chart / docker-compose
- Prometheus metrics
- Structured logging
- Config via env + YAML

### Quality
- Unit + integration tests
- CI pipeline GitHub Actions
- Code coverage >80%
- Documentation site
- Tutorials

## Timeline
- Week 1: Core hardening
- Week 2: Web/MCP features
- Week 3: Docker & observability
- Week 4: QA, docs, release

## Success Criteria
- Deployable via docker-compose
- AI can list/inspect/inject via MCP
- Live view works for 100+ nodes
- Zero critical bugs
