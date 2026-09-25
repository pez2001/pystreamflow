# PyStreamFlow Implementation

Generated from production plan.

## Architecture
- Core: Node, Stream, Graph, Engine
- Nodes: Input, Output, Modifier, LLM
- Web: Shared FastAPI server for inputs + live view
- MCP: FastAPI JSON-RPC tools for AI
- Deployment: Docker + Compose

## Implemented Components
- Core models, stream primitives, persistence
- BaseNode with last-items buffer
- WebInputNode with shared server, multi-instance support
- LMStudioNode with OpenAI compatible API
- Live view API `/nodes`, `/nodes/{id}/last`
- MCP server with list_nodes, get_node_last, send_to_node
- Dockerfiles, docker-compose.yml, build scripts

## Pending Production Hardening
- Backpressure & flow control
- Auth & rate limiting
- Prometheus metrics
- Health checks
- CI/CD pipeline
- Documentation site

## How to Run
```bash
docker compose up -d
# API http://localhost:8000
# Web inputs http://localhost:8080
# MCP http://localhost:9000/mcp/tools
```

## Next Steps
- Add tests
- Add auth
- Add metrics
- Finalize docs
