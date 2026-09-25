# PyStreamFlow

Python based modern node graph editor for stream flow routing.

## Features
- Node graph editor with stream routing
- YAML workflows with versioning
- CLI + Web API + MCP server for AI
- Live view of node contents
- Multi WebInput nodes
- LM Studio LLM integration
- Docker production deployment

## Quick Start
```bash
docker compose -f docker-compose.prod.yml up -d
```
API: http://localhost:8000
Web UI: http://localhost:8000/
Web inputs: http://localhost:8080
MCP: http://localhost:8000/mcp/tools
Docs: http://localhost:8000/docs/

This also brings up a `caddy` container that fronts all of the above with
real HTTPS (`https://<hostname>`, edit the one hostname line in the
`Caddyfile` to match your own) - see `docs/user_guide.md`'s "HTTPS via
Caddy" section for the one-time step of trusting its certificate.

## Documentation
Online context help is available in the editor. Hover over palette items for tips linking to [docs/nodes/index.md](docs/nodes/index.md).

## Install
pip install -e .
