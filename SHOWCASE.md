# PyStreamFlow Showcase

## Overview
Working demo of all PyStreamFlow functionality.

## Access
- UI Editor: http://localhost:8000/ui
- Showcase: http://localhost:8000/showcase
- API: http://localhost:8000/docs
- MCP: http://localhost:9000/mcp/tools

## Demo Workflow
File: `demo_workflow.yaml`

Pipeline:
Web Input → Trim → Upper → Replace → Split → Join → Log Output
Web Input → LM Studio → Log Output
Web Input → Encoding Convert → Log Output

## Features Demonstrated
- Web Input nodes, multi-instance
- Text manipulation: Upper, Lower, Trim, Replace, Substring, Reverse, Title, Strip, Split, Join
- Numeric manipulation: Add, Sub, Mul, Div, Mod, Pow, Min, Max, Clamp, Round, Abs
- Boolean logic: And, Or, Not, Xor, Nand, Nor, Xnor
- Encoding conversion
- LLM LM Studio connection
- Live view of node contents
- MCP protocol for AI interaction
- Visual connection wiring with drag
- Canvas pan/zoom/fit
- YAML import/export
- CLI pipe/tail

## Quick Test
```bash
# Start stack
docker compose up -d

# Send data to demo web input
curl -X POST http://localhost:8080/demo/in -H "Content-Type: application/json" -d '{"msg":"hello world"}'

# Tail output
curl http://localhost:8000/nodes/log_out/last?n=10
```

## Production Ready
Docker multi-stage build, FastAPI API, vanilla JS editor, plugin system, CLI tools.
