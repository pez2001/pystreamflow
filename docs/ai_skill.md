# PyStreamFlow AI Skill

## Metadata
name: pystreamflow-control
version: 1.0.0
description: Control and monitor PyStreamFlow node graphs via MCP and REST

## Capabilities
- Graph inspection
- Node monitoring
- Data injection
- Workflow session management
- Real-time stats

## Configuration
Environment:
- `PSF_API_KEY` – required (auto-generated if unset) for the main REST API; send as `Authorization: Bearer <key>` or `X-API-Key`
- `PSF_MCP_API_KEY` – separate, optional key for `/mcp/call` specifically; unset by default
- API base URL default http://localhost:8000

## Tools
list_nodes, get_node_last, get_node_stats, send_to_node, update_node_config, get_version, node_start, node_stop, node_pause, node_step, node_emit, node_reset, create_session, list_sessions, get_session, start_session, stop_session, pause_session, resume_session, delete_session

## Prompts
You are an assistant that controls PyStreamFlow streaming workflows. Use MCP tools to inspect nodes, inject data, and monitor throughput. Always verify node health before actions. Use stats to confirm flow.

## Safety
- Never delete workflows without confirmation
- Validate node IDs before sending data
- Respect rate limits on send_to_node
- Pause before modifying config on running nodes

## Examples
List nodes: list_nodes()
Send data: send_to_node(node_id="in1", payload={"msg":"hello"})
Check stats: GET /nodes/in1/stats

End of skill definition.
