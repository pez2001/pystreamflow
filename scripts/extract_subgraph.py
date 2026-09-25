#!/usr/bin/env python3
"""
Extract a subgraph from a workflow YAML.
Usage:
  python scripts/extract_subgraph.py workflow.yaml --nodes id1 id2 --out subgraph.yaml
"""
import argparse, yaml, sys
from pathlib import Path

def extract(workflow_path, node_ids, out_path):
    data = yaml.safe_load(Path(workflow_path).read_text())
    nodes = data.get('nodes', [])
    edges = data.get('edges', [])
    node_map = {n['id']: n for n in nodes}
    selected = set(node_ids)
    # filter edges where both ends selected
    filtered_edges = [e for e in edges if e['source'] in selected and e['target'] in selected]
    # find boundary nodes
    inputs = set()
    outputs = set()
    for e in filtered_edges:
        inputs.add(e['source'])
        outputs.add(e['target'])
    # also keep nodes with external connections
    # build subgraph
    subgraph_nodes = [n for n in nodes if n['id'] in selected]
    subgraph = {
        'nodes': subgraph_nodes,
        'edges': filtered_edges,
        'inputs': list(inputs - outputs),
        'outputs': list(outputs - inputs)
    }
    Path(out_path).write_text(yaml.safe_dump(subgraph))
    print(f"Subgraph written to {out_path}")

if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('workflow')
    p.add_argument('--nodes', nargs='+', required=True)
    p.add_argument('--out', required=True)
    args = p.parse_args()
    extract(args.workflow, args.nodes, args.out)
