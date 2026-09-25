"""
Coverage for pystreamflow/nodes/input_script.py (ScriptInputNode) -
previously 18% covered. Runs a real script via a real subprocess (using
sys.executable so it works regardless of what's on PATH), rather than
mocking subprocess execution, since asyncio.create_subprocess_exec is
cheap enough here to exercise for real.
"""
import asyncio
import sys

import pytest

from pystreamflow.nodes.input_script import ScriptInputNode


async def _run_once_and_get(node, count=1, timeout=5.0):
    await node.start()
    try:
        items = []
        for _ in range(count):
            items.append(await asyncio.wait_for(node.out_pipe.get(), timeout=timeout))
        return items
    finally:
        await node.stop()


async def test_script_input_requires_script_path():
    n = ScriptInputNode('n', {})
    with pytest.raises(ValueError, match='script_path is required'):
        await n.init()


async def test_script_input_lines_mode(tmp_path):
    script = tmp_path / 'print_lines.py'
    script.write_text("print('line one')\nprint('line two')\n")
    n = ScriptInputNode('n', {
        'script_path': str(script), 'interpreter': sys.executable,
        'interval': 0, 'emit_mode': 'lines',
    })
    items = await _run_once_and_get(n, count=2)
    assert [i['line'] for i in items] == ['line one', 'line two']
    assert all(i['returncode'] == 0 for i in items)


async def test_script_input_full_mode(tmp_path):
    script = tmp_path / 'print_once.py'
    script.write_text("print('hello')\n")
    n = ScriptInputNode('n', {
        'script_path': str(script), 'interpreter': sys.executable,
        'interval': 0, 'emit_mode': 'full',
    })
    items = await _run_once_and_get(n, count=1)
    assert items[0]['stdout'] == 'hello\n'
    assert items[0]['returncode'] == 0

async def test_script_input_json_mode_valid_json(tmp_path):
    script = tmp_path / 'print_json.py'
    script.write_text('print(\'{"a": 1}\')\n')
    n = ScriptInputNode('n', {
        'script_path': str(script), 'interpreter': sys.executable,
        'interval': 0, 'emit_mode': 'json',
    })
    items = await _run_once_and_get(n, count=1)
    assert items[0]['payload'] == {'a': 1}

async def test_script_input_json_mode_invalid_json_falls_back_to_full(tmp_path):
    script = tmp_path / 'print_text.py'
    script.write_text("print('not json')\n")
    n = ScriptInputNode('n', {
        'script_path': str(script), 'interpreter': sys.executable,
        'interval': 0, 'emit_mode': 'json',
    })
    items = await _run_once_and_get(n, count=1)
    assert items[0]['stdout'] == 'not json\n'
    assert 'payload' not in items[0]

async def test_script_input_unknown_emit_mode_falls_back_to_full_shape(tmp_path):
    script = tmp_path / 'print_x.py'
    script.write_text("print('x')\n")
    n = ScriptInputNode('n', {
        'script_path': str(script), 'interpreter': sys.executable,
        'interval': 0, 'emit_mode': 'nonsense',
    })
    items = await _run_once_and_get(n, count=1)
    assert items[0]['stdout'] == 'x\n'

async def test_script_input_stderr_lines_emitted_with_stream_tag(tmp_path):
    script = tmp_path / 'print_err.py'
    script.write_text("import sys\nprint('oops', file=sys.stderr)\n")
    n = ScriptInputNode('n', {
        'script_path': str(script), 'interpreter': sys.executable,
        'interval': 0, 'emit_mode': 'lines',
    })
    items = await _run_once_and_get(n, count=1)
    assert items[0]['line'] == 'oops'
    assert items[0]['stream'] == 'stderr'

async def test_script_input_missing_script_emits_error_item(tmp_path):
    missing = tmp_path / 'does_not_exist.py'
    n = ScriptInputNode('n', {
        'script_path': str(missing), 'interval': 0,
    })
    items = await _run_once_and_get(n, count=1)
    assert 'error' in items[0]
    assert 'Script not found' in items[0]['error']

async def test_script_input_timeout_emits_error_item(tmp_path):
    script = tmp_path / 'sleep_forever.py'
    script.write_text("import time\ntime.sleep(5)\n")
    n = ScriptInputNode('n', {
        'script_path': str(script), 'interpreter': sys.executable,
        'interval': 0, 'timeout': 0.2,
    })
    items = await _run_once_and_get(n, count=1, timeout=5.0)
    assert 'error' in items[0]
    assert 'timed out' in items[0]['error']

async def test_script_input_repeats_on_interval(tmp_path):
    script = tmp_path / 'print_tick.py'
    script.write_text("print('tick')\n")
    n = ScriptInputNode('n', {
        'script_path': str(script), 'interpreter': sys.executable,
        'interval': 0.05, 'emit_mode': 'lines',
    })
    items = await _run_once_and_get(n, count=2, timeout=5.0)
    assert [i['line'] for i in items] == ['tick', 'tick']

async def test_script_input_args_as_space_separated_string(tmp_path):
    script = tmp_path / 'echo_args.py'
    script.write_text("import sys\nprint(' '.join(sys.argv[1:]))\n")
    n = ScriptInputNode('n', {
        'script_path': str(script), 'interpreter': sys.executable,
        'interval': 0, 'emit_mode': 'lines', 'args': 'foo bar',
    })
    items = await _run_once_and_get(n, count=1)
    assert items[0]['line'] == 'foo bar'
