"""
Phase 5: real, behavior-checking tests (not just "does it start without
crashing") for node types that had little or no coverage before this
pass. Each test drives the node through a real Pipe end to end and checks
the actual emitted value, the same discipline test_transform_nodes.py and
test_logic_gates.py already established for the numeric/text/boolean
families.
"""
import asyncio
import json
import logging
import socket
import sys
import time

import pytest

from pystreamflow.core.stream import Pipe
from pystreamflow.nodes.base64_decode_node import Base64DecodeNode
from pystreamflow.nodes.base64_encode_node import Base64EncodeNode
from pystreamflow.nodes.clock_node import ClockNode
from pystreamflow.nodes.display import DisplayNode
from pystreamflow.nodes.encoding_convert import EncodingConvertNode
from pystreamflow.nodes.html_scraper_node import HTMLScraperNode
from pystreamflow.nodes.input_directory import DirectoryInputNode
from pystreamflow.nodes.input_generator import GeneratorInputNode
from pystreamflow.nodes.input_log import LogInputNode
from pystreamflow.nodes.input_process import ProcessInputNode
from pystreamflow.nodes.input_python import PythonScriptInputNode
from pystreamflow.nodes.input_shell import ShellInputNode
from pystreamflow.nodes.input_socket import SocketInputNode
from pystreamflow.nodes.json_input import JSONInputNode
from pystreamflow.nodes.json_output import JSONOutputNode
from pystreamflow.nodes.line_buffer import LineBufferNode
from pystreamflow.nodes.line_splitter import LineSplitterNode
from pystreamflow.nodes.logic_and import AndNode
from pystreamflow.nodes.logic_compare import CompareNode
from pystreamflow.nodes.logic_math import MathNode
from pystreamflow.nodes.logic_not import NotNode
from pystreamflow.nodes.modifier_fork import ForkNode
from pystreamflow.nodes.modifier_json import JSONExtractNode
from pystreamflow.nodes.modifier_script import ScriptNode
from pystreamflow.nodes.modifier_template import TemplateNode
from pystreamflow.nodes.output_file import FileOutputNode
from pystreamflow.nodes.output_log import LogOutputNode
from pystreamflow.nodes.output_process import ProcessOutputNode
from pystreamflow.nodes.output_socket import SocketOutputNode
from pystreamflow.nodes.queue_fifo_node import FIFOQueueNode
from pystreamflow.nodes.queue_lifo_node import LIFOQueueNode
from pystreamflow.nodes.rolling_window_buffer_node import RollingWindowBufferNode
from pystreamflow.nodes.stack_node import StackNode
from pystreamflow.nodes.table import TableNode
from pystreamflow.nodes.tokenizer import TokenizerNode
from pystreamflow.nodes.trim_string import TrimStringNode
from pystreamflow.nodes.url_input import UrlInputNode
from pystreamflow.nodes.user_prompt_node import UserPromptNode
from pystreamflow.nodes.value_constant import ConstantValueNode


async def _feed_and_get(node, input_item, timeout=2.0):
    inp, out = Pipe(), Pipe()
    node.add_input('in', inp)
    node.add_output('out', out)
    await node.start()
    try:
        await inp.put(input_item)
        return await asyncio.wait_for(out.get(), timeout=timeout)
    finally:
        await node.stop()


def _append_text(path, text):
    # Plain sync helper (not `async def`) so tests that append to a file
    # from inside an async test function don't call a blocking `open()`
    # directly inside a coroutine (ruff ASYNC230).
    with open(path, 'a') as f:
        f.write(text)


# ---------- Simple single-in/single-out transforms ----------

async def test_encoding_convert_decode_to_text():
    n = EncodingConvertNode('n', {'decode_to_text': True})
    result = await _feed_and_get(n, b'hello')
    assert result == 'hello'

async def test_encoding_convert_to_bytes():
    n = EncodingConvertNode('n', {'decode_to_text': False})
    result = await _feed_and_get(n, 'hello')
    assert result == b'hello'

async def test_json_output_pretty():
    n = JSONOutputNode('n', {'pretty': True})
    result = await _feed_and_get(n, {'a': 1})
    assert result == '{\n  "a": 1\n}'

async def test_json_output_compact():
    n = JSONOutputNode('n', {'pretty': False})
    result = await _feed_and_get(n, {'a': 1})
    assert result == '{"a": 1}'

async def test_line_splitter_splits_on_newline():
    n = LineSplitterNode('n', {})
    inp, out = Pipe(), Pipe()
    n.add_input('in', inp)
    n.add_output('out', out)
    await n.start()
    try:
        await inp.put('line1\nline2\n')
        first = await asyncio.wait_for(out.get(), timeout=2.0)
        second = await asyncio.wait_for(out.get(), timeout=2.0)
        assert first == 'line1'
        assert second == 'line2'
    finally:
        await n.stop()

async def test_line_splitter_keepends():
    n = LineSplitterNode('n', {'keepends': True})
    inp, out = Pipe(), Pipe()
    n.add_input('in', inp)
    n.add_output('out', out)
    await n.start()
    try:
        await inp.put('a\n')
        first = await asyncio.wait_for(out.get(), timeout=2.0)
        assert first == 'a\n'
    finally:
        await n.stop()

async def test_line_buffer_multi_mode_batches_by_count():
    n = LineBufferNode('n', {'mode': 'multi', 'lines': 2, 'timeout': 5.0})
    inp, out = Pipe(), Pipe()
    n.add_input('in', inp)
    n.add_output('out', out)
    await n.start()
    try:
        await inp.put('a')
        await inp.put('b')
        result = await asyncio.wait_for(out.get(), timeout=2.0)
        assert result == ['a', 'b']
    finally:
        await n.stop()

async def test_line_buffer_single_mode_emits_immediately():
    n = LineBufferNode('n', {'mode': 'single'})
    result = await _feed_and_get(n, 'x')
    assert result == 'x'

async def test_line_buffer_flushes_on_timeout():
    n = LineBufferNode('n', {'mode': 'multi', 'lines': 5, 'timeout': 0.1})
    inp, out = Pipe(), Pipe()
    n.add_input('in', inp)
    n.add_output('out', out)
    await n.start()
    try:
        await inp.put('only-one')
        # Fewer than `lines` items - must flush on timeout instead of
        # waiting forever for more.
        result = await asyncio.wait_for(out.get(), timeout=2.0)
        assert result == ['only-one']
    finally:
        await n.stop()

async def test_tokenizer_default_pattern():
    n = TokenizerNode('n', {})
    inp, out = Pipe(), Pipe()
    n.add_input('in', inp)
    n.add_output('out', out)
    await n.start()
    try:
        await inp.put('hello world')
        first = await asyncio.wait_for(out.get(), timeout=2.0)
        second = await asyncio.wait_for(out.get(), timeout=2.0)
        assert (first, second) == ('hello', 'world')
    finally:
        await n.stop()

async def test_tokenizer_custom_pattern():
    n = TokenizerNode('n', {'pattern': r'\d+'})
    inp, out = Pipe(), Pipe()
    n.add_input('in', inp)
    n.add_output('out', out)
    await n.start()
    try:
        await inp.put('a1 b22 c333')
        got = [await asyncio.wait_for(out.get(), timeout=2.0) for _ in range(3)]
        assert got == ['1', '22', '333']
    finally:
        await n.stop()

async def test_trim_string_default():
    n = TrimStringNode('n', {})
    assert await _feed_and_get(n, '  hi  ') == 'hi'

async def test_trim_string_custom_chars():
    n = TrimStringNode('n', {'chars': 'x'})
    assert await _feed_and_get(n, 'xxhixx') == 'hi'

async def test_compare_node_eq():
    n = CompareNode('n', {'operator': 'eq', 'compare_value': 5})
    assert await _feed_and_get(n, 5) is True
    n2 = CompareNode('n2', {'operator': 'eq', 'compare_value': 5})
    assert await _feed_and_get(n2, 6) is False

@pytest.mark.parametrize('operator,compare_value,item,expected', [
    ('ne', 5, 6, True),
    ('gt', 5, 6, True),
    ('lt', 5, 3, True),
    ('gte', 5, 5, True),
    ('lte', 5, 5, True),
    ('contains', 'ell', 'hello', True),
    ('unknown_op', 5, 5, False),
])
async def test_compare_node_operators(operator, compare_value, item, expected):
    n = CompareNode('n', {'operator': operator, 'compare_value': compare_value})
    assert await _feed_and_get(n, item) == expected

async def test_compare_node_raises_internally_returns_false():
    # Comparing incompatible types (e.g. gt between str and int) raises
    # inside _compare()'s try/except, which must swallow it and return
    # False rather than crashing the node.
    n = CompareNode('n', {'operator': 'gt', 'compare_value': 5})
    assert await _feed_and_get(n, 'not-a-number') is False

async def test_math_node_add():
    n = MathNode('n', {'op': 'add', 'value': 3})
    assert await _feed_and_get(n, 4) == 7.0

async def test_math_node_int_stays_int():
    n = MathNode('n', {'op': 'add', 'value': 3})
    result = await _feed_and_get(n, 4)
    assert isinstance(result, int)
    assert result == 7

async def test_math_node_unknown_op_falls_back_to_add():
    n = MathNode('n', {'op': 'nonexistent', 'value': 1})
    assert await _feed_and_get(n, 2) == 3.0

async def test_math_node_error_passthrough():
    n = MathNode('n', {'op': 'add', 'value': 1})
    assert await _feed_and_get(n, 'not-a-number') == 'not-a-number'

async def test_json_extract_node_no_path_returns_whole_object():
    n = JSONExtractNode('n', {})
    result = await _feed_and_get(n, '{"a": {"b": 1}}')
    assert result == {'a': {'b': 1}}

async def test_json_extract_node_with_path():
    n = JSONExtractNode('n', {'path': 'a.b'})
    result = await _feed_and_get(n, '{"a": {"b": 42}}')
    assert result == 42

async def test_json_extract_node_invalid_json_passthrough():
    n = JSONExtractNode('n', {})
    result = await _feed_and_get(n, 'not json at all')
    assert result == 'not json at all'

async def test_json_extract_node_extracts_from_a_real_dict_item():
    """Regression test for a real bug found live, from a direct report:
    "connected api input out to json extract in, set path to 'msg',
    connected json extract out to display - the message went through
    unchanged". ApiInputNode (and most upstream nodes) emit an already-
    parsed Python dict, not a JSON string - json.loads(str(item)) on a
    dict's repr (single-quoted keys) always raised, silently falling back
    to passthrough every time. Every prior test here only ever fed this
    node a literal JSON *string*, which is exactly why this never
    surfaced before.
    """
    n = JSONExtractNode('n', {'path': 'msg'})
    result = await _feed_and_get(n, {'msg': 'hello world'})
    assert result == 'hello world'

async def test_json_extract_node_no_path_returns_dict_item_unchanged():
    n = JSONExtractNode('n', {})
    result = await _feed_and_get(n, {'msg': 'hello world'})
    assert result == {'msg': 'hello world'}

async def test_json_extract_node_nested_path_on_a_real_dict_item():
    n = JSONExtractNode('n', {'path': 'a.b'})
    result = await _feed_and_get(n, {'a': {'b': 42}})
    assert result == 42

async def test_json_extract_node_walks_into_a_list_by_index():
    n = JSONExtractNode('n', {'path': 'items.1'})
    result = await _feed_and_get(n, {'items': ['x', 'y', 'z']})
    assert result == 'y'

async def test_json_extract_node_unresolvable_path_yields_empty_dict_not_a_crash():
    # Previously: walking a path past a scalar (e.g. 'a.b.c' where 'a.b'
    # is already a leaf value) raised AttributeError internally, silently
    # swallowed by the outer except and passed through as the *original*
    # item - now each unresolvable step yields {} instead of crashing the
    # whole extraction.
    n = JSONExtractNode('n', {'path': 'a.b.c'})
    result = await _feed_and_get(n, {'a': {'b': 'leaf-value'}})
    assert result == {}

async def test_script_node_runs_real_script_result_style():
    # ScriptNode now actually exec()s the configured script against the
    # incoming item (reusing ScriptedOutputNode's dual-exec strategy) -
    # this used to always return a fixed f'scripted:{item}' string no
    # matter what the script said.
    n = ScriptNode('n', {'script': 'result = data.upper()'})
    result = await _feed_and_get(n, 'hi')
    assert result == {'script': 'result = data.upper()', 'input': 'hi', 'output': 'HI'}

async def test_script_node_runs_real_script_return_style():
    n = ScriptNode('n', {'script': 'return data * 2'})
    result = await _feed_and_get(n, 21)
    assert result == {'script': 'return data * 2', 'input': 21, 'output': 42}

async def test_script_node_with_no_result_emits_none_output():
    n = ScriptNode('n', {'script': 'x = data'})
    result = await _feed_and_get(n, 'hi')
    assert result == {'script': 'x = data', 'input': 'hi', 'output': None}

async def test_script_node_execution_can_be_disabled(monkeypatch):
    monkeypatch.setenv('PSF_ALLOW_SCRIPT_NODES', '0')
    n = ScriptNode('n', {'script': 'result = data.upper()'})
    result = await _feed_and_get(n, 'hi')
    assert result['error'] == 'script execution disabled (PSF_ALLOW_SCRIPT_NODES=0)'
    assert result['input'] == 'hi'

async def test_template_node_substitutes():
    n = TemplateNode('n', {'template': 'value is {data}'})
    result = await _feed_and_get(n, 42)
    assert result == 'value is 42'

async def test_file_output_node_records_written_items():
    n = FileOutputNode('n', {'path': '/tmp/does-not-matter.txt'})
    inp = Pipe()
    n.add_input('in', inp)
    await n.start()
    try:
        await inp.put('hello')
        await asyncio.sleep(0.1)
        last = n.get_last(1)
        assert last and last[0]['written'] == 'hello'
        assert last[0]['path'] == '/tmp/does-not-matter.txt'
        assert 'bytes_written' in last[0]
    finally:
        await n.stop()

async def test_file_output_node_actually_writes_real_bytes_to_disk(tmp_path):
    # The actual bug: this node's process() loop used to be a bare
    # "# Simulate write" comment - despite the class name, the editor's
    # "Append items to file" description, and a `path` config field that
    # looks exactly like every other real-I/O node's, nothing was ever
    # written to the filesystem at all. Confirm the file genuinely exists
    # on disk with the expected content now.
    target = tmp_path / 'out.txt'
    n = FileOutputNode('n', {'path': str(target)})
    inp = Pipe()
    n.add_input('in', inp)
    await n.start()
    try:
        await inp.put('first line')
        await inp.put('second line')
        for _ in range(50):
            if len(n.get_last(100)) >= 2:
                break
            await asyncio.sleep(0.05)
    finally:
        await n.stop()
    assert target.read_text() == 'first line\nsecond line\n'

async def _feed_file_output_and_get_last(n, item):
    # FileOutputNode is a pure sink (record_output(), no real 'out'
    # pipe) - unlike _feed_and_get(), which waits on an emitted 'out'
    # item, this polls the live-view buffer the same way the other
    # FileOutputNode tests above already do.
    inp = Pipe()
    n.add_input('in', inp)
    await n.start()
    try:
        await inp.put(item)
        for _ in range(50):
            last = n.get_last(1)
            if last:
                return last[0]
            await asyncio.sleep(0.05)
        raise AssertionError('FileOutputNode never recorded the item')
    finally:
        await n.stop()

async def test_file_output_node_creates_missing_parent_directories(tmp_path):
    # A freshly-mounted, still-empty Docker volume (or a freshly checked
    # out project) won't have the target directory yet - the first write
    # shouldn't fail with FileNotFoundError just because of that.
    target = tmp_path / 'nested' / 'dir' / 'out.txt'
    n = FileOutputNode('n', {'path': str(target)})
    result = await _feed_file_output_and_get_last(n, 'hello')
    assert 'error' not in result
    assert target.read_text() == 'hello\n'

async def test_file_output_node_defaults_into_files_dir_env_var(tmp_path, monkeypatch):
    monkeypatch.setenv('PSF_FILES_DIR', str(tmp_path))
    n = FileOutputNode('n', {})
    result = await _feed_file_output_and_get_last(n, 'hello')
    assert result['path'] == str(tmp_path / 'output.txt')
    assert (tmp_path / 'output.txt').read_text() == 'hello\n'

async def test_file_output_node_last_items_buffer_is_capped():
    # Regression test: FileOutputNode used to append straight to
    # self._last_items, bypassing both emit() and the new
    # BaseNode.record_output() helper it now uses - which meant the
    # _max_last cap those apply never got enforced here, so a long-running
    # FileOutputNode's live-view buffer grew without bound instead of
    # staying capped like every other node type's does.
    n = FileOutputNode('n', {'path': '/tmp/does-not-matter.txt', 'max_last': 3})
    inp = Pipe()
    n.add_input('in', inp)
    await n.start()
    try:
        for i in range(6):
            await inp.put(f'item-{i}')
        for _ in range(30):
            if len(n.get_last(100)) >= 3:
                break
            await asyncio.sleep(0.05)
        last = n.get_last(100)
        assert len(last) == 3
        assert last[-1]['written'] == 'item-5'
    finally:
        await n.stop()

async def test_process_output_node():
    # ProcessOutputNode now actually spawns `cat` and pipes the item to
    # its stdin, emitting the real stdout - this used to just echo
    # {'cmd', 'input'} back out without ever running anything.
    n = ProcessOutputNode('n', {'cmd': 'cat'})
    result = await _feed_and_get(n, 'data-in')
    assert result['cmd'] == 'cat'
    assert result['input'] == 'data-in'
    assert result['output'] == 'data-in'
    assert result['returncode'] == 0

async def test_process_output_node_execution_can_be_disabled(monkeypatch):
    monkeypatch.setenv('PSF_ALLOW_SHELL_NODES', '0')
    n = ProcessOutputNode('n', {'cmd': 'cat'})
    result = await _feed_and_get(n, 'data-in')
    assert result['error'] == 'command execution disabled (PSF_ALLOW_SHELL_NODES=0)'

async def test_process_output_node_defaults_cwd_into_data_dir(tmp_path, monkeypatch):
    # ProcessOutputNode previously had no `cwd` support at all (unlike
    # ProcessInputNode/ShellInputNode) - it never passed one to the
    # subprocess, so a relative path in `cmd` landed wherever this
    # process's own cwd happened to be. `pwd` run with no explicit `cwd`
    # config should now default into PSF_DATA_DIR.
    monkeypatch.setenv('PSF_DATA_DIR', str(tmp_path))
    n = ProcessOutputNode('n', {'cmd': 'pwd'})
    result = await _feed_and_get(n, 'unused')
    assert result['output'].strip() == str(tmp_path)

async def test_process_output_node_cwd_can_be_overridden(tmp_path):
    n = ProcessOutputNode('n', {'cmd': 'pwd', 'cwd': str(tmp_path)})
    result = await _feed_and_get(n, 'unused')
    assert result['output'].strip() == str(tmp_path)

async def test_socket_output_node():
    # SocketOutputNode now actually opens a real TCP connection and sends
    # the item - this used to just echo {'sent_to', 'data'} back out
    # without ever touching the network. Spin up a real local TCP server
    # to receive it.
    received = []
    server_ready = asyncio.Event()

    async def handle(reader, writer):
        data = await reader.read(1024)
        received.append(data)
        writer.close()

    server = await asyncio.start_server(handle, '127.0.0.1', 0)
    port = server.sockets[0].getsockname()[1]
    async with server:
        server_ready.set()
        n = SocketOutputNode('n', {'host': '127.0.0.1', 'port': port})
        result = await _feed_and_get(n, 'payload')
        await asyncio.sleep(0.2)  # let the server task finish reading
        assert result['sent_to'] == f'127.0.0.1:{port}'
        assert result['data'] == 'payload'
        assert result['bytes_sent'] == len(b'payload\n')
        assert received == [b'payload\n']

async def test_socket_output_node_connection_refused_reports_error():
    # Nothing listening on this port - a real connection attempt should
    # fail and be reported as an error item, not silently "succeed".
    server = await asyncio.start_server(lambda r, w: None, '127.0.0.1', 0)
    port = server.sockets[0].getsockname()[1]
    server.close()
    await server.wait_closed()
    n = SocketOutputNode('n', {'host': '127.0.0.1', 'port': port, 'timeout': 2.0})
    result = await _feed_and_get(n, 'payload')
    assert 'error' in result

async def test_socket_output_node_can_be_disabled(monkeypatch):
    monkeypatch.setenv('PSF_ALLOW_SOCKET_NODES', '0')
    n = SocketOutputNode('n', {'host': '127.0.0.1', 'port': 9002})
    result = await _feed_and_get(n, 'payload')
    assert result['error'] == 'outbound socket connections disabled (PSF_ALLOW_SOCKET_NODES=0)'


def _reserve_free_port() -> int:
    # SocketOutputNode's server mode does its own bind() inside process(),
    # so a test needs to know which port it will bind *before* starting
    # it. Reserve one by binding-then-releasing rather than hard-coding a
    # port number that might collide with something else in CI.
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('127.0.0.1', 0))
    port = s.getsockname()[1]
    s.close()
    return port


async def test_socket_output_node_server_mode_listens_and_broadcasts_to_clients():
    # A real user reported "socket output node is not opening a port" -
    # the pre-existing (and still-default) 'client' mode genuinely never
    # listens on anything, by design (it dials *out* per item). This is
    # the new 'server' mode: the node itself listens, symmetric with
    # SocketInputNode, and broadcasts each item to whoever is connected.
    port = _reserve_free_port()
    n = SocketOutputNode('n', {'mode': 'server', 'host': '127.0.0.1', 'port': port})
    inp, out = Pipe(), Pipe()
    n.add_input('in', inp)
    n.add_output('out', out)
    await n.start()
    try:
        reader = writer = None
        for _ in range(50):
            try:
                reader, writer = await asyncio.open_connection('127.0.0.1', port)
                break
            except OSError:
                await asyncio.sleep(0.05)
        assert writer is not None, 'SocketOutputNode server mode never opened its listening port'

        await asyncio.sleep(0.1)  # let _accept_client register the new writer
        await inp.put('broadcast me')
        result = await asyncio.wait_for(out.get(), timeout=2.0)
        assert result['data'] == 'broadcast me'
        assert result['clients'] == 1

        received = await asyncio.wait_for(reader.readuntil(b'\n'), timeout=2.0)
        assert received == b'broadcast me\n'

        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
    finally:
        await n.stop()


async def test_socket_output_node_server_mode_opens_its_port_even_with_nothing_wired_to_in():
    # The actual root cause of the bug report: opening the listening
    # socket used to be gated behind the same "wait for 'in' to be
    # wired" loop client mode correctly uses for its outbound connect -
    # which meant a freshly-created, not-yet-wired server-mode node
    # never opened its port at all. Confirm it does now, with zero
    # wiring in place (no add_input call whatsoever).
    port = _reserve_free_port()
    n = SocketOutputNode('n', {'mode': 'server', 'host': '127.0.0.1', 'port': port})
    n.add_output('out', Pipe())
    await n.start()
    try:
        connected = False
        for _ in range(50):
            try:
                _reader, writer = await asyncio.open_connection('127.0.0.1', port)
                connected = True
                writer.close()
                try:
                    await writer.wait_closed()
                except Exception:
                    pass
                break
            except OSError:
                await asyncio.sleep(0.05)
        assert connected, 'server mode never opened its port with nothing wired to in'
    finally:
        await n.stop()


async def test_socket_output_node_server_mode_emits_even_with_no_clients_connected():
    # A broadcast with nobody listening is still a legitimate send, not
    # an error - matches how the real 'clients' count is reported.
    port = _reserve_free_port()
    n = SocketOutputNode('n', {'mode': 'server', 'host': '127.0.0.1', 'port': port})
    result = await _feed_and_get(n, 'lonely broadcast')
    assert result['data'] == 'lonely broadcast'
    assert result['clients'] == 0


async def test_socket_output_node_server_mode_can_be_disabled(monkeypatch):
    monkeypatch.setenv('PSF_ALLOW_SOCKET_NODES', '0')
    port = _reserve_free_port()
    n = SocketOutputNode('n', {'mode': 'server', 'host': '127.0.0.1', 'port': port})
    result = await _feed_and_get(n, 'payload')
    assert result['error'] == 'outbound socket connections disabled (PSF_ALLOW_SOCKET_NODES=0)'


# ---------- Base64 ----------

async def test_base64_encode_default():
    n = Base64EncodeNode('n', {})
    inp, out = Pipe(), Pipe()
    n.add_input('in', inp)
    n.add_output('out', out)
    await n.start()
    try:
        await inp.put('hello')
        result = await asyncio.wait_for(out.get(), timeout=2.0)
        assert result == 'aGVsbG8='
    finally:
        await n.stop()

async def test_base64_decode_default():
    n = Base64DecodeNode('n', {})
    inp, out = Pipe(), Pipe()
    n.add_input('in', inp)
    n.add_output('out', out)
    await n.start()
    try:
        await inp.put('aGVsbG8=')
        result = await asyncio.wait_for(out.get(), timeout=2.0)
        assert result == 'hello'
    finally:
        await n.stop()

async def test_base64_decode_adds_missing_padding():
    n = Base64DecodeNode('n', {'validate_padding': True})
    inp, out = Pipe(), Pipe()
    n.add_input('in', inp)
    n.add_output('out', out)
    await n.start()
    try:
        await inp.put('aGVsbG8')  # missing '=' padding
        result = await asyncio.wait_for(out.get(), timeout=2.0)
        assert result == 'hello'
    finally:
        await n.stop()

async def test_base64_encode_urlsafe():
    n = Base64EncodeNode('n', {'urlsafe': True})
    inp, out = Pipe(), Pipe()
    n.add_input('in', inp)
    n.add_output('out', out)
    await n.start()
    try:
        await inp.put(b'\xfb\xff')
        result = await asyncio.wait_for(out.get(), timeout=2.0)
        assert '+' not in result and '/' not in result
    finally:
        await n.stop()


# ---------- Sources ----------

async def test_generator_input_emits_count_then_stops():
    n = GeneratorInputNode('n', {'count': 3})
    out = Pipe()
    n.add_output('out', out)
    await n.start()
    try:
        got = [await asyncio.wait_for(out.get(), timeout=2.0) for _ in range(3)]
        assert got == [{'value': 0}, {'value': 1}, {'value': 2}]
    finally:
        await n.stop()

async def test_generator_input_honors_configured_value():
    # Regression test: `value` used to be silently ignored - every item
    # was an auto-incrementing counter no matter what was configured.
    n = GeneratorInputNode('n', {'count': 2, 'value': 'hello', 'interval': 0.05})
    out = Pipe()
    n.add_output('out', out)
    await n.start()
    try:
        got = [await asyncio.wait_for(out.get(), timeout=2.0) for _ in range(2)]
        assert got == [{'value': 'hello'}, {'value': 'hello'}]
    finally:
        await n.stop()

async def test_generator_input_honors_configured_interval():
    # Regression test: `interval` used to be silently ignored - this node
    # always slept a hardcoded 0.5s between emissions no matter what.
    n = GeneratorInputNode('n', {'count': 3, 'interval': 0.05})
    out = Pipe()
    n.add_output('out', out)
    start = time.monotonic()
    await n.start()
    try:
        for _ in range(3):
            await asyncio.wait_for(out.get(), timeout=2.0)
        elapsed = time.monotonic() - start
        # 3 emissions at a real ~0.05s interval should finish well under
        # 1s; the old hardcoded 0.5s-per-item behavior would take ~1.5s.
        assert elapsed < 1.0
    finally:
        await n.stop()

async def test_generator_input_can_be_stopped_and_restarted_after_exhausting_count():
    # Bug report: "the generator node couldn't be stopped and started
    # (node top bar doesn't turn green), a click on run is needed to
    # actually turn it on" - this node's process() naturally returns once
    # self.i reaches self.count, and self.i was never reset by anything
    # short of a fresh instance's own init() - so restarting the *same*
    # instance via stop()+start() used to re-enter process()'s
    # `while ... self.i < self.count` loop with self.i already at count,
    # exit it instantly, and never emit again (matching "top bar doesn't
    # turn green" - _health never gets a chance to stay 'healthy' - and a
    # separate report that nothing new ever reached whatever was wired
    # downstream). Only stop() clearing _initialized (core/node.py) fixes
    # this generically, without GeneratorInputNode needing its own
    # override - confirm it actually re-runs init() (which zeroes self.i)
    # and genuinely emits a fresh batch, not just that health flips green.
    n = GeneratorInputNode('n', {'count': 3, 'interval': 0.01})
    out = Pipe()
    n.add_output('out', out)
    await n.start()
    try:
        first_batch = [await asyncio.wait_for(out.get(), timeout=2.0) for _ in range(3)]
        assert first_batch == [{'value': 0}, {'value': 1}, {'value': 2}]
        # process() has now returned on its own (count exhausted) - the
        # node is still "running" (nothing called stop() yet) but idle.
        await asyncio.sleep(0.05)
        assert n.i == 3

        await n.stop()
        assert n._initialized is False

        await n.start()
        assert n.i == 0  # init() actually re-ran
        second_batch = [await asyncio.wait_for(out.get(), timeout=2.0) for _ in range(3)]
        assert second_batch == [{'value': 0}, {'value': 1}, {'value': 2}]
    finally:
        await n.stop()

async def test_json_input_node_tails_a_real_file_and_emits_parsed_values(tmp_path):
    # The actual bug: despite reading config['source'] into self.source,
    # the old process() never referenced it again anywhere - it
    # unconditionally emitted one fixed synthetic sample forever,
    # regardless of source or any real external input. Confirm it now
    # really tails a real file and emits the actual parsed JSON values.
    target = tmp_path / 'stream.ndjson'
    target.write_text('')
    n = JSONInputNode('n', {'source': str(target)})
    out = Pipe()
    n.add_output('out', out)
    await n.start()
    try:
        _append_text(target, '{"hello": "world"}\n')
        item = await asyncio.wait_for(out.get(), timeout=5.0)
        assert item == {'hello': 'world'}

        _append_text(target, '{"second": 2}\n')
        item2 = await asyncio.wait_for(out.get(), timeout=5.0)
        assert item2 == {'second': 2}
    finally:
        await n.stop()

async def test_json_input_node_reports_invalid_json_as_an_error_item(tmp_path):
    target = tmp_path / 'stream.ndjson'
    target.write_text('')
    n = JSONInputNode('n', {'source': str(target)})
    out = Pipe()
    n.add_output('out', out)
    await n.start()
    try:
        _append_text(target, 'not valid json\n')
        item = await asyncio.wait_for(out.get(), timeout=5.0)
        assert item['raw'] == 'not valid json'
        assert 'error' in item
    finally:
        await n.stop()

async def test_json_input_node_skips_blank_lines(tmp_path):
    target = tmp_path / 'stream.ndjson'
    target.write_text('')
    n = JSONInputNode('n', {'source': str(target)})
    out = Pipe()
    n.add_output('out', out)
    await n.start()
    try:
        _append_text(target, '\n\n{"only": "this"}\n\n')
        item = await asyncio.wait_for(out.get(), timeout=5.0)
        assert item == {'only': 'this'}
        # No second item should follow from the blank lines.
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(out.get(), timeout=0.5)
    finally:
        await n.stop()

async def test_json_input_node_reads_real_json_lines_from_stdin():
    # Verifies the (harder-to-unit-test) default 'stdin' mode end to end
    # by actually spawning a real child process, writing real JSON lines
    # to its real stdin, and reading back what the node really emitted -
    # not by mocking sys.stdin, since connect_read_pipe needs a genuine
    # OS-level pipe to attach to.
    script = (
        "import asyncio, json\n"
        "from pystreamflow.nodes.json_input import JSONInputNode\n"
        "from pystreamflow.core.stream import Pipe\n"
        "async def main():\n"
        "    n = JSONInputNode('n', {'source': 'stdin'})\n"
        "    out = Pipe()\n"
        "    n.add_output('out', out)\n"
        "    await n.start()\n"
        "    item = await asyncio.wait_for(out.get(), timeout=10.0)\n"
        "    print(json.dumps(item))\n"
        "    await n.stop()\n"
        "asyncio.run(main())\n"
    )
    proc = await asyncio.create_subprocess_exec(
        sys.executable, '-c', script,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        proc.stdin.write(b'{"from": "real stdin"}\n')
        await proc.stdin.drain()
        proc.stdin.close()
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=15.0)
    finally:
        if proc.returncode is None:
            proc.kill()
            await proc.wait()
    assert proc.returncode == 0, stderr.decode()
    lines = [l for l in stdout.decode().splitlines() if l.strip()]
    assert json.loads(lines[-1]) == {'from': 'real stdin'}

@pytest.mark.parametrize('cls,config,expected_key', [
    # These four used to always emit one fixed, canned sample no matter
    # what the config said ('log sample' / 'process output' / 'python
    # sample' / 'shell output') - now each really runs the configured
    # command/script and reports genuine output, so `expected_key in
    # item` alone is no longer a meaningful check; every one of these
    # configs is chosen so the emitted value can be asserted exactly.
    (ProcessInputNode, {'cmd': 'echo hello-from-process', 'interval': 0}, 'output'),
    (PythonScriptInputNode, {'script': 'print("hello-from-python")', 'interval': 0}, 'output'),
    (ShellInputNode, {'command': 'echo hello-from-shell', 'interval': 0}, 'output'),
], ids=lambda x: getattr(x, '__name__', x) if isinstance(x, type) else x)
async def test_input_nodes_run_real_commands(cls, config, expected_key):
    n = cls('n', config)
    out = Pipe()
    n.add_output('out', out)
    await n.start()
    try:
        item = await asyncio.wait_for(out.get(), timeout=5.0)
        assert expected_key in item
        assert 'hello-from' in item[expected_key]
    finally:
        await n.stop()

@pytest.mark.parametrize('cls,config,env_var', [
    (ProcessInputNode, {'cmd': 'echo hi', 'interval': 0}, 'PSF_ALLOW_SHELL_NODES'),
    (ShellInputNode, {'command': 'echo hi', 'interval': 0}, 'PSF_ALLOW_SHELL_NODES'),
    (PythonScriptInputNode, {'script': 'print("hi")', 'interval': 0}, 'PSF_ALLOW_SCRIPT_NODES'),
], ids=lambda x: getattr(x, '__name__', x) if isinstance(x, type) else x)
async def test_input_nodes_execution_can_be_disabled(cls, config, env_var, monkeypatch):
    monkeypatch.setenv(env_var, '0')
    n = cls('n', config)
    out = Pipe()
    n.add_output('out', out)
    await n.start()
    try:
        item = await asyncio.wait_for(out.get(), timeout=5.0)
        assert 'error' in item
        assert 'disabled' in item['error']
    finally:
        await n.stop()

@pytest.mark.parametrize('cls,config_key', [
    (ProcessInputNode, 'cmd'),
    (ShellInputNode, 'command'),
], ids=lambda x: getattr(x, '__name__', x) if isinstance(x, type) else x)
async def test_input_nodes_default_cwd_into_data_dir(cls, config_key, tmp_path, monkeypatch):
    # Previously an unset `cwd` meant "this process's own cwd" (`/app`
    # under the packaged Dockerfile - not any of the project's mounted
    # volumes). Now it defaults into PSF_DATA_DIR so a relative-path
    # command reads/writes somewhere the operator can actually see.
    monkeypatch.setenv('PSF_DATA_DIR', str(tmp_path))
    n = cls('n', {config_key: 'pwd', 'interval': 0})
    out = Pipe()
    n.add_output('out', out)
    await n.start()
    try:
        item = await asyncio.wait_for(out.get(), timeout=5.0)
        assert item['output'].strip() == str(tmp_path)
    finally:
        await n.stop()

@pytest.mark.parametrize('cls,config_key', [
    (ProcessInputNode, 'cmd'),
    (ShellInputNode, 'command'),
], ids=lambda x: getattr(x, '__name__', x) if isinstance(x, type) else x)
async def test_input_nodes_cwd_can_be_overridden(cls, config_key, tmp_path):
    n = cls('n', {config_key: 'pwd', 'interval': 0, 'cwd': str(tmp_path)})
    out = Pipe()
    n.add_output('out', out)
    await n.start()
    try:
        item = await asyncio.wait_for(out.get(), timeout=5.0)
        assert item['output'].strip() == str(tmp_path)
    finally:
        await n.stop()

async def test_log_input_node_captures_a_real_log_record():
    # LogInputNode now attaches a real logging.Handler and emits actual
    # records logged through Python's logging module - previously it
    # ignored `level`/any real log source entirely and just emitted a
    # fixed {'level': ..., 'msg': 'log sample'} once a second forever.
    logger_name = 'pystreamflow.tests.log_input_node_capture'
    n = LogInputNode('n', {'logger_name': logger_name, 'level': 'INFO'})
    out = Pipe()
    n.add_output('out', out)
    await n.start()
    try:
        # Give the node a moment to attach its handler before logging.
        await asyncio.sleep(0.1)
        logging.getLogger(logger_name).info('hello from the real logger')
        item = await asyncio.wait_for(out.get(), timeout=2.0)
        assert item['msg'] == 'hello from the real logger'
        assert item['level'] == 'INFO'
        assert item['logger'] == logger_name
    finally:
        await n.stop()

async def test_log_input_node_filters_below_configured_level():
    logger_name = 'pystreamflow.tests.log_input_node_level_filter'
    n = LogInputNode('n', {'logger_name': logger_name, 'level': 'WARNING'})
    out = Pipe()
    n.add_output('out', out)
    await n.start()
    try:
        await asyncio.sleep(0.1)
        lg = logging.getLogger(logger_name)
        lg.info('should be filtered out')
        lg.warning('should come through')
        item = await asyncio.wait_for(out.get(), timeout=2.0)
        assert item['msg'] == 'should come through'
        assert item['level'] == 'WARNING'
    finally:
        await n.stop()

async def test_clock_node_ticks_up():
    n = ClockNode('n', {'interval': 0.05, 'start_value': 10})
    out = Pipe()
    n.add_output('out', out)
    await n.start()
    try:
        first = await asyncio.wait_for(out.get(), timeout=2.0)
        second = await asyncio.wait_for(out.get(), timeout=2.0)
        assert first['tick'] == 10
        assert second['tick'] == 11
    finally:
        await n.stop()

async def test_clock_node_counts_down():
    n = ClockNode('n', {'interval': 0.05, 'start_value': 5, 'count_up': False})
    out = Pipe()
    n.add_output('out', out)
    await n.start()
    try:
        first = await asyncio.wait_for(out.get(), timeout=2.0)
        second = await asyncio.wait_for(out.get(), timeout=2.0)
        assert (first['tick'], second['tick']) == (5, 4)
    finally:
        await n.stop()

async def test_clock_node_resets_on_input():
    n = ClockNode('n', {'interval': 0.05, 'start_value': 0, 'reset_on_input': True})
    inp, out = Pipe(), Pipe()
    n.add_input('reset', inp)
    n.add_output('out', out)
    await n.start()
    try:
        await asyncio.wait_for(out.get(), timeout=2.0)  # tick 0
        await asyncio.wait_for(out.get(), timeout=2.0)  # tick 1
        await inp.put('reset-now')
        # After a reset signal, the next tick should be back at start_value.
        seen_reset = False
        for _ in range(5):
            item = await asyncio.wait_for(out.get(), timeout=2.0)
            if item['tick'] == 0:
                seen_reset = True
                break
        assert seen_reset
    finally:
        await n.stop()

async def test_display_node_emits_formatted_message():
    n = DisplayNode('n', {'prefix': '> ', 'suffix': ' <'})
    result = await _feed_and_get(n, 'hi')
    assert result == {'display': '> hi <', 'item': 'hi'}

async def test_display_node_empty_prefix_suffix_adds_no_brackets():
    # Regression test: an empty prefix/suffix (the default) used to leave
    # a stray "[]" in the printed message when 'title' was blank
    # (f"[{title}] ..." with title==''); prefix/suffix are glued on
    # literally now, so nothing extra should appear.
    n = DisplayNode('n', {})
    result = await _feed_and_get(n, 'hi')
    assert result == {'display': 'hi', 'item': 'hi'}


# ---------- Stack / queues / rolling window ----------

async def test_stack_node_push_pop_mode():
    n = StackNode('n', {'mode': 'push_pop'})
    inp, out = Pipe(), Pipe()
    n.add_input('in', inp)
    n.add_output('out', out)
    await n.start()
    try:
        await inp.put('a')
        r1 = await asyncio.wait_for(out.get(), timeout=2.0)
        await inp.put('b')
        r2 = await asyncio.wait_for(out.get(), timeout=2.0)
        assert r1 == {'stack': ['a'], 'top': 'a'}
        assert r2 == {'stack': ['a', 'b'], 'top': 'b'}
    finally:
        await n.stop()

async def test_stack_node_clear_mode():
    n = StackNode('n', {'mode': 'clear'})
    result = await _feed_and_get(n, 'anything')
    assert result == {'stack': []}

async def test_stack_node_reads_from_every_wired_input_not_just_the_first():
    # port_schema.py declares this node's inputs as DYNAMIC and
    # documents it as a "true multi-input" node type alongside AndNode/
    # MergeNode - both of which really do poll every wired pipe. This
    # used to only ever read from whichever single pipe happened to be
    # wired first (`next(iter(self.inputs.values()), None)`), silently
    # ignoring anything wired under a second port name.
    n = StackNode('n', {'mode': 'push_pop'})
    first, second, out = Pipe(), Pipe(), Pipe()
    n.add_input('a', first)
    n.add_input('b', second)
    n.add_output('out', out)
    await n.start()
    try:
        await second.put('from-second-port')
        result = await asyncio.wait_for(out.get(), timeout=2.0)
        assert result['top'] == 'from-second-port'
    finally:
        await n.stop()

async def test_fifo_queue_node_appends_in_order():
    n = FIFOQueueNode('n', {})
    inp, out = Pipe(), Pipe()
    n.add_input('in', inp)
    n.add_output('out', out)
    await n.start()
    try:
        await inp.put('a')
        r1 = await asyncio.wait_for(out.get(), timeout=2.0)
        await inp.put('b')
        r2 = await asyncio.wait_for(out.get(), timeout=2.0)
        assert r1 == {'queue': ['a'], 'size': 1}
        assert r2 == {'queue': ['a', 'b'], 'size': 2}
    finally:
        await n.stop()

async def test_fifo_queue_node_reads_from_every_wired_input_not_just_the_first():
    n = FIFOQueueNode('n', {})
    first, second, out = Pipe(), Pipe(), Pipe()
    n.add_input('a', first)
    n.add_input('b', second)
    n.add_output('out', out)
    await n.start()
    try:
        await second.put('from-second-port')
        result = await asyncio.wait_for(out.get(), timeout=2.0)
        assert 'from-second-port' in result['queue']
    finally:
        await n.stop()

async def test_lifo_queue_node_top_is_most_recent():
    n = LIFOQueueNode('n', {})
    inp, out = Pipe(), Pipe()
    n.add_input('in', inp)
    n.add_output('out', out)
    await n.start()
    try:
        await inp.put('a')
        await asyncio.wait_for(out.get(), timeout=2.0)
        await inp.put('b')
        r2 = await asyncio.wait_for(out.get(), timeout=2.0)
        assert r2 == {'queue': ['a', 'b'], 'top': 'b'}
    finally:
        await n.stop()

async def test_lifo_queue_node_reads_from_every_wired_input_not_just_the_first():
    n = LIFOQueueNode('n', {})
    first, second, out = Pipe(), Pipe(), Pipe()
    n.add_input('a', first)
    n.add_input('b', second)
    n.add_output('out', out)
    await n.start()
    try:
        await second.put('from-second-port')
        result = await asyncio.wait_for(out.get(), timeout=2.0)
        assert result['top'] == 'from-second-port'
    finally:
        await n.stop()

async def test_rolling_window_buffer_flushes_on_trigger():
    n = RollingWindowBufferNode('n', {})
    data_pipe, trigger_pipe, hist_pipe = Pipe(), Pipe(), Pipe()
    n.add_input('in', data_pipe)
    n.add_input('trigger', trigger_pipe)
    n.add_output('history', hist_pipe)
    await n.start()
    try:
        await data_pipe.put('a')
        await data_pipe.put('b')
        await asyncio.sleep(0.1)
        await trigger_pipe.put('flush')
        result = await asyncio.wait_for(hist_pipe.get(), timeout=2.0)
        assert result == {'buffer': ['a', 'b'], 'size': 2}
    finally:
        await n.stop()

async def test_rolling_window_buffer_clears_when_not_retained():
    n = RollingWindowBufferNode('n', {'retain_after_flush': False})
    data_pipe, trigger_pipe, hist_pipe = Pipe(), Pipe(), Pipe()
    n.add_input('in', data_pipe)
    n.add_input('trigger', trigger_pipe)
    n.add_output('history', hist_pipe)
    await n.start()
    try:
        await data_pipe.put('a')
        await asyncio.sleep(0.1)
        await trigger_pipe.put('flush')
        await asyncio.wait_for(hist_pipe.get(), timeout=2.0)
        assert len(n.buffer) == 0
    finally:
        await n.stop()


# ---------- User prompt ----------

async def test_user_prompt_node_emits_default_on_timeout_when_unwired():
    n = UserPromptNode('n', {'prompt': 'Q?', 'timeout': 0.05, 'default': 'D'})
    prompt_pipe, out = Pipe(), Pipe()
    n.add_output('prompt', prompt_pipe)
    n.add_output('out', out)
    await n.start()
    try:
        prompt_item = await asyncio.wait_for(prompt_pipe.get(), timeout=2.0)
        assert prompt_item == {'prompt': 'Q?', 'node_id': 'n'}
        out_item = await asyncio.wait_for(out.get(), timeout=2.0)
        assert out_item == 'D'
    finally:
        await n.stop()

async def test_user_prompt_node_emits_user_supplied_value():
    n = UserPromptNode('n', {'prompt': 'Q?', 'timeout': 5.0, 'default': 'D'})
    user_pipe, out = Pipe(), Pipe()
    n.add_input('user', user_pipe)
    n.add_output('out', out)
    await n.start()
    try:
        await user_pipe.put('user-answer')
        out_item = await asyncio.wait_for(out.get(), timeout=2.0)
        assert out_item == 'user-answer'
    finally:
        await n.stop()


# ---------- Table ----------

async def test_table_node_emits_snapshot_after_interval():
    n = TableNode('n', {'columns': ['x'], 'emit_interval': 0.05})
    inp, out = Pipe(), Pipe()
    n.add_input('in', inp)
    n.add_output('out', out)
    await n.start()
    try:
        await inp.put({'x': 1})
        snapshot = await asyncio.wait_for(out.get(), timeout=2.0)
        assert snapshot['columns'] == ['x']
        assert {'x': 1} in snapshot['rows']
    finally:
        await n.stop()

async def test_table_node_wraps_non_dict_rows():
    n = TableNode('n', {'emit_interval': 0.05})
    inp, out = Pipe(), Pipe()
    n.add_input('in', inp)
    n.add_output('out', out)
    await n.start()
    try:
        await inp.put('scalar-row')
        snapshot = await asyncio.wait_for(out.get(), timeout=2.0)
        assert {'value': 'scalar-row'} in snapshot['rows']
    finally:
        await n.stop()

async def test_table_node_reads_from_every_wired_input_not_just_the_first():
    n = TableNode('n', {'emit_interval': 0.05})
    first, second, out = Pipe(), Pipe(), Pipe()
    n.add_input('a', first)
    n.add_input('b', second)
    n.add_output('out', out)
    await n.start()
    try:
        await second.put({'from': 'second-port'})
        snapshot = await asyncio.wait_for(out.get(), timeout=2.0)
        assert {'from': 'second-port'} in snapshot['rows']
    finally:
        await n.stop()


# ---------- HTML scraper (regex-only path; bs4 may or may not be installed) ----------

async def test_html_scraper_default_strips_tags():
    n = HTMLScraperNode('n', {})
    result = await _feed_and_get(n, '<p>Hello <b>World</b></p>')
    assert result['scraped'] == 'Hello World'

async def test_html_scraper_regex_extractor():
    n = HTMLScraperNode('n', {'extractors': [{'name': 'nums', 'pattern': r'\d+'}]})
    # Force the pure-regex fallback path regardless of whether bs4 is
    # installed, since that's the behavior guaranteed not to depend on an
    # optional dependency.
    n.bs4 = None
    result = await _feed_and_get(n, 'a1 b22')
    assert result == {'nums': ['1', '22']}


# ---------- ConstantValueNode repeat mode ----------

async def test_constant_value_node_repeat_mode_emits_more_than_once():
    n = ConstantValueNode('n', {'value': 'x', 'repeat': True, 'interval': 0.02})
    out = Pipe()
    n.add_output('out', out)
    await n.start()
    try:
        first = await asyncio.wait_for(out.get(), timeout=2.0)
        second = await asyncio.wait_for(out.get(), timeout=2.0)
        assert first == second == 'x'
    finally:
        await n.stop()


# ---------- URL input (no network required) ----------

async def test_url_input_node_requires_url():
    n = UrlInputNode('n', {})
    with pytest.raises(ValueError):
        await n.init()

async def test_url_input_node_get_via_mock_transport():
    import httpx

    def handler(request):
        return httpx.Response(200, json={'ok': True})

    n = UrlInputNode('n', {'url': 'http://example.test/data', 'poll_interval': 10.0})
    await n.init()
    out = Pipe()
    n.add_output('out', out)

    import pystreamflow.nodes.url_input as url_input_module
    original_client = url_input_module.httpx.AsyncClient

    def patched_client(*args, **kwargs):
        kwargs['transport'] = httpx.MockTransport(handler)
        return original_client(*args, **kwargs)

    url_input_module.httpx.AsyncClient = patched_client
    try:
        await n.start()
        item = await asyncio.wait_for(out.get(), timeout=2.0)
        assert item == {'ok': True}
    finally:
        await n.stop()
        url_input_module.httpx.AsyncClient = original_client


# ---------- Socket input (real localhost TCP, no external network) ----------

async def test_socket_input_node_receives_real_tcp_data():
    # Unlike most node types, SocketInputNode's own init() creates and
    # wires its 'out' pipe (self.out_pipe) itself - wiring a fresh Pipe
    # before start() would just get clobbered when init() runs, so read
    # from the pipe init() actually created instead.
    n = SocketInputNode('n', {'host': '127.0.0.1', 'port': 0})
    await n.start()
    out = n.out_pipe
    try:
        # port=0 means the OS picks a free port; wait for the server to
        # actually bind before connecting to it.
        for _ in range(50):
            if n._server is not None and n._server.sockets:
                break
            await asyncio.sleep(0.02)
        assert n._server is not None and n._server.sockets
        real_port = n._server.sockets[0].getsockname()[1]

        _reader, writer = await asyncio.open_connection('127.0.0.1', real_port)
        writer.write(b'hello-socket')
        await writer.drain()
        writer.close()

        item = await asyncio.wait_for(out.get(), timeout=2.0)
        assert item['data'] == 'hello-socket'
    finally:
        await n.stop()


# ---------- Late-wire regression coverage ----------
#
# Systemic bug found while investigating a direct report ("json extract
# path='msg', connected to display - message went through unchanged"):
# JSONExtractNode's process() (see modifier_json.py) captured its input
# pipe exactly once - either right before `while self._running:` even
# started, or via an early `if not pipe: return` that exited process()
# entirely - so a wire added via POST /nodes/connect *after* the node was
# created and auto-started (the normal ad-hoc node-editor sequence: create
# the node, then wire it) was never seen. This exact shape turned out to
# be systemic: a sub-agent audit of every node type in pystreamflow/nodes/
# found the identical bug in 17 files total. All 17 are fixed the same
# way - re-fetch the pipe(s) fresh from self.inputs on every outer loop
# iteration instead of once outside it - and every one of them gets a
# dedicated regression test below, wiring the input only after start(),
# mirroring TestLateConnectRewiring's already-established pattern in
# test_api_nodes_and_real_network.py for the same class of bug in a
# different set of node types (WebOutputNode/WebOutputJSONNode/
# ApiOutputNode/LMStudioNode, fixed in an earlier round).
#
# _feed_and_get() above always wires before start(), so it can never
# exercise this bug - that's exactly why none of the existing tests using
# it caught this in the first place.

async def _late_wire_and_get(node, input_item, out_name='out', in_name='in', timeout=3.0):
    out = Pipe()
    node.add_output(out_name, out)
    await node.start()
    try:
        await asyncio.sleep(0.05)  # let process() enter its loop first
        inp = Pipe()
        node.add_input(in_name, inp)  # simulates POST /nodes/connect after auto-start
        await inp.put(input_item)
        return await asyncio.wait_for(out.get(), timeout=timeout)
    finally:
        await node.stop()

async def test_line_splitter_picks_up_a_wire_added_after_start():
    n = LineSplitterNode('n', {})
    result = await _late_wire_and_get(n, 'late-line\n')
    assert result == 'late-line'

async def test_compare_node_picks_up_a_wire_added_after_start():
    n = CompareNode('n', {'operator': 'eq', 'compare_value': 'late'})
    result = await _late_wire_and_get(n, 'late')
    assert result is True

async def test_math_node_picks_up_a_wire_added_after_start():
    n = MathNode('n', {'op': 'add', 'value': 1})
    result = await _late_wire_and_get(n, 2)
    assert result == 3

async def test_not_node_picks_up_a_wire_added_after_start():
    n = NotNode('n', {})
    result = await _late_wire_and_get(n, False)
    assert result is True

async def test_fork_node_picks_up_a_wire_added_after_start():
    # ForkNode has DYNAMIC outputs (port_schema.py) - forks to whatever is
    # wired, under whatever name, so this also exercises that its
    # `for out_name in self.outputs:` emit loop still works correctly once
    # the input side is fixed to be re-fetched live.
    n = ForkNode('n', {})
    result = await _late_wire_and_get(n, 'late-fork-item', out_name='out0')
    assert result == 'late-fork-item'

async def test_template_node_picks_up_a_wire_added_after_start():
    n = TemplateNode('n', {'template': 'got: {data}'})
    result = await _late_wire_and_get(n, 'late')
    assert result == 'got: late'

async def test_file_output_node_picks_up_a_wire_added_after_start():
    n = FileOutputNode('n', {'path': '/tmp/does-not-matter.txt'})
    await n.start()
    try:
        await asyncio.sleep(0.05)
        inp = Pipe()
        n.add_input('in', inp)
        await inp.put('late-file-item')
        for _ in range(50):
            last = n.get_last(1)
            if last and last[0].get('written') == 'late-file-item':
                break
            await asyncio.sleep(0.05)
        else:
            raise AssertionError('FileOutputNode never recorded the late-wired item')
    finally:
        await n.stop()

async def test_log_output_node_picks_up_a_wire_added_after_start():
    n = LogOutputNode('n', {'level': 'INFO'})
    result = await _late_wire_and_get(n, 'late-log-item')
    assert result == {'level': 'INFO', 'log': 'late-log-item'}

async def test_log_output_node_writes_a_real_log_file(tmp_path):
    # The actual bug: this node logged through Python's `logging` module
    # (whatever handler the process happened to have - uvicorn's console
    # handler under the daemon, nothing at all under a bare script), but
    # never produced an actual log *file* on disk, despite the node's own
    # name and the project's `logs` Docker volume mount sitting unused.
    logfile = tmp_path / 'node.log'
    n = LogOutputNode('n', {'level': 'WARNING', 'file': str(logfile)})
    result = await _feed_and_get(n, 'something happened')
    assert result == {'level': 'WARNING', 'log': 'something happened'}
    content = logfile.read_text()
    assert 'something happened' in content
    assert 'WARNING' in content

async def test_log_output_node_defaults_into_logs_dir_env_var(tmp_path, monkeypatch):
    monkeypatch.setenv('PSF_LOGS_DIR', str(tmp_path))
    n = LogOutputNode('n', {})
    await _feed_and_get(n, 'default path item')
    default_logfile = tmp_path / 'pystreamflow.log'
    assert default_logfile.exists()
    assert 'default path item' in default_logfile.read_text()

async def test_log_output_node_two_instances_sharing_a_file_do_not_duplicate_lines(tmp_path):
    # Guards the module-level _attached_file_handlers dedup: without it,
    # a second LogOutputNode instance pointed at the same file would
    # stack a second FileHandler onto the shared logger and every future
    # line (from either node) would be written twice.
    logfile = tmp_path / 'shared.log'
    n1 = LogOutputNode('n1', {'file': str(logfile)})
    await _feed_and_get(n1, 'from-n1')
    n2 = LogOutputNode('n2', {'file': str(logfile)})
    await _feed_and_get(n2, 'from-n2')
    lines = [l for l in logfile.read_text().splitlines() if 'from-n1' in l]
    assert len(lines) == 1

async def test_tokenizer_picks_up_a_wire_added_after_start():
    n = TokenizerNode('n', {})
    result = await _late_wire_and_get(n, 'hello world')
    assert result == 'hello'

async def test_trim_string_picks_up_a_wire_added_after_start():
    n = TrimStringNode('n', {})
    result = await _late_wire_and_get(n, '  late  ')
    assert result == 'late'

async def test_and_node_picks_up_a_wire_added_after_start():
    # Representative of the whole boolean-gate family (And/Or/Nand/Nor/
    # Xor/Xnor) and MergeNode, all fixed identically: `pipes =
    # list(self.inputs.values())` moved from before the loop to its first
    # line inside it. AndNode/etc. iterate self.inputs.values() rather
    # than reading a single named port, so the late wire here is added
    # under an arbitrary port name ('in0'), same as MergeNode's own
    # multi-input wiring.
    n = AndNode('n', {'inputs_needed': 1})
    result = await _late_wire_and_get(n, True, in_name='in0')
    assert result is True

async def _collect(pipe, count, timeout=5.0):
    items = []
    for _ in range(count):
        items.append(await asyncio.wait_for(pipe.get(), timeout=timeout))
    return items

async def test_directory_input_node_emits_files_and_dirs_on_separate_ports(tmp_path):
    (tmp_path / 'a.txt').write_text('a')
    (tmp_path / 'b.txt').write_text('b')
    (tmp_path / 'sub').mkdir()
    n = DirectoryInputNode('n', {'path': str(tmp_path), 'poll_interval': 0.05})
    files_out, dirs_out = Pipe(), Pipe()
    n.add_output('files', files_out)
    n.add_output('dirs', dirs_out)
    await n.start()
    try:
        files = await _collect(files_out, 2)
        dirs = await _collect(dirs_out, 1)
        assert {str(tmp_path / 'a.txt'), str(tmp_path / 'b.txt')} == set(files)
        assert dirs == [str(tmp_path / 'sub')]
        # Neither port should have anything mixed in from the other -
        # a file must never show up on `dirs` or vice versa.
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(files_out.get(), timeout=0.3)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(dirs_out.get(), timeout=0.3)
    finally:
        await n.stop()

async def test_directory_input_node_non_recursive_ignores_nested_files(tmp_path):
    (tmp_path / 'top.txt').write_text('x')
    nested_dir = tmp_path / 'nested'
    nested_dir.mkdir()
    (nested_dir / 'inner.txt').write_text('y')
    n = DirectoryInputNode('n', {'path': str(tmp_path), 'recursive': False, 'poll_interval': 0.05})
    files_out, dirs_out = Pipe(), Pipe()
    n.add_output('files', files_out)
    n.add_output('dirs', dirs_out)
    await n.start()
    try:
        files = await _collect(files_out, 1)
        dirs = await _collect(dirs_out, 1)
        assert files == [str(tmp_path / 'top.txt')]
        assert dirs == [str(nested_dir)]
        # inner.txt must not appear - non-recursive stops at the top level.
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(files_out.get(), timeout=0.3)
    finally:
        await n.stop()

async def test_directory_input_node_recursive_walks_nested_directories(tmp_path):
    nested_dir = tmp_path / 'nested'
    nested_dir.mkdir()
    inner_file = nested_dir / 'inner.txt'
    inner_file.write_text('y')
    n = DirectoryInputNode('n', {'path': str(tmp_path), 'recursive': True, 'poll_interval': 0.05})
    files_out, dirs_out = Pipe(), Pipe()
    n.add_output('files', files_out)
    n.add_output('dirs', dirs_out)
    await n.start()
    try:
        files = await _collect(files_out, 1)
        dirs = await _collect(dirs_out, 1)
        assert files == [str(inner_file)]
        assert dirs == [str(nested_dir)]
    finally:
        await n.stop()

async def test_directory_input_node_only_emits_each_path_once(tmp_path):
    (tmp_path / 'a.txt').write_text('a')
    n = DirectoryInputNode('n', {'path': str(tmp_path), 'poll_interval': 0.05})
    files_out = Pipe()
    n.add_output('files', files_out)
    await n.start()
    try:
        first = await asyncio.wait_for(files_out.get(), timeout=2.0)
        assert first == str(tmp_path / 'a.txt')
        # Several more poll cycles pass with nothing new on disk - the
        # already-seen file must not be re-emitted every pass.
        await asyncio.sleep(0.3)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(files_out.get(), timeout=0.2)

        # A genuinely new file, though, should show up on the next pass.
        (tmp_path / 'b.txt').write_text('b')
        second = await asyncio.wait_for(files_out.get(), timeout=2.0)
        assert second == str(tmp_path / 'b.txt')
    finally:
        await n.stop()

async def test_directory_input_node_missing_directory_is_not_an_error(tmp_path):
    missing = tmp_path / 'does-not-exist-yet'
    n = DirectoryInputNode('n', {'path': str(missing), 'poll_interval': 0.05})
    files_out = Pipe()
    n.add_output('files', files_out)
    await n.start()
    try:
        await asyncio.sleep(0.2)
        assert n._last_error is not None
        assert n._running  # a missing directory must not crash/stop the node
        # Once the directory shows up for real, it should start emitting.
        missing.mkdir()
        (missing / 'late.txt').write_text('z')
        item = await asyncio.wait_for(files_out.get(), timeout=2.0)
        assert item == str(missing / 'late.txt')
    finally:
        await n.stop()

async def test_directory_input_node_path_and_recursive_are_live_attribute_wired(tmp_path):
    # set_attribute() is exactly what a real `type: attribute` graph edge
    # drives (see core/engine.py's _pump_attribute) - confirm both `path`
    # and `recursive` actually take effect on the node's next scan pass
    # rather than only updating config with no visible effect, matching
    # the same live-rewiring convention FileInputNode's own tests cover.
    dir_a = tmp_path / 'a'
    dir_a.mkdir()
    (dir_a / 'from_a.txt').write_text('a')
    dir_b = tmp_path / 'b'
    dir_b.mkdir()
    (dir_b / 'sub').mkdir()

    n = DirectoryInputNode('n', {'path': str(dir_a), 'poll_interval': 0.05})
    files_out, dirs_out = Pipe(), Pipe()
    n.add_output('files', files_out)
    n.add_output('dirs', dirs_out)
    await n.start()
    try:
        first = await asyncio.wait_for(files_out.get(), timeout=2.0)
        assert first == str(dir_a / 'from_a.txt')

        n.set_attribute('path', str(dir_b))
        n.set_attribute('recursive', True)
        # Switching path should reset the "already seen" tracking too -
        # dir_b's own subdirectory must be reported even though nothing
        # in dir_a ever touched that path string.
        d = await asyncio.wait_for(dirs_out.get(), timeout=2.0)
        assert d == str(dir_b / 'sub')
    finally:
        await n.stop()
