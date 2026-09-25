"""
Coverage for pystreamflow/nodes/html_scraper_node.py (HTMLScraperNode) -
previously 62% covered. bs4 (BeautifulSoup) is not an installed/declared
dependency of this project in this environment, so init()'s `self.bs4 =
None` fallback is what real runs exercise here - the pure-regex path is
tested directly against the real node. The bs4-based css_selector branch
(lines that only run when bs4 imports successfully) is exercised by
injecting a minimal fake standing in for BeautifulSoup's small subset of
the interface this file actually calls (`soup.select(css)` returning
elements with `.get_text()`/`.get(attr)`/`str()`), which tests this
module's own dispatch logic around that interface without depending on
the third-party library actually being installed.
"""
import asyncio

from pystreamflow.core.stream import Pipe
from pystreamflow.nodes.html_scraper_node import HTMLScraperNode


async def _feed_and_get(node, item, timeout=2.0):
    inp, out = Pipe(), Pipe()
    node.add_input('in', inp)
    node.add_output('out', out)
    await node.start()
    try:
        await inp.put(item)
        return await asyncio.wait_for(out.get(), timeout=timeout)
    finally:
        await node.stop()


async def _feed_and_get_with_fake_bs4(node, item, elements_by_selector, timeout=2.0):
    # init() genuinely re-detects bs4 (None, since it isn't installed in
    # this environment) every time it runs, and start() calls init() the
    # first time it's invoked - so the fake has to be installed *after*
    # that first init() call, with _initialized manually marked True so
    # start() doesn't clobber it by re-running init() again.
    await node.init()
    node._initialized = True
    node.bs4 = _fake_bs4_factory(elements_by_selector)
    inp, out = Pipe(), Pipe()
    node.add_input('in', inp)
    node.add_output('out', out)
    await node.start()
    try:
        await inp.put(item)
        return await asyncio.wait_for(out.get(), timeout=timeout)
    finally:
        await node.stop()


class _FakeElement:
    def __init__(self, text, attrs=None, raw=None):
        self._text = text
        self._attrs = attrs or {}
        self._raw = raw if raw is not None else f'<span>{text}</span>'

    def get_text(self, strip=True):
        return self._text.strip() if strip else self._text

    def get(self, attr, default=''):
        return self._attrs.get(attr, default)

    def __str__(self):
        return self._raw


class _FakeSoup:
    def __init__(self, elements_by_selector):
        self._elements_by_selector = elements_by_selector

    def select(self, css):
        return self._elements_by_selector.get(css, [])


def _fake_bs4_factory(elements_by_selector):
    def factory(html, parser):
        return _FakeSoup(elements_by_selector)
    return factory


# ---------- No extractors configured (regex-free default path) ----------

async def test_default_output_strips_html_tags():
    n = HTMLScraperNode('n', {})
    result = await _feed_and_get(n, '<div><p>Hello <b>World</b></p></div>')
    assert result == {'scraped': 'Hello World'}

async def test_default_output_key_is_configurable():
    n = HTMLScraperNode('n', {'output_key': 'text'})
    result = await _feed_and_get(n, '<p>hi</p>')
    assert result == {'text': 'hi'}

async def test_idles_with_no_input_pipe():
    n = HTMLScraperNode('n', {})
    await n.start()
    await asyncio.sleep(0.15)
    await n.stop()


# ---------- Pure-regex path (bs4 unavailable - the real behavior here) ----------

async def test_regex_extractor_without_bs4():
    n = HTMLScraperNode('n', {'extractors': [{'name': 'nums', 'pattern': r'\d+'}]})
    result = await _feed_and_get(n, 'a1 b22 c333')
    assert n.bs4 is None  # bs4 isn't installed in this environment
    assert result == {'nums': ['1', '22', '333']}

async def test_regex_extractor_single_match_collapses_to_scalar():
    n = HTMLScraperNode('n', {'extractors': [{'name': 'num', 'pattern': r'\d+'}]})
    result = await _feed_and_get(n, 'only 42 here')
    assert result == {'num': '42'}

async def test_regex_extractor_default_field_name():
    n = HTMLScraperNode('n', {'extractors': [{'pattern': r'\d+'}]})
    result = await _feed_and_get(n, '7')
    assert result == {'field': '7'}

async def test_regex_extractor_without_pattern_is_skipped():
    n = HTMLScraperNode('n', {'extractors': [{'name': 'nothing'}]})
    result = await _feed_and_get(n, 'anything')
    assert result == {}

async def test_regex_extractor_ignore_case():
    n = HTMLScraperNode('n', {'extractors': [
        {'name': 'hits', 'pattern': 'hello', 'ignore_case': True},
    ]})
    result = await _feed_and_get(n, 'HELLO world hello')
    assert result == {'hits': ['HELLO', 'hello']}


# ---------- bs4-based path (faked, since bs4 isn't installed here) ----------

async def test_css_selector_single_match_text_only_collapses_to_scalar():
    n = HTMLScraperNode('n', {'extractors': [{'name': 'title', 'css_selector': 'h1'}]})
    result = await _feed_and_get_with_fake_bs4(
        n, '<h1>My Title</h1>', {'h1': [_FakeElement('  My Title  ')]})
    assert result == {'title': 'My Title'}

async def test_css_selector_multiple_matches_returns_list():
    n = HTMLScraperNode('n', {'extractors': [{'name': 'links', 'css_selector': 'a', 'attr': 'href'}]})
    result = await _feed_and_get_with_fake_bs4(
        n, '<a href="/1">1</a><a href="/2">2</a>',
        {'a': [_FakeElement('link1', {'href': '/1'}), _FakeElement('link2', {'href': '/2'})]})
    assert result == {'links': ['/1', '/2']}

async def test_css_selector_text_only_false_returns_raw_html():
    n = HTMLScraperNode('n', {'extractors': [{'name': 'raw', 'css_selector': 'span', 'text_only': False}]})
    result = await _feed_and_get_with_fake_bs4(
        n, '<span class="x">x</span>',
        {'span': [_FakeElement('x', raw='<span class="x">x</span>')]})
    assert result == {'raw': '<span class="x">x</span>'}

async def test_bs4_available_extractor_without_css_falls_back_to_regex():
    n = HTMLScraperNode('n', {'extractors': [{'name': 'nums', 'pattern': r'\d+'}]})
    # bs4 "available" (faked) but this extractor has no css_selector.
    result = await _feed_and_get_with_fake_bs4(n, 'x1 y2', {})
    assert result == {'nums': ['1', '2']}

async def test_bs4_available_no_matches_for_selector_returns_empty_list():
    n = HTMLScraperNode('n', {'extractors': [{'name': 'missing', 'css_selector': '.nope'}]})
    result = await _feed_and_get_with_fake_bs4(n, '<div></div>', {})
    assert result == {'missing': []}
