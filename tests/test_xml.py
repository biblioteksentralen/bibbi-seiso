from lxml import etree
import pytest
from soji.common.xml import XmlNode, NodeNotFound

test_data = """<?xml version='1.0' encoding='utf-8'?>
<ns2:VIAFCluster xmlns:foaf="http://xmlns.com/foaf/0.1/" xmlns:ns2="http://example.com/">
    <ns2:id>69021674</ns2:id>
    <ns2:doc about="http://viaf.org/viaf/69021674/">
        <ns2:example resource="http://viaf.org/viaf/data">Person</ns2:example>
        <ns2:topic>http://viaf.org/viaf/69021674</ns2:topic>
    </ns2:doc>
</ns2:VIAFCluster>
""".encode('utf-8')


@pytest.fixture
def test_node() -> XmlNode:
    return XmlNode(etree.fromstring(test_data), 'http://example.com/')


def test_get_attribute(test_node: XmlNode):
    assert test_node.first(':doc/:example').get('resource') == 'http://viaf.org/viaf/data'
    assert test_node.first(':doc/:example').attrib['resource'] == 'http://viaf.org/viaf/data'


def test_xpath(test_node: XmlNode):
    assert test_node.text('//:example[@resource="http://viaf.org/viaf/data"]', True) == 'Person'


def test_get_text(test_node: XmlNode):
    assert test_node.text(':id') == '69021674'
    assert test_node.text(':doc/:topic') == 'http://viaf.org/viaf/69021674'


def test_first_returns_none_if_not_found(test_node: XmlNode):
    assert test_node.first(':id') is not None
    assert test_node.first(':id/:example') is None


def test_raise_on_text_node_not_found(test_node: XmlNode):
    with pytest.raises(NodeNotFound):
        test_node.text(':id/:example')
    assert 'def' == test_node.text(':id/:example', default='def')


def test_serialize(test_node):
    assert test_node.serialize() == test_data.decode('utf-8')
