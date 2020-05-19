from __future__ import annotations
from typing import Generator, Optional, List
from lxml import etree  # type: ignore
import re


class NodeNotFound(Exception):
    pass


class XmlNode:

    def __init__(self, node: etree._Element, namespace: str):
        self.node = node
        self.ns = namespace
        self.nsmap = {'main': self.ns}

    def make(self, node: etree._Element):
        return XmlNode(node, self.ns)

    def path(self, path: str, xpath: bool = False):
        if xpath:
            path = re.sub('(^|/):', r'\1main:', path)
        else:
            path = re.sub('(^|/):', r'\1{%s}' % self.ns, path)
        return path

    def all(self, path: str, xpath: bool = False) -> List[XmlNode]:
        if xpath:
            return [
                self.make(node)
                for node in self.node.xpath(self.path(path, xpath=True), namespaces=self.nsmap)
            ]
        else:
            return [
                self.make(node)
                for node in self.node.findall(self.path(path))
            ]

    def first(self, path: str, xpath: bool = False) -> Optional[XmlNode]:
        try:
            return self.all(path, xpath)[0]
        except IndexError:
            return None

    def text(self, path: str = None, xpath: bool = False, default: str = None) -> str:
        if path is None:
            return self.node.text
        node = self.first(path, xpath)
        if node is None:
            if default is not None:
                return default
            raise NodeNotFound()
        return node.text()

    def all_text(self, path: str, xpath: bool = False) -> List[str]:
        return [node.text() for node in self.all(path, xpath)]

    def serialize(self):
        return etree.tostring(self.node,
                              pretty_print=True,
                              xml_declaration=True,
                              encoding='utf-8').decode('utf-8')

    def __repr__(self):
        return '<XmlNode %s>' % self.node.tag

    def __getattr__(self, name):
        return getattr(self.node, name)
