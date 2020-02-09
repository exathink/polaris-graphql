# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar


import abc
from abc import abstractmethod, abstractstaticmethod


class NamedNodeResolver(abc.ABC):

    @staticmethod
    @abstractmethod
    def named_node_selector(**kwargs):
        pass


class ConnectionResolver(abc.ABC):
    @staticmethod
    @abstractmethod
    def connection_nodes_selector(**kwargs):
        pass


class InterfaceResolver(abc.ABC):

    @staticmethod
    @abstractmethod
    def interface_selector(named_node_cte, **kwargs):
        pass
