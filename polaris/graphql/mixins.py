# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from .join_utils import resolve_instance
from .connection_utils import NodeResolverQuery

class KeyIdResolverMixin:
    def __init__(self, key, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.key = key

    def resolve_id(self, info, **kwargs):
        return self.key



class InterfaceResolverMixin(KeyIdResolverMixin):
    NamedNodeResolver=None
    InterfaceResolvers = None

    @classmethod
    def get_node(cls, info, id):
        return cls.resolve_instance(id)

    @classmethod
    def resolve_instance(cls, key, **kwargs):
        return resolve_instance(
            cls.NamedNodeResolver,
            cls.InterfaceResolvers,
            resolver_context=cls.__name__,
            params=dict(key=key),
            output_type=cls,
            **kwargs
        )

    @classmethod
    def resolve_connection(cls, parent_relationship, named_node_resolver, params, **kwargs):
        return NodeResolverQuery(
            named_node_resolver=named_node_resolver,
            interface_resolvers=cls.InterfaceResolvers,
            resolver_context=parent_relationship,
            params=params,
            output_type=cls
        )

    def get_node_query_params(self, **kwargs):
        return dict(key=self.key)

class NamedNodeResolverMixin(InterfaceResolverMixin):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = kwargs.get('name', None)


    def resolve_name(self, info, **kwargs):
        return self.name