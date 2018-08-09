# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar

from .join_utils import resolve_instance
from .connection_utils import NodeResolverQuery, QueryConnectionField, CountableConnection

import graphene
from graphene.types.objecttype import ObjectType, ObjectTypeOptions


class SelectableObjectOptions(ObjectTypeOptions):
    named_node_resolver = None
    interface_resolvers = None
    interface_enum = None
    connection_class = None

class Selectable(ObjectType):

    class Meta:
        abstract = True

    @classmethod
    def __init_subclass_with_meta__(cls,
                                    interfaces = None,
                                    named_node_resolver=None,
                                    interface_resolvers=None,
                                    connection_class = None,
                                    interface_enum=None,
                                    **options):

        _meta = SelectableObjectOptions(cls)

        assert named_node_resolver, "Property named_node_resolver for class Meta is required"
        _meta.named_node_resolver = named_node_resolver

        assert interface_resolvers, "Property interface_resolvers for class Meta is required"
        _meta.interface_resolvers = interface_resolvers

        if interface_enum is None:
            interface_enum = graphene.Enum(
            f'{cls.__name__}Interfaces', [
                    (interface.__name__, interface.__name__)
                    for interface in interfaces
            ]
        )
        _meta.interface_enum = interface_enum


        if connection_class:
            _meta.connection_class = connection_class

        super().__init_subclass_with_meta__(_meta=_meta, interfaces=interfaces, **options)


    @classmethod
    def Field(cls, **kwargs):
        return graphene.Field(
            cls,
            key=graphene.Argument(type=graphene.String, required=True),
            interfaces=graphene.Argument(
                graphene.List(cls._meta.interface_enum),
                required=False,
            ),
            **kwargs
        )

    @classmethod
    def ConnectionField(cls, **kwargs):
        assert cls._meta.connection_class, f"Class {cls.__name__} must specify Meta attribute connection_class" \
                                           f"in order to use default ConnectionField method from Selectable"
        return QueryConnectionField(
            cls._meta.connection_class(),
            interfaces=graphene.Argument(
                graphene.List(cls._meta.interface_enum),
                required=True,
            ),
            **kwargs
        )

    @classmethod
    def get_node(cls, info, id):
        return cls.resolve_instance(id)




    @classmethod
    def resolve_instance(cls, key, **kwargs):
        return resolve_instance(
            cls._meta.named_node_resolver,
            cls._meta.interface_resolvers,
            resolver_context=cls.__name__,
            params=dict(key=key),
            output_type=cls,
            **kwargs
        )

    @classmethod
    def resolve_interface(cls, interface, params, **kwargs):
        return resolve_instance(
            cls._meta.named_node_resolver,
            cls._meta.interface_resolvers,
            resolver_context=cls.__name__,
            interface=interface,
            params=params,
            **kwargs
        )

    @classmethod
    def resolve_connection(cls, parent_relationship, named_node_resolver, params, **kwargs):
        return NodeResolverQuery(
            named_node_resolver=named_node_resolver,
            interface_resolvers=cls._meta.interface_resolvers,
            resolver_context=parent_relationship,
            params=params,
            output_type=cls,
            **kwargs
        )


    @classmethod
    def interface_resolvers(cls):
        return cls._meta.interface_resolvers

    @classmethod
    def named_node_resolver(cls):
        return cls._meta.named_node_resolver
