# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import inspect
from collections import namedtuple
import re
import graphene
from graphene.types.base import BaseType as GraphqlType
from sqlalchemy import case


class GraphQLImplementationError(Exception):
    pass


def init_tuple(tuple_type, **kwargs):
    return tuple_type(**{field: kwargs.get(field) for field in tuple_type._fields})


def create_tuple(clazz):
    return namedtuple(clazz.__name__, properties(clazz))


def properties(clazz):
    return [
        attr[0] for attr in inspect.getmembers(
            clazz,
            lambda a: isinstance(a, GraphqlType) or isinstance(a, graphene.Field) or isinstance(a, graphene.types.structures.Structure)
        )
    ]


def is_required(field, interface):
    attribute = getattr(interface, field, None)
    if attribute:
        kwargs = getattr(attribute, 'kwargs', None)
        if kwargs:
            return kwargs.get('required')
    else:
        raise GraphQLImplementationError(f"The attribute {field} was not found on interface {interface} ")


def is_paging(args):
    return 'first' in args or 'before' in args or 'after' in args or 'last' in args


def snake_case(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def days_between(start_date, end_date):
    return abs((start_date - end_date).days)


# SqlAlchmy expression utils
def nulls_to_zero(column_expr):
    return case(
        [
            (column_expr == None, 0)
        ],
        else_=column_expr
    )
