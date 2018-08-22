# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import inspect
from collections import namedtuple
import re
from graphene.types.base import BaseType as GraphqlType

def init_tuple(tuple, **kwargs):
    if all([field in kwargs for field in tuple._fields]):
        return tuple(**{field:kwargs.get(field) for field in tuple._fields})


def create_tuple(clazz):
    return namedtuple(clazz.__name__, properties(clazz))

def properties(clazz):
    return [
        attr[0] for attr in inspect.getmembers(clazz, lambda a: isinstance(a,GraphqlType))
    ]

def is_paging(args):
    return 'first' in args or 'before' in args or 'after' in args

def snake_case(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

def days_between(start_date, end_date):
    return abs((start_date - end_date).days)