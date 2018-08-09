# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import inspect
from collections import namedtuple

def init_tuple(tuple, **kwargs):
    if all([field in kwargs for field in tuple._fields]):
        return tuple(**{field:kwargs.get(field) for field in tuple._fields})


def create_tuple(clazz):
    return namedtuple(clazz.__name__, properties(clazz))

def properties(clazz):
    return [
        attr[0] for attr in inspect.getmembers(clazz, lambda a: not(inspect.isroutine(a)))
             if not attr[0].startswith('_') and not attr[0].endswith('_')
    ]