# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
from abc import abstractmethod, ABC
from functools import partial

import graphene
from graphene.relay import Connection, ConnectionField
from graphene.relay.connection import PageInfo
from graphql_relay.connection.arrayconnection import connection_from_list_slice, connection_from_list
from sqlalchemy.sql import select, func, text

from polaris.common import db
from .join_utils import cte_join, collect_join_resolvers
from .utils import is_paging


class ConnectionQuery(ABC):
    def __init__(self, **kwargs):
        self.limit = None
        self.offset = None

    @staticmethod
    def decode_slice(slc):
        """ Copied from sqlalchemy.orm.utils
        """
        ret = []
        for x in slc.start, slc.stop, slc.step:
            if hasattr(x, '__index__'):
                x = x.__index__()
            ret.append(x)
        return tuple(ret)

    def slice(self, start, stop):
        """Copied and adapted from sqlalchemy.orm.Query.
        """
        if start is not None and stop is not None:
            self.offset = (self.offset or 0) + start
            self.limit = stop - start
        elif start is None and stop is not None:
            self.limit = stop
        elif start is not None and stop is None:
            self.offset = (self.offset or 0) + start

        if self.offset == 0:
            self.offset = None

    def __getitem__(self, item):
        """Copied and adapted from sqlalchemy.orm.Query.
                """
        if isinstance(item, slice):
            start, stop, step = self.decode_slice(item)

            if isinstance(stop, int) and \
                    isinstance(start, int) and \
                    stop - start <= 0:
                return []

            # perhaps we should execute a count() here so that we
            # can still use LIMIT/OFFSET ?
            elif (isinstance(start, int) and start < 0) \
                    or (isinstance(stop, int) and stop < 0):
                return list(self)[item]

            self.slice(start, stop)
            return list(self)
        else:
            if item == -1:
                return list(self)[-1]
            else:
                return list(self[item:item + 1])[0]

    def __iter__(self):
        result = self.execute()
        return iter(result)

    @abstractmethod
    def count(self):
        pass

    @abstractmethod
    def execute(self):
        pass


class SQLConnectionQuery(ConnectionQuery):
    def __init__(self, orm_session, sql, **kwargs):
        super().__init__(**kwargs)
        self.session = orm_session
        self.sql = sql
        self.params = kwargs

    def count(self):
        return self.session.connection().execute(self.count_query, self.params).scalar()

    @property
    def count_query(self):
        return select([func.count()]).select_from(
            text(f"({self.sql}) as ____")
        )

    def execute(self):
        base_query = self.sql
        if self.limit:
            base_query = f"{base_query} LIMIT {self.limit}"

        if self.offset:
            base_query = f"{base_query} OFFSET {self.offset}"

        # this class assumes that session will be closed in the calling scope.
        # connection will be soft-closed per semantics of execute method.
        result_proxy = self.session.connection().execute(text(base_query), self.params)
        result = result_proxy.fetchall()
        return result


class QueryConnectionField(ConnectionField):

    def __init__(self, type, *args, **kwargs):
        kwargs.setdefault('countOnly', graphene.Boolean())
        super().__init__(type, *args, **kwargs)

    @classmethod
    def connection_resolver(cls, resolver, connection_type, root, info, **args):
        resolved = resolver(root, info, **args)
        if isinstance(resolved, ConnectionQuery):
            if args.get('countOnly') is not None:
                count = resolved.count()
                iterable = []
                connection = connection_from_list(
                    iterable,
                    args,
                    connection_type=connection_type,
                    pageinfo_type=PageInfo,
                    edge_type=connection_type.Edge,
                )
                connection.iterable = iterable
                connection.count = count
            elif is_paging(args):
                # We should pay the cost of doing the count
                # only when we are actually paging.
                count = resolved.count()
                connection = connection_from_list_slice(
                    resolved,
                    args,
                    slice_start=0,
                    list_length=count,
                    list_slice_length=count,
                    connection_type=connection_type,
                    pageinfo_type=PageInfo,
                    edge_type=connection_type.Edge,
                )
                connection.iterable = resolved
                connection.count = count
            else:
                # if not, just get the whole list of query results
                iterable = list(resolved)
                count = len(iterable)
                connection = connection_from_list(
                    iterable,
                    args,
                    connection_type=connection_type,
                    pageinfo_type=PageInfo,
                    edge_type=connection_type.Edge,
                )
                connection.iterable = iterable
                connection.count = count
        else:
            connection = super().resolve_connection(connection_type, args, resolved)

        return connection

    def get_resolver(self, parent_resolver):
        return partial(self.connection_resolver, parent_resolver, self.type)


class CountableConnection(Connection):
    class Meta:
        abstract = True

    count = graphene.Int()


def count(selectable):
    alias = selectable.alias()
    return select([func.count(alias.c.key)]).select_from(alias)


class NodeResolverQuery(ConnectionQuery):

    def __init__(self, named_node_resolver, interface_resolvers, resolver_context, params, output_type=None, **kwargs):
        super().__init__(**kwargs)
        self.query = cte_join(named_node_resolver, collect_join_resolvers(interface_resolvers, **kwargs),
                              resolver_context, **kwargs)
        self.output_type = output_type
        self.params = params

    def count(self):
        with db.create_session() as session:
            return session.execute(count(self.query), self.params).scalar()

    def execute(self):
        base_query = self.query
        if self.limit:
            base_query = base_query.limit(self.limit)

        if self.offset:
            base_query = base_query.offset(self.offset)

        with db.create_session() as session:
            result = session.execute(base_query, self.params).fetchall()
            return [
                self.output_type(**{key: value for key, value in row.items()})
                for row in result
            ] if self.output_type else result
