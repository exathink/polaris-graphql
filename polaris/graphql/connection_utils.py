# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2011-2018) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
from abc import abstractmethod, ABC
from functools import partial
from contextlib import contextmanager
import graphene
from graphene.relay import Connection, ConnectionField
from graphene.relay.connection import PageInfo
from graphql_relay.connection.arrayconnection import connection_from_list_slice, connection_from_list
from graphene.utils.subclass_with_meta import SubclassWithMeta

from sqlalchemy.sql import select, func, text

from polaris.common import db
from .join_utils import cte_join, collect_join_resolvers
from .utils import is_paging, snake_case

from graphene.types.objecttype import ObjectTypeOptions

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

    DB_SUMMARIZATION_THRESHOLD = 1000

    def __init__(self, type, *args, **kwargs):
        kwargs.setdefault('summariesOnly', graphene.Argument(graphene.Boolean, required=False, default_value=False))
        kwargs.setdefault('summarize_db', graphene.Argument(graphene.Boolean, required=False, default_value=False))
        super().__init__(type, *args, **kwargs)


    @classmethod
    def get_summarizers(cls, summary_interfaces):
        db_summarizers = dict()
        result_set_summarizers = dict()
        for interface_name in summary_interfaces:
            summarizer = ConnectionSummarizer.get_summarizer(interface_name)
            if summarizer:
                if hasattr(summarizer, 'summarize_db'):
                    db_summarizers[interface_name] = summarizer
                if hasattr(summarizer, 'summarize_result_set'):
                    result_set_summarizers[interface_name] = summarizer

        return db_summarizers, result_set_summarizers

    @classmethod
    def compute_db_summaries(cls, target_summaries, db_summarizers, connection_resolver_query, return_result_set):
        summary_results = dict()
        result_set = None
        with db.create_session() as session:
            with connection_resolver_query.create_temp_table(session) as connection_query_temp:
                for summary in target_summaries:
                    if summary in db_summarizers:
                        summarizer = db_summarizers[summary]
                        summary_results[summary] = summarizer.summarize_db(connection_query_temp, session)

                if len(summary_results) < len(target_summaries) or return_result_set:
                    result_set = connection_resolver_query.execute(join_session=session, to_object=False)

        return summary_results, result_set


    @classmethod
    def compute_result_set_summaries(cls, target_summaries, result_set_summarizers, result_set, summary_result=None):
        if summary_result is None:
            summary_result = dict()

        for summary in target_summaries:
            if summary not in summary_result and summary in result_set_summarizers:
                summarizer = result_set_summarizers[summary]
                summary_result[summary] = summarizer.summarize_result_set(result_set)

        return summary_result


    @classmethod
    def resolve_summaries(cls, connection_resolver_query, return_result_set=True, **kwargs):
        summary_result = dict()
        result_set = None
        total_data_size = None

        if 'summaries' in kwargs:
            total_data_size = connection_resolver_query.count()
            target_summaries = kwargs.get('summaries')
            db_summary_result = dict()

            db_summarizers, result_set_summarizers = cls.get_summarizers(target_summaries)
            summarize_db = (kwargs.get('summarize_db') or total_data_size > cls.DB_SUMMARIZATION_THRESHOLD) and len(db_summarizers) > 0
            if result_set is None and summarize_db:
                db_summary_result, result_set = cls.compute_db_summaries(target_summaries, db_summarizers, connection_resolver_query, return_result_set)

            result_set_summary_result = dict()
            if len(db_summary_result) < len(target_summaries):
                for key in db_summary_result:
                    target_summaries.pop(key)
                    result_set_summarizers.pop(key)

                if len(result_set_summarizers) > 0:
                    if result_set is None:
                        result_set = connection_resolver_query.execute(to_object=False)

                    result_set_summary_result = cls.compute_result_set_summaries(target_summaries, result_set_summarizers, result_set, summary_result=db_summary_result)

            summary_result = {**db_summary_result, **result_set_summary_result}

        return summary_result, total_data_size, connection_resolver_query.to_object(result_set) if return_result_set else None



    @classmethod
    def update_connection_properties(cls, connection, summary_results):
        for interface, summary_result in summary_results.items():
            connection.update_resolved(interface, summary_result)



    @classmethod
    def connection_resolver(cls, resolver, connection_type, root, info, **kwargs):
        resolved = resolver(root, info, **kwargs)
        if isinstance(resolved, ConnectionResolverQuery):
            connection_resolver_query = resolved
            if kwargs.get('summariesOnly'):

                summary_result, total_data_size, _ = cls.resolve_summaries(
                    connection_resolver_query,
                    return_result_set=False,
                    **kwargs
                )
                iterable = []
                connection = connection_from_list(
                    iterable,
                    kwargs,
                    connection_type=connection_type,
                    pageinfo_type=PageInfo,
                    edge_type=connection_type.Edge,
                )
                connection.iterable = iterable
                connection.count = total_data_size or connection_resolver_query.count()
                cls.update_connection_properties(
                    connection,
                    summary_result
                )


            elif is_paging(kwargs):
                summary_result, total_data_size,  _ = cls.resolve_summaries(
                    connection_resolver_query,
                    return_result_set=False,
                    **kwargs
                )
                # In this case we are relying on the
                # paging capabilities of the connection_resolver_query to apply
                # LIMIT and OFFSET to the query based on the slice requested from
                # connection kwargs and only extract a subset of query rows
                count = total_data_size or connection_resolver_query.count()
                connection = connection_from_list_slice(
                    connection_resolver_query,
                    kwargs,
                    slice_start=0,
                    list_length=count,
                    list_slice_length=count,
                    connection_type=connection_type,
                    pageinfo_type=PageInfo,
                    edge_type=connection_type.Edge,
                )
                connection.iterable = resolved
                connection.count = count
                cls.update_connection_properties(
                    connection,
                    summary_result
                )

            else:
                # if not we are getting summaries and full result sets.
                # first try and resolve summaries, and use the full
                # result set returned from this, if any for the connection
                summary_result, _, iterable = cls.resolve_summaries(
                    connection_resolver_query,
                    return_result_set=True,
                    **kwargs
                )
                if not iterable:
                    # In this case we need to finally execute the query on our own
                    # and get the data
                    iterable = connection_resolver_query.execute()

                count = len(iterable)
                connection = connection_from_list(
                    iterable,
                    kwargs,
                    connection_type=connection_type,
                    pageinfo_type=PageInfo,
                    edge_type=connection_type.Edge,
                )
                connection.iterable = iterable
                connection.count = count
                cls.update_connection_properties(
                    connection,
                    summary_result
                )

        else:
            connection = super().resolve_connection(connection_type, kwargs, resolved)

        return connection

    def get_resolver(self, parent_resolver):
        return partial(self.connection_resolver, parent_resolver, self.type)


class ConnectionObjectOptions(ObjectTypeOptions):
    summaries = None
    summaries_enum = None


class CountableConnection(Connection):
    _meta_extra = None

    @classmethod
    def __init_subclass_with_meta__(cls, summaries=(), interfaces=(), node=None, name=None, **options):
        _meta = ConnectionObjectOptions(cls)

        _interfaces = interfaces
        if len(summaries) > 0:
            _meta.summaries = summaries
            _interfaces = (*_interfaces, *summaries)

            _meta.summaries_enum = graphene.Enum(
                f'{cls.__name__}ConnectionSummaries', [
                    (interface.__name__, interface.__name__)
                    for interface in summaries
                ])


        # Base connection class does not respect passed in _meta object
        # and freezes it after initialization, so we need to hack our way
        # around it. _meta_extra is and using meta class method for access is our way of doing this
        cls._meta_extra = _meta
        super().__init_subclass_with_meta__(node, name, interfaces=_interfaces, **options)

    @classmethod
    def meta(cls, attr):
        return getattr(cls._meta, attr, None) or getattr(cls._meta_extra, attr, None)

    class Meta:
        abstract = True

    count = graphene.Int()




def count(selectable):
    alias = selectable.alias()
    return select([func.count(alias.c.key)]).select_from(alias)


class ConnectionResolverQuery(ConnectionQuery):



    def __init__(self, named_node_resolver, interface_resolvers, resolver_context, params, output_type=None, **kwargs):
        super().__init__(**kwargs)
        self.resolver_context = resolver_context
        self.query = cte_join(named_node_resolver, collect_join_resolvers(interface_resolvers, **kwargs),
                              resolver_context, **kwargs)
        self.output_type = output_type
        self.params = params
        self.temp_table = None

    def count(self):
        with db.create_session() as session:
            return session.execute(count(self.query), self.params).scalar()

    @contextmanager
    def create_temp_table(self, session):
        try:
            if self.temp_table is None:
                self.temp_table = db.create_temp_table(f'{self.resolver_context}_connection_temp', self.query.c)
                self.temp_table.create(session.connection)

            insert_temp_table = self.temp_table.insert().from_select(
                self.query.c,
                self.query
            )
            session.connection.execute(insert_temp_table, self.params)
            yield self.temp_table
        finally:
            self.temp_table = None

    def select_temp_table(self, join_session=None, to_object=True):
        with db.create_session(join_session) as session:
            result = session.connection.execute(select(self.temp_table.c)).fetchall()
            return self.to_object(result) if self.output_type and to_object else result

    def to_object(self, result):
        return [
            self.output_type(**{key: value for key, value in row.items()})
            for row in result
        ] if result is not None and self.output_type else []

    def execute(self, join_session=None, to_object=True):
        if self.temp_table is not None:
            return self.select_temp_table(join_session, to_object)

        base_query = self.query
        if self.limit:
            base_query = base_query.limit(self.limit)

        if self.offset:
            base_query = base_query.offset(self.offset)

        with db.create_session(join_session) as session:
            result = session.execute(base_query, self.params).fetchall()
            return self.to_object(result ) if self.output_type and to_object else result

class ConnectionSummarizerOptions(ObjectTypeOptions):
    interface = None
    connection_property = None


class ConnectionSummarizer(SubclassWithMeta):

    registry = dict()

    @classmethod
    def __init_subclass_with_meta__(cls, interface=None, connection_property=None,  **meta_options):

        _meta = ConnectionSummarizerOptions(cls)
        _meta.interface = interface
        if interface:
            interface_name = interface.__name__
            _meta.connection_property = connection_property or snake_case(interface_name)
            cls.register(interface_name, cls)

        cls._meta = _meta
        super().__init_subclass_with_meta__(**meta_options)

    @classmethod
    def register(cls, interface_name, summarizer):
        cls.registry[interface_name] = summarizer

    @classmethod
    def get_summarizer(cls, interface_name):
        return cls.registry.get(interface_name)




    @classmethod
    def meta(cls, attr):
        return getattr(cls._meta, attr, None)





