# -*- coding: utf-8 -*-

# Copyright: © Exathink, LLC (2016-2017) All Rights Reserved

# Unauthorized use or copying of this file and its contents, via any medium
# is strictly prohibited. The work product in this file is proprietary and
# confidential.

# Author: Krishna Kumar
import logging

from sqlalchemy import text, select, join

from polaris.common import db
from polaris.graphql.utils import properties
from .utils import is_paging, GraphQLImplementationError, is_required


def resolve_local_join(result_rows, join_field, output_type):
    if len(result_rows) == 1:
        instances = result_rows[0]
    else:
        instance_hash = {}
        for rows in result_rows:
            for row in rows:
                join_value = row[join_field]
                if join_value is not None:
                    current = instance_hash.get(join_value, None)
                    if current is None:
                        instance_hash[join_value] = {}
                    for key, value in row.items():
                        instance_hash[join_value][key] = value
        instances = instance_hash.values()

    return [output_type(**instance) for instance in instances]


def text_join(resolvers, resolver_context, join_field='id', **kwargs):
    alias = lambda interface: interface.__name__

    if len(resolvers) > 0:
        # build a list of output columns for the queries
        # list is built by unqualified names reading from left to right on the list of queries.
        # if there are duplicate columns between queries the summary one encountered is selected and rest are
        # dropped from the output columns. The resulting set of columns must be a valid
        # set of attributes to pass on to the constructor of output_type.
        seen_columns = set()
        output_columns = []
        for resolver in resolvers:
            for field in properties(resolver.interface):
                if field not in seen_columns:
                    seen_columns.add(field)
                    output_columns.append(text(f'{alias(resolver.interface)}.{field}'))

        # Convert input pairs (interface, raw-sql) into pairs (table_alias, text(raw-sql) tuples
        # these will be user to construct the final join statement
        subqueries = [
            (alias(resolver.interface), text(f"({resolver.query}) AS {alias(resolver.interface)}"))
            for resolver in resolvers
        ]

        # Create the join statement:
        # join statements are of the form subquery[0] left outer join subquery[i] on alias[0].join_field = alias[i].field for i > 0
        # In practice subquery[0] will be the named node generator, so this will contain all the entities in the space,
        # but otherwise it is possible that we return less tha the full set of entities in the space.
        root_alias, selectable = subqueries[0]
        for alias, subquery in subqueries[1:]:
            selectable = join(selectable, subquery, onclause=text(f"{root_alias}.{join_field} = {alias}.{join_field}"),
                              isouter=True)

        # Select the output columns from the resulting join
        return select(output_columns).select_from(selectable)


def resolve_remote_join(queries, output_type, join_field='id', params=None):
    with db.create_session() as session:
        result = session.execute(text_join(queries, join_field), params).fetchall()
        return [output_type(**{key: value for key, value in row.items()}) for row in result]


def resolve_named_node_resolver_for_view(named_node_resolver, **kwargs):
    if hasattr(named_node_resolver, 'views'):
        view = kwargs.get('view', 'Default')
        return named_node_resolver.named_node_resolvers.get(view)
    else:
        return named_node_resolver


def get_named_node_resolver_interface_fields(named_node_resolver):
    if hasattr(named_node_resolver, 'interface'):
        return properties(named_node_resolver.interface)
    elif hasattr(named_node_resolver, 'interfaces'):
        fields = []
        for interface in named_node_resolver.interfaces:
            fields.extend(properties(interface))
        return fields
    else:
        return []


def cte_join(named_nodes_resolver, subquery_resolvers, resolver_context, join_field='id', **kwargs):
    named_nodes_selector = getattr(named_nodes_resolver, 'named_node_selector',
                                     getattr(named_nodes_resolver, 'connection_nodes_selector',
                                             getattr(named_nodes_resolver, 'selectable', None)))
    if named_nodes_selector is None:
        raise GraphQLImplementationError(
            f'Context: {resolver_context} Resolver: {named_nodes_resolver.__name__}: '
            f' Could not resolve named_nodes_selector'
        )

    if len(subquery_resolvers) > 0:
        named_nodes_query = named_nodes_selector(**kwargs).cte(resolver_context)
    else:
        named_nodes_query = named_nodes_selector(**kwargs).alias(resolver_context)

    subqueries = []
    sort_order = []

    if hasattr(named_nodes_resolver, 'sort_order'):
        sort_order.extend(named_nodes_resolver.sort_order(named_nodes_query, **kwargs))

    for resolver in subquery_resolvers:
        interface_selector = getattr(resolver, 'interface_selector', getattr(resolver, 'selectable', None))
        if interface_selector is None:
            raise GraphQLImplementationError(
                f'Context: {resolver_context} Resolver: {resolver.__name__}: '
                f' Could not resolve interface_selector'
            )

        selectable = interface_selector(named_nodes_query, **kwargs).alias(resolver.interface.__name__)
        subqueries.append((resolver.interface, selectable))
        if is_paging(kwargs) and getattr(resolver, 'sort_order', None):
            sort_order.extend(resolver.sort_order(selectable, **kwargs))

    seen_columns = set()
    output_columns = []

    # Add all the columns from the named node CTE
    for col in get_named_node_resolver_interface_fields(named_nodes_resolver):
        if col in named_nodes_query.columns:
            seen_columns.add(col)
            output_columns.append(named_nodes_query.c[col])
        else:
            raise GraphQLImplementationError(f"Named node selector query for {named_nodes_resolver}  does not return an expected column named  {col}")

    # Add the columns from the subqueries based on the interfaces they expose
    for interface, selectable in subqueries:
        for field in properties(interface):
            if field not in seen_columns:
                if field in selectable.c:
                    seen_columns.add(field)
                    output_columns.append(selectable.c[field])
                else:
                    if is_required(field, interface):
                        raise GraphQLImplementationError(
                            f'Context: {resolver_context} Resolver: {resolver.__name__}: '
                            f' Selectable query for interface {interface} does not define a value for column {field}'
                        )

    joined = named_nodes_query
    for _, selectable in subqueries:
        joined = joined.outerjoin(selectable, named_nodes_query.c[join_field] == selectable.c[join_field])
    # Select the output columns from the resulting join
    query = select(output_columns).select_from(joined)

    if 'apply_distinct' in kwargs:
        if hasattr(named_nodes_resolver, 'apply_distinct_columns'):
            apply_distinct_columns = named_nodes_resolver.apply_distinct_columns()
            query = query.distinct(*[col for col in query.c if col.name in apply_distinct_columns])
        else:
            query = query.distinct()

    if len(sort_order) > 0:
        query = query.order_by(*sort_order)

    return query


def resolve_join(named_node_resolver, interface_resolvers, resolver_context, params, output_type=None, join_field='id',
                 **kwargs):
    with db.orm_session() as session:
        query = cte_join(named_node_resolver, interface_resolvers, resolver_context, join_field, **kwargs)
        result = session.connection().execute(query, params).fetchall()
        return [
            output_type(**{key: value for key, value in row.items()})
            for row in result
        ] if output_type else result


def collect_join_resolvers(interface_resolvers, **kwargs):
    interfaces = [interface
                  for interface in set(kwargs.get('interfaces', [])) | set(kwargs.get('interface', []))]
    return [interface_resolvers.get(interface) for interface in interfaces if
            interface_resolvers.get(interface) is not None]


def resolve_collection(named_node_resolver, interface_resolvers, resolver_context, params, **kwargs):
    resolvers = collect_join_resolvers(interface_resolvers, **kwargs)
    return resolve_join(named_node_resolver, resolvers, resolver_context, params, **kwargs)


def resolve_instance(named_node_resolver, interface_resolvers, resolver_context, params, **kwargs):
    resolved = resolve_collection(named_node_resolver, interface_resolvers, resolver_context, params, **kwargs)
    return resolved[0] if len(resolved) == 1 else None
