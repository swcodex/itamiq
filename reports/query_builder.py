from django.db.models import Q
from .models import Table, Column, Relationship
from django.db import connections
from django.core.paginator import Paginator
from .graph_processor import get_all_related_tables
import logging
import sys 


logger = logging.getLogger(__name__)

class QueryBuilder:
    def __init__(self, selected_columns, main_table_id, where_clause='', params=None, pagination=None):
        self.selected_columns = selected_columns
        self.where_clause = where_clause
        self.params = params or []
        logger.debug(f"QueryBuilder initialized with WHERE clause: {self.where_clause}")
        logger.debug(f"QueryBuilder initialized with params: {self.params}")
        self.pagination = pagination
        self.tables = set(column.table for column in selected_columns)
        self.all_related_tables = self._get_all_related_tables()
        self.main_table = self._identify_main_table(main_table_id)
        self.joined_tables = set([self.main_table.name])

    def _get_all_related_tables(self):
        all_related_tables = set()
        for table in self.tables:
            related = get_all_related_tables(table.id)
            logger.debug(f"Related tables for {table.name}: {[t.name for t in related]}")
            all_related_tables.update(related)
        return all_related_tables

    def _identify_main_table(self, main_table_id):
        main_table = Table.objects.get(id=main_table_id)
        logger.info(f"Using user-selected main table: {main_table.name}")
        return main_table

    def _build_select_clause(self):
        select_clause = ", ".join([f"{column.table.name}.{column.name}" for column in self.selected_columns])
        logger.debug(f"Built SELECT clause: {select_clause}")
        return select_clause

    def _build_join_clause(self):
        join_clause = ""
        tables_to_join = self.tables.copy()
        tables_to_join.remove(self.main_table)

        logger.debug(f"Tables to join: {[t.name for t in tables_to_join]}")
        for table in tables_to_join:
            join_path = self._find_join_path(self.main_table, table)
            logger.debug(f"Join path for {table.name}: {[r.from_table.name + ' -> ' + r.to_table.name for r in join_path] if join_path else 'No path found'}")
            if join_path:
                join_clause += self._process_join_path(join_path)
            else:
                logger.warning(f"Could not find join path to {table.name}")

        logger.debug(f"Built JOIN clause: {join_clause}")
        return join_clause

    def _find_join_path(self, from_table, to_table):
        logger.debug(f"Finding join path from {from_table.name} to {to_table.name}")
        queue = [(from_table, [])]
        visited = set()

        while queue:
            current_table, path = queue.pop(0)
            if current_table == to_table:
                logger.debug(f"Path found: {[r.from_table.name + ' -> ' + r.to_table.name for r in path]}")
                return path

            if current_table in visited:
                continue

            visited.add(current_table)

            relationships = Relationship.objects.filter(Q(from_table=current_table) | Q(to_table=current_table))
            logger.debug(f"Relationships for {current_table.name}: {relationships}")
            for rel in relationships:
                next_table = rel.to_table if rel.from_table == current_table else rel.from_table
                if next_table not in visited:
                    new_path = path + [rel]
                    queue.append((next_table, new_path))

        logger.debug(f"No path found from {from_table.name} to {to_table.name}")
        return None

    def _process_join_path(self, join_path):
        join_clause = ""
        for relationship in join_path:
            from_table = relationship.from_table
            to_table = relationship.to_table
            if to_table.name not in self.joined_tables:
                condition = f"{from_table.name}.{relationship.from_column.name} = {to_table.name}.{relationship.to_column.name}"
                join_clause += f" LEFT JOIN {to_table.name} ON {condition}"
                self.joined_tables.add(to_table.name)
                logger.debug(f"Added join: {join_clause}")
            elif from_table.name not in self.joined_tables:
                condition = f"{to_table.name}.{relationship.to_column.name} = {from_table.name}.{relationship.from_column.name}"
                join_clause += f" LEFT JOIN {from_table.name} ON {condition}"
                self.joined_tables.add(from_table.name)
                logger.debug(f"Added join: {join_clause}")
        return join_clause

    def _build_where_clause(self):
        return f" WHERE {self.where_clause}" if self.where_clause else ""
    '''
    def _build_where_clause(self):
        if not self.filters:
            return ""
        where_conditions = []
        for column_id, operator, value in self.filters:
            column = Column.objects.get(id=column_id)
            where_conditions.append(f"{column.table.name}.{column.name} {operator} %s")
        where_clause = " WHERE " + " AND ".join(where_conditions)
        logger.debug(f"Built WHERE clause: {where_clause}")
        return where_clause
        '''
    def _build_pagination_clause(self):
        if not self.pagination:
            return ""
        limit = self.pagination.get('limit', 10)
        offset = self.pagination.get('offset', 0)
        pagination_clause = f" LIMIT {limit} OFFSET {offset}"
        logger.debug(f"Built PAGINATION clause: {pagination_clause}")
        return pagination_clause

    def build_query(self, count_only=False):
        if count_only:
            select_clause = "COUNT(*) as total_count"
        else:
            select_clause = self._build_select_clause()
        
        join_clause = self._build_join_clause()
        where_clause = self._build_where_clause()
        
        #query = f"SELECT {select_clause} FROM {self.main_table.name}{join_clause}{where_clause}"
        query = f"SELECT {select_clause} FROM {self.main_table.name}{join_clause}{where_clause}"

        if not count_only:
            pagination_clause = self._build_pagination_clause()
            query += pagination_clause
        
        logger.info(f"Final query: {query}")
        logger.debug(f"Query params: {self.params}")
        return query

def build_query(selected_columns, main_table_id, where_clause='', params=None, pagination=None, count_only=False):
    query_builder = QueryBuilder(selected_columns, main_table_id, where_clause, params, pagination)
    return query_builder.build_query(count_only)


def translate_query_builder_rules(rules):
    logger.debug(f"Translating rules: {rules}")
    if not rules or 'rules' not in rules:
        logger.debug("No rules to translate")
        return '', []

    where_clauses = []
    params = []

    def process_rule(rule):
        if 'condition' in rule:
            nested_clauses, nested_params = process_group(rule)
            where_clauses.append(f"({nested_clauses})")
            params.extend(nested_params)
        else:
            column_id = rule['field']
            operator = rule['operator']
            value = rule['value']

            # Get the actual column name and table name
            column = Column.objects.get(id=column_id)
            field = f"{column.table.name}.{column.name}"

            if operator == 'equal':
                where_clauses.append(f"{field} = %s")
                params.append(value)
            elif operator == 'not_equal':
                where_clauses.append(f"{field} != %s")
                params.append(value)
            elif operator == 'in':
                where_clauses.append(f"{field} IN %s")
                params.append(tuple(value))
            elif operator == 'not_in':
                where_clauses.append(f"{field} NOT IN %s")
                params.append(tuple(value))
            elif operator == 'less':
                where_clauses.append(f"{field} < %s")
                params.append(value)
            elif operator == 'less_or_equal':
                where_clauses.append(f"{field} <= %s")
                params.append(value)
            elif operator == 'greater':
                where_clauses.append(f"{field} > %s")
                params.append(value)
            elif operator == 'greater_or_equal':
                where_clauses.append(f"{field} >= %s")
                params.append(value)
            elif operator == 'between':
                where_clauses.append(f"{field} BETWEEN %s AND %s")
                params.extend(value)
            elif operator == 'not_between':
                where_clauses.append(f"{field} NOT BETWEEN %s AND %s")
                params.extend(value)
            elif operator == 'begins_with':
                where_clauses.append(f"{field} LIKE %s")
                params.append(f"{value}%")
            elif operator == 'not_begins_with':
                where_clauses.append(f"{field} NOT LIKE %s")
                params.append(f"{value}%")
            elif operator == 'contains':
                where_clauses.append(f"{field} LIKE %s")
                params.append(f"%{value}%")
            elif operator == 'not_contains':
                where_clauses.append(f"{field} NOT LIKE %s")
                params.append(f"%{value}%")
            elif operator == 'ends_with':
                where_clauses.append(f"{field} LIKE %s")
                params.append(f"%{value}")
            elif operator == 'not_ends_with':
                where_clauses.append(f"{field} NOT LIKE %s")
                params.append(f"%{value}")
            elif operator == 'is_empty':
                where_clauses.append(f"({field} = '' OR {field} IS NULL)")
            elif operator == 'is_not_empty':
                where_clauses.append(f"({field} != '' AND {field} IS NOT NULL)")
            elif operator == 'is_null':
                where_clauses.append(f"{field} IS NULL")
            elif operator == 'is_not_null':
                where_clauses.append(f"{field} IS NOT NULL")
            else:
                raise ValueError(f"Unsupported operator: {operator}")

    def process_group(group):
        if 'condition' not in group or 'rules' not in group:
            raise ValueError("Invalid group structure")

        condition = group['condition'].upper()
        if condition not in ('AND', 'OR'):
            raise ValueError(f"Invalid condition: {condition}")

        group_clauses = []
        group_params = []

        for rule in group['rules']:
            process_rule(rule)

        return f" {condition} ".join(where_clauses), params

    return process_group(rules)



def execute_query(query, params=None):
    with connections['itam'].cursor() as cursor:
        cursor.execute(query, params)
        if query.strip().upper().startswith('SELECT COUNT'):
            return cursor.fetchone()[0]  # Return the count directly
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_paginated_results(selected_columns, main_table_id, where_clause, params, page=1, per_page=10):
    pagination = {'limit': per_page, 'offset': (page - 1) * per_page}
    
    # Get total count
    count_query = build_query(selected_columns, main_table_id, where_clause, params, count_only=True)
    total_count = execute_query(count_query, params)
    
    # Get paginated results
    results_query = build_query(selected_columns, main_table_id, where_clause, params, pagination)
    results = execute_query(results_query, params)
    
    paginator = Paginator(range(total_count), per_page)
    page_obj = paginator.get_page(page)
    
    return {
        'results': results or [], # Ensure this is always a list
        'total_count': total_count,
        'page_obj': page_obj,
    }