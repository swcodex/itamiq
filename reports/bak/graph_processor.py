from collections import deque
from django.db.models import Q
from .models import Table, Relationship


def get_join_conditions(path):
    join_conditions = []
    for i in range(len(path) - 1):
        from_table = path[i]
        to_table = path[i + 1]
        
        relationship = Relationship.objects.filter(
            Q(from_table=from_table, to_table=to_table) |
            Q(from_table=to_table, to_table=from_table)
        ).select_related('from_table', 'to_column', 'to_table', 'from_column').first()

        if relationship:
            condition = f"{relationship.from_table.name}.{relationship.from_column.name} = {relationship.to_table.name}.{relationship.to_column.name}"
            join_conditions.append(condition)

    return join_conditions


def get_all_related_tables(start_table_id, max_depth=4):
    """
    Find all related tables up to a specified depth.
    
    :param start_table_id: The ID of the starting Table
    :param max_depth: Maximum depth of relationships to traverse
    :return: Set of related Table objects
    """
    # Fetch all tables and relationships upfront
    all_tables = {table.id: table for table in Table.objects.all()}
    all_relationships = list(Relationship.objects.all().values('from_table_id', 'to_table_id'))
    
    if start_table_id not in all_tables:
        raise ValueError(f"Table with id {start_table_id} not found")

    start_table = all_tables[start_table_id]
    visited = set()
    related_tables = set()
    tables_to_process = [(start_table_id, 0)]
    
    # Create a dictionary to store relationships for quick lookup
    relationship_dict = {}
    for rel in all_relationships:
        from_id, to_id = rel['from_table_id'], rel['to_table_id']
        relationship_dict.setdefault(from_id, set()).add(to_id)
        relationship_dict.setdefault(to_id, set()).add(from_id)
    
    while tables_to_process:
        current_table_id, depth = tables_to_process.pop(0)
        
        if current_table_id in visited or depth > max_depth:
            continue

        visited.add(current_table_id)
        current_table = all_tables[current_table_id]
        if current_table != start_table:
            related_tables.add(current_table)

        if depth < max_depth:
            for related_table_id in relationship_dict.get(current_table_id, []):
                if related_table_id not in visited:
                    tables_to_process.append((related_table_id, depth + 1))

    return related_tables
