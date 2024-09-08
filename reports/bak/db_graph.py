from collections import defaultdict
from typing import Dict, List, Tuple

class DBGraph:
    def __init__(self):
        self.graph = defaultdict(dict)

    def add_relationship(self, table1: str, table2: str, fk_column: str, referenced_column: str):
        """Add a relationship between two tables to the graph."""
        self.graph[table1][table2] = (fk_column, referenced_column)
        self.graph[table2][table1] = (referenced_column, fk_column)

    def get_related_tables(self, table: str) -> List[str]:
        """Get all tables directly related to the given table."""
        return list(self.graph[table].keys())

    def find_path(self, start: str, end: str) -> List[Tuple[str, str, str, str]]:
        """Find a path between two tables using BFS."""
        visited = set()
        queue = [(start, [])]
        
        while queue:
            (node, path) = queue.pop(0)
            if node not in visited:
                visited.add(node)
                
                if node == end:
                    return path
                
                for next_node in self.graph[node]:
                    if next_node not in visited:
                        fk_column, referenced_column = self.graph[node][next_node]
                        new_path = path + [(node, next_node, fk_column, referenced_column)]
                        queue.append((next_node, new_path))
        
        return []  # No path found

    def get_all_tables(self) -> List[str]:
        """Get all tables in the graph."""
        return list(self.graph.keys())

def build_graph_from_db_schema(cursor) -> DBGraph:
    """Build a DBGraph instance from the database schema."""
    graph = DBGraph()
    
    # Query to get foreign key relationships
    cursor.execute("""
        SELECT 
            TABLE_NAME,
            COLUMN_NAME,
            REFERENCED_TABLE_NAME,
            REFERENCED_COLUMN_NAME
        FROM
            INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE
            REFERENCED_TABLE_SCHEMA = DATABASE()
            AND REFERENCED_TABLE_NAME IS NOT NULL
    """)
    
    for table, column, referenced_table, referenced_column in cursor.fetchall():
        graph.add_relationship(table, referenced_table, column, referenced_column)
    
    return graph


def find_all_paths(self, start: str, end: str, path=None):
    if path is None:
        path = []
    path = path + [start]
    if start == end:
        return [path]
    if start not in self.graph:
        return []
    paths = []
    for node in self.graph[start]:
        if node not in path:
            newpaths = self.find_all_paths(node, end, path)
            for newpath in newpaths:
                paths.append(newpath)
    return paths
