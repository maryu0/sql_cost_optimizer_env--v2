"""
Database executor for running SQL queries, measuring performance,
checking equivalence, and generating EXPLAIN plans.
Uses SQLite in-memory for safety and reproducibility.
"""
import sqlite3
import time
from typing import List, Tuple, Dict, Any, Optional
import hashlib
import json


class DatabaseExecutor:
    """
    Manages SQLite database operations for the environment.
    Provides query execution, performance measurement, and result equivalence checking.
    """

    def __init__(self, db_path: str = ":memory:"):
        """
        Initialize database connection.
        
        Args:
            db_path: SQLite database path (default: in-memory)
        """
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None
        self.cursor: Optional[sqlite3.Cursor] = None
        self._connect()

    def _connect(self):
        """Establish database connection."""
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row  # Enable column access by name
        self.cursor = self.conn.cursor()

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()

    def execute_schema(self, schema_sql: str) -> None:
        """
        Execute schema DDL (CREATE TABLE, CREATE INDEX, etc.).
        
        Args:
            schema_sql: SQL DDL statements
        """
        try:
            self.cursor.executescript(schema_sql)
            self.conn.commit()
        except sqlite3.Error as e:
            raise ValueError(f"Schema execution error: {e}")

    def execute_seed_data(self, seed_sql: str) -> None:
        """
        Execute seed data INSERT statements.
        
        Args:
            seed_sql: SQL INSERT statements
        """
        try:
            self.cursor.executescript(seed_sql)
            self.conn.commit()
        except sqlite3.Error as e:
            raise ValueError(f"Seed data execution error: {e}")

    def execute_query_timed(self, query: str) -> Tuple[List[Dict[str, Any]], float]:
        """
        Execute query and measure execution time.
        
        Args:
            query: SQL query to execute
            
        Returns:
            Tuple of (results, execution_time_ms)
        """
        try:
            # Warm up: Execute once to populate cache
            self.cursor.execute(query)
            self.cursor.fetchall()

            # Actual timing run
            start_time = time.perf_counter()
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            end_time = time.perf_counter()

            execution_time_ms = (end_time - start_time) * 1000

            # Convert Row objects to dictionaries
            results_list = [dict(row) for row in results]

            return results_list, execution_time_ms

        except sqlite3.Error as e:
            raise ValueError(f"Query execution error: {e}")

    def get_explain_plan(self, query: str) -> str:
        """
        Get EXPLAIN QUERY PLAN output for a query.
        
        Args:
            query: SQL query to analyze
            
        Returns:
            EXPLAIN QUERY PLAN output as formatted string
        """
        try:
            self.cursor.execute(f"EXPLAIN QUERY PLAN {query}")
            plan_rows = self.cursor.fetchall()
            
            # Format the plan
            plan_lines = []
            for row in plan_rows:
                # SQLite EXPLAIN QUERY PLAN returns: id, parent, notused, detail
                plan_lines.append(f"  {row[3]}")
            
            return "\n".join(plan_lines) if plan_lines else "No plan available"

        except sqlite3.Error as e:
            return f"Error generating plan: {e}"

    def check_results_equivalent(
        self, 
        results1: List[Dict[str, Any]], 
        results2: List[Dict[str, Any]]
    ) -> bool:
        """
        Check if two query results are equivalent.
        Compares row counts and content (order-agnostic for non-ordered queries).
        
        Args:
            results1: First query results
            results2: Second query results
            
        Returns:
            True if results are equivalent, False otherwise
        """
        # Check row counts
        if len(results1) != len(results2):
            return False

        # If empty, they're equivalent
        if len(results1) == 0:
            return True

        # Sort results for comparison (order-agnostic)
        # Convert to JSON for consistent comparison
        def normalize_row(row: Dict[str, Any]) -> str:
            # Sort keys and convert to JSON
            return json.dumps(row, sort_keys=True, default=str)

        sorted1 = sorted([normalize_row(row) for row in results1])
        sorted2 = sorted([normalize_row(row) for row in results2])

        return sorted1 == sorted2

    def get_sample_data(self, table_name: str, limit: int = 5) -> str:
        """
        Get sample data from a table for preview.
        
        Args:
            table_name: Name of the table
            limit: Maximum number of rows to return
            
        Returns:
            Formatted string with sample data
        """
        try:
            query = f"SELECT * FROM {table_name} LIMIT {limit}"
            self.cursor.execute(query)
            rows = self.cursor.fetchall()

            if not rows:
                return f"Table '{table_name}' is empty"

            # Format as table
            columns = [desc[0] for desc in self.cursor.description]
            lines = [" | ".join(columns)]
            lines.append("-" * len(lines[0]))

            for row in rows:
                lines.append(" | ".join(str(val) for val in row))

            return "\n".join(lines)

        except sqlite3.Error as e:
            return f"Error fetching sample data: {e}"

    def get_table_info(self) -> Dict[str, Any]:
        """
        Get information about all tables in the database.
        
        Returns:
            Dictionary with table names, column info, and row counts
        """
        try:
            # Get all table names
            self.cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%'"
            )
            tables = [row[0] for row in self.cursor.fetchall()]

            table_info = {}
            for table in tables:
                # Get column info
                self.cursor.execute(f"PRAGMA table_info({table})")
                columns = [
                    {"name": col[1], "type": col[2], "pk": bool(col[5])}
                    for col in self.cursor.fetchall()
                ]

                # Get row count
                self.cursor.execute(f"SELECT COUNT(*) FROM {table}")
                row_count = self.cursor.fetchone()[0]

                # Get indexes
                self.cursor.execute(f"PRAGMA index_list({table})")
                indexes = [idx[1] for idx in self.cursor.fetchall()]

                table_info[table] = {
                    "columns": columns,
                    "row_count": row_count,
                    "indexes": indexes
                }

            return table_info

        except sqlite3.Error as e:
            return {"error": str(e)}

    def create_index(self, index_sql: str) -> bool:
        """
        Create an index and return success status.
        
        Args:
            index_sql: CREATE INDEX statement
            
        Returns:
            True if successful, False otherwise
        """
        try:
            self.cursor.execute(index_sql)
            self.conn.commit()
            return True
        except sqlite3.Error:
            return False

    def validate_query_syntax(self, query: str) -> Tuple[bool, str]:
        """
        Validate SQL query syntax without executing.
        
        Args:
            query: SQL query to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            # Use EXPLAIN to validate syntax without execution
            self.cursor.execute(f"EXPLAIN {query}")
            return True, ""
        except sqlite3.Error as e:
            return False, str(e)

    def reset_database(self):
        """Drop all tables and reset to clean state."""
        try:
            # Get all table names
            self.cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%'"
            )
            tables = [row[0] for row in self.cursor.fetchall()]

            # Drop all tables
            for table in tables:
                self.cursor.execute(f"DROP TABLE IF EXISTS {table}")

            self.conn.commit()
        except sqlite3.Error as e:
            raise ValueError(f"Database reset error: {e}")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
