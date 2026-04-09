"""
Deterministic graders for SQL optimization tasks.
Each grader returns a score between 0.0 and 1.0 based on objective criteria.
"""
import re
from typing import Dict, Any, List, Tuple
from src.utils.db_executor import DatabaseExecutor


class IndexAdvisorGrader:
    """
    Grades Task 1: Index Advisor
    Checks if suggested indexes match required columns and tables.
    """

    def grade(
        self,
        action_sql: str,
        db: DatabaseExecutor,
        baseline_time_ms: float,
        optimized_time_ms: float
    ) -> Tuple[float, str]:
        """
        Grade the index advisor action.
        
        Args:
            action_sql: The CREATE INDEX statements
            db: Database executor instance
            baseline_time_ms: Baseline query execution time
            optimized_time_ms: Optimized query execution time
            
        Returns:
            Tuple of (score, feedback)
        """
        score = 0.01
        feedback_parts = []

        # Extract CREATE INDEX statements
        index_pattern = r'CREATE\s+INDEX\s+(\w+)\s+ON\s+(\w+)\s*\(([^)]+)\)'
        matches = re.findall(index_pattern, action_sql, re.IGNORECASE)

        if not matches:
            return 0.01, "No valid CREATE INDEX statements found."

        # Required indexes for optimal performance
        required_indexes = {
            ("users", "country"),
            ("orders", "status"),
            ("orders", "user_id")
        }

        found_indexes = set()
        for index_name, table_name, columns in matches:
            # Parse column names (handle multi-column indexes)
            column_list = [col.strip().lower() for col in columns.split(',')]
            for col in column_list:
                found_indexes.add((table_name.lower(), col))

        # Calculate score based on required indexes found
        correct_indexes = found_indexes & required_indexes
        score = len(correct_indexes) / len(required_indexes) if required_indexes else 0.0
        score = max(0.01, min(0.99, score))  # Ensure within range

        # Feedback
        if score >= 0.9:
            feedback_parts.append("✓ All required indexes identified correctly!")
        elif score >= 0.6:
            feedback_parts.append("✓ Most required indexes identified.")
            missing = required_indexes - found_indexes
            if missing:
                feedback_parts.append(f"Missing: {missing}")
        else:
            feedback_parts.append("✗ Insufficient indexes identified.")
            missing = required_indexes - found_indexes
            feedback_parts.append(f"Required but missing: {missing}")

        # Bonus for performance improvement
        if optimized_time_ms > 0 and baseline_time_ms > 0:
            speedup = baseline_time_ms / optimized_time_ms
            if speedup >= 2.0:
                score = min(0.99, score + 0.1)
                feedback_parts.append(f"✓ Excellent speedup: {speedup:.2f}x")
            elif speedup >= 1.5:
                feedback_parts.append(f"✓ Good speedup: {speedup:.2f}x")
            else:
                feedback_parts.append(f"✗ Minimal speedup: {speedup:.2f}x")

        # Penalty for over-indexing
        if len(matches) > 5:
            score *= 0.8
            feedback_parts.append("✗ Too many indexes (over-indexing can hurt write performance)")

        return max(0.01, min(0.99, score)), " ".join(feedback_parts)


class QueryRewriterGrader:
    """
    Grades Task 2: Query Rewriter
    Checks if query uses JOINs instead of subqueries and maintains correctness.
    """

    def grade(
        self,
        action_sql: str,
        original_results: List[Dict[str, Any]],
        optimized_results: List[Dict[str, Any]],
        baseline_time_ms: float,
        optimized_time_ms: float
    ) -> Tuple[float, str]:
        """
        Grade the query rewriter action.
        
        Args:
            action_sql: The optimized query
            original_results: Results from original query
            optimized_results: Results from optimized query
            baseline_time_ms: Baseline query execution time
            optimized_time_ms: Optimized query execution time
            
        Returns:
            Tuple of (score, feedback)
        """
        score = 0.01
        feedback_parts = []

        # Normalize SQL for analysis
        sql_normalized = action_sql.upper()

        # Check 1: Must use JOIN
        has_join = 'JOIN' in sql_normalized
        if has_join:
            score += 0.3
            feedback_parts.append("✓ Uses JOIN instead of subqueries")
        else:
            feedback_parts.append("✗ Should use JOIN for better performance")

        # Check 2: Should not have subqueries in SELECT clause
        # Pattern: SELECT ... ( ... SELECT ... ) ...
        has_subquery_in_select = bool(
            re.search(r'SELECT[^(]*\([^)]*SELECT', sql_normalized)
        )
        if not has_subquery_in_select:
            score += 0.3
            feedback_parts.append("✓ No correlated subqueries in SELECT")
        else:
            feedback_parts.append("✗ Still contains subqueries in SELECT clause")

        # Check 3: Must use GROUP BY for aggregation
        has_group_by = 'GROUP BY' in sql_normalized
        if has_group_by:
            score += 0.2
            feedback_parts.append("✓ Uses GROUP BY for aggregation")
        else:
            feedback_parts.append("✗ Missing GROUP BY for proper aggregation")

        # Check 4: Results correctness (critical)
        results_match = self._compare_results(original_results, optimized_results)
        if results_match:
            score += 0.2
            feedback_parts.append("✓ Results match original query")
        else:
            score *= 0.5  # Severe penalty for incorrect results
            feedback_parts.append("✗ CRITICAL: Results do not match original query!")

        # Ensure score is within valid range
        score = max(0.01, min(0.99, score))

        # Bonus for performance improvement
        if optimized_time_ms > 0 and baseline_time_ms > 0:
            speedup = baseline_time_ms / optimized_time_ms
            if speedup >= 3.0:
                score = min(0.99, score + 0.15)
                feedback_parts.append(f"✓ Excellent speedup: {speedup:.2f}x")
            elif speedup >= 2.0:
                score = min(0.99, score + 0.1)
                feedback_parts.append(f"✓ Good speedup: {speedup:.2f}x")
            elif speedup >= 1.5:
                feedback_parts.append(f"✓ Moderate speedup: {speedup:.2f}x")
            else:
                feedback_parts.append(f"✗ Insufficient speedup: {speedup:.2f}x (need 2x+)")

        return max(0.01, min(0.99, score)), " ".join(feedback_parts)

    def _compare_results(
        self,
        results1: List[Dict[str, Any]],
        results2: List[Dict[str, Any]]
    ) -> bool:
        """Compare two result sets for equivalence."""
        if len(results1) != len(results2):
            return False

        # Sort both result sets by all columns for comparison
        def sort_key(row: Dict[str, Any]) -> tuple:
            return tuple(str(v) for v in row.values())

        sorted1 = sorted(results1, key=sort_key)
        sorted2 = sorted(results2, key=sort_key)

        return sorted1 == sorted2


class SchemaNormalizerGrader:
    """
    Grades Task 3: Schema Normalizer
    Checks if schema is properly normalized with foreign keys and data integrity.
    """

    def grade(
        self,
        action_sql: str,
        db: DatabaseExecutor
    ) -> Tuple[float, str]:
        """
        Grade the schema normalizer action.
        
        Args:
            action_sql: The normalization SQL (CREATE TABLE, INSERT, etc.)
            db: Database executor instance
            
        Returns:
            Tuple of (score, feedback)
        """
        score = 0.01
        feedback_parts = []

        sql_normalized = action_sql.upper()

        # Check 1: Must create new tables
        create_table_count = len(re.findall(r'CREATE\s+TABLE', sql_normalized))
        if create_table_count >= 2:
            score += 0.25
            feedback_parts.append(f"✓ Created {create_table_count} new tables for normalization")
        else:
            feedback_parts.append("✗ Should create dimension tables for normalization")

        # Check 2: Must have FOREIGN KEY constraints
        has_foreign_keys = 'FOREIGN KEY' in sql_normalized
        if has_foreign_keys:
            score += 0.25
            feedback_parts.append("✓ Includes foreign key constraints")
        else:
            feedback_parts.append("✗ Missing foreign key constraints for referential integrity")

        # Check 3: Should create indexes for foreign keys
        has_indexes = 'CREATE INDEX' in sql_normalized
        if has_indexes:
            score += 0.15
            feedback_parts.append("✓ Creates indexes for efficient lookups")
        else:
            feedback_parts.append("⚠ Consider adding indexes on foreign keys")

        # Check 4: Must have data migration (INSERT statements)
        has_inserts = 'INSERT INTO' in sql_normalized
        if has_inserts:
            score += 0.2
            feedback_parts.append("✓ Includes data migration logic")
        else:
            feedback_parts.append("✗ Missing data migration to populate new tables")

        # Check 5: Look for specific required tables
        required_tables = ['USER_LOCATIONS', 'DEVICES']
        found_tables = []
        for table in required_tables:
            if table in sql_normalized or table.replace('_', '') in sql_normalized:
                found_tables.append(table)

        if len(found_tables) >= 2:
            score += 0.15
            feedback_parts.append("✓ Created required dimension tables")
        elif len(found_tables) == 1:
            score += 0.075
            feedback_parts.append("⚠ Partial dimension tables created")
        else:
            feedback_parts.append("✗ Missing key dimension tables (locations, devices)")

        # Ensure score is within valid range
        score = max(0.01, min(0.99, score))

        return max(0.01, min(0.99, score)), " ".join(feedback_parts)


def get_grader(task_type: str):
    """
    Get the appropriate grader for a task type.
    
    Args:
        task_type: Task type (index-advisor, query-rewriter, schema-normalizer)
        
    Returns:
        Grader instance
    """
    graders = {
        "index-advisor": IndexAdvisorGrader(),
        "query-rewriter": QueryRewriterGrader(),
        "schema-normalizer": SchemaNormalizerGrader()
    }

    if task_type not in graders:
        raise ValueError(f"Unknown task type: {task_type}")

    return graders[task_type]
