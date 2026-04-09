"""
Task 1: Index Advisor (Easy)
Suggest CREATE INDEX statements for slow queries with inefficient WHERE clauses.
"""

TASK_CONFIG = {
    "name": "index-advisor",
    "difficulty": "easy",
    "weight": 0.7,
    "score": 0.7,
    "description": "Suggest indexes to optimize slow WHERE clause queries",
    "grader": {
        "name": "index-advisor-grader",
        "type": "deterministic",
        "config": {
            "criteria": [
                "Identifies correct tables and columns for indexing",
                "Performance improvement of 1.5x or better",
                "Avoids over-indexing (max 5 indexes)"
            ]
        },
        "criteria": [
            "Identifies correct tables and columns for indexing",
            "Performance improvement of 1.5x or better",
            "Avoids over-indexing (max 5 indexes)"
        ]
    },
    
    "initial_query": """
        SELECT u.name, u.email, COUNT(o.order_id) as order_count
        FROM users u
        LEFT JOIN orders o ON u.user_id = o.user_id
        WHERE u.country = 'USA' AND o.status = 'completed'
        GROUP BY u.user_id, u.name, u.email
        HAVING COUNT(o.order_id) > 5
        ORDER BY order_count DESC;
    """,
    
    "hint": "This query filters on users.country and orders.status without indexes. Consider which columns are used in WHERE clauses.",
    
    "success_criteria": {
        "required_indexes": [
            {"table": "users", "column": "country"},
            {"table": "orders", "column": "status"}
        ],
        "min_speedup": 1.5,  # At least 1.5x faster
        "max_indexes": 3  # Don't over-index
    }
}


def get_expected_solution() -> str:
    """
    Return the expected optimal solution for validation.
    """
    return """
    CREATE INDEX idx_users_country ON users(country);
    CREATE INDEX idx_orders_status ON orders(status);
    CREATE INDEX idx_orders_user_id ON orders(user_id);
    """


def get_explanation() -> str:
    """
    Return explanation of the optimization strategy.
    """
    return """
    The query performs filtering on users.country and orders.status, both of which
    benefit from indexes to avoid full table scans. Additionally, the JOIN on 
    orders.user_id should be indexed for efficient lookups.
    
    Recommended indexes:
    1. idx_users_country - Speeds up WHERE u.country = 'USA'
    2. idx_orders_status - Speeds up WHERE o.status = 'completed'
    3. idx_orders_user_id - Speeds up JOIN operations
    
    Expected performance improvement: 2-5x faster query execution.
    """
