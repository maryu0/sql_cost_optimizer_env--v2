"""
Task 2: Query Rewriter (Medium)
Rewrite inefficient queries (N+1 patterns, subqueries) into efficient JOINs.
"""

TASK_CONFIG = {
    "name": "query-rewriter",
    "difficulty": "medium",
    "weight": 0.75,
    "score": 0.75,
    "description": "Rewrite subqueries into efficient JOINs for better performance",
    "grader": {
        "name": "query-rewriter-grader",
        "type": "deterministic",
        "config": {
            "criteria": [
                "Uses JOIN instead of subqueries",
                "Returns identical results to original query",
                "Performance improvement of 2x or better",
                "Includes proper GROUP BY for aggregations"
            ]
        },
        "criteria": [
            "Uses JOIN instead of subqueries",
            "Returns identical results to original query",
            "Performance improvement of 2x or better",
            "Includes proper GROUP BY for aggregations"
        ]
    },
    
    "initial_query": """
        SELECT 
            p.product_id,
            p.name,
            p.price,
            (SELECT COUNT(*) FROM order_items oi WHERE oi.product_id = p.product_id) as times_ordered,
            (SELECT SUM(oi.quantity) FROM order_items oi WHERE oi.product_id = p.product_id) as total_quantity,
            (SELECT AVG(oi.price) FROM order_items oi WHERE oi.product_id = p.product_id) as avg_sale_price
        FROM products p
        WHERE p.category = 'Electronics'
        ORDER BY times_ordered DESC
        LIMIT 20;
    """,
    
    "hint": "This query has correlated subqueries that execute once per row. Consider rewriting with LEFT JOIN and GROUP BY.",
    
    "success_criteria": {
        "must_use_join": True,
        "no_subqueries_in_select": True,
        "min_speedup": 2.0,  # At least 2x faster
        "results_must_match": True
    }
}


def get_expected_solution() -> str:
    """
    Return the expected optimal solution for validation.
    """
    return """
    SELECT 
        p.product_id,
        p.name,
        p.price,
        COALESCE(COUNT(oi.order_item_id), 0) as times_ordered,
        COALESCE(SUM(oi.quantity), 0) as total_quantity,
        COALESCE(AVG(oi.price), 0.0) as avg_sale_price
    FROM products p
    LEFT JOIN order_items oi ON p.product_id = oi.product_id
    WHERE p.category = 'Electronics'
    GROUP BY p.product_id, p.name, p.price
    ORDER BY times_ordered DESC
    LIMIT 20;
    """


def get_explanation() -> str:
    """
    Return explanation of the optimization strategy.
    """
    return """
    The original query uses three correlated subqueries in the SELECT clause,
    each scanning the order_items table once per product. This creates an N+1
    query pattern that becomes extremely slow with large datasets.
    
    Optimization strategy:
    1. Replace correlated subqueries with a single LEFT JOIN
    2. Use GROUP BY to aggregate order_items data per product
    3. Use COALESCE to handle products with no orders (NULL handling)
    4. Maintain identical results while reducing query complexity from O(N*M) to O(N)
    
    Performance improvement: 3-10x faster depending on data size.
    This optimization reduces database round-trips and leverages JOIN optimizations.
    """
