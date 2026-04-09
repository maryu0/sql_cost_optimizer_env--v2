"""
Seed data generator for SQL optimization tasks.
Provides realistic database schemas and sample data for e-commerce and analytics scenarios.
"""


def get_ecommerce_schema() -> str:
    """
    Get e-commerce database schema (denormalized for optimization exercises).
    """
    return """
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        email VARCHAR(255),
        name VARCHAR(255),
        created_at TIMESTAMP,
        country VARCHAR(100)
    );

    CREATE TABLE IF NOT EXISTS products (
        product_id INTEGER PRIMARY KEY,
        name VARCHAR(255),
        category VARCHAR(100),
        price DECIMAL(10, 2),
        stock INTEGER
    );

    CREATE TABLE IF NOT EXISTS orders (
        order_id INTEGER PRIMARY KEY,
        user_id INTEGER,
        total_amount DECIMAL(10, 2),
        status VARCHAR(50),
        created_at TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS order_items (
        order_item_id INTEGER PRIMARY KEY,
        order_id INTEGER,
        product_id INTEGER,
        quantity INTEGER,
        price DECIMAL(10, 2)
    );
    """


def get_ecommerce_seed_data() -> str:
    """
    Get e-commerce seed data (1000 users, 500 products, 2000 orders).
    """
    return """
    -- Insert 1000 users
    INSERT INTO users (user_id, email, name, created_at, country)
    SELECT 
        id,
        'user' || id || '@example.com',
        'User ' || id,
        datetime('2023-01-01', '+' || (id % 365) || ' days'),
        CASE (id % 5)
            WHEN 0 THEN 'USA'
            WHEN 1 THEN 'UK'
            WHEN 2 THEN 'Canada'
            WHEN 3 THEN 'Germany'
            ELSE 'France'
        END
    FROM (
        WITH RECURSIVE cnt(id) AS (
            SELECT 1
            UNION ALL
            SELECT id + 1 FROM cnt WHERE id < 1000
        )
        SELECT id FROM cnt
    );

    -- Insert 500 products
    INSERT INTO products (product_id, name, category, price, stock)
    SELECT 
        id,
        'Product ' || id,
        CASE (id % 10)
            WHEN 0 THEN 'Electronics'
            WHEN 1 THEN 'Clothing'
            WHEN 2 THEN 'Books'
            WHEN 3 THEN 'Home'
            WHEN 4 THEN 'Sports'
            WHEN 5 THEN 'Toys'
            WHEN 6 THEN 'Food'
            WHEN 7 THEN 'Beauty'
            WHEN 8 THEN 'Garden'
            ELSE 'Automotive'
        END,
        (id % 100) + 10.0,
        (id % 500) + 1
    FROM (
        WITH RECURSIVE cnt(id) AS (
            SELECT 1
            UNION ALL
            SELECT id + 1 FROM cnt WHERE id < 500
        )
        SELECT id FROM cnt
    );

    -- Insert 2000 orders
    INSERT INTO orders (order_id, user_id, total_amount, status, created_at)
    SELECT 
        id,
        ((id - 1) % 1000) + 1,
        (id % 200) + 20.0,
        CASE (id % 4)
            WHEN 0 THEN 'completed'
            WHEN 1 THEN 'pending'
            WHEN 2 THEN 'shipped'
            ELSE 'cancelled'
        END,
        datetime('2023-01-01', '+' || (id % 365) || ' days')
    FROM (
        WITH RECURSIVE cnt(id) AS (
            SELECT 1
            UNION ALL
            SELECT id + 1 FROM cnt WHERE id < 2000
        )
        SELECT id FROM cnt
    );

    -- Insert 5000 order items
    INSERT INTO order_items (order_item_id, order_id, product_id, quantity, price)
    SELECT 
        id,
        ((id - 1) % 2000) + 1,
        ((id - 1) % 500) + 1,
        (id % 5) + 1,
        ((id % 100) + 10.0)
    FROM (
        WITH RECURSIVE cnt(id) AS (
            SELECT 1
            UNION ALL
            SELECT id + 1 FROM cnt WHERE id < 5000
        )
        SELECT id FROM cnt
    );
    """


def get_analytics_schema() -> str:
    """
    Get analytics database schema (denormalized for performance issues).
    """
    return """
    CREATE TABLE IF NOT EXISTS events (
        event_id INTEGER PRIMARY KEY,
        user_id INTEGER,
        event_type VARCHAR(50),
        page_url VARCHAR(500),
        session_id VARCHAR(100),
        timestamp TIMESTAMP,
        user_agent VARCHAR(500),
        ip_address VARCHAR(50),
        country VARCHAR(100),
        city VARCHAR(100),
        device_type VARCHAR(50),
        browser VARCHAR(50)
    );

    CREATE TABLE IF NOT EXISTS page_views (
        page_view_id INTEGER PRIMARY KEY,
        user_id INTEGER,
        page_url VARCHAR(500),
        referrer VARCHAR(500),
        timestamp TIMESTAMP,
        time_on_page INTEGER
    );
    """


def get_analytics_seed_data() -> str:
    """
    Get analytics seed data (10000 events, 8000 page views).
    """
    return """
    -- Insert 10000 events
    INSERT INTO events (event_id, user_id, event_type, page_url, session_id, timestamp, user_agent, ip_address, country, city, device_type, browser)
    SELECT 
        id,
        ((id - 1) % 1000) + 1,
        CASE (id % 5)
            WHEN 0 THEN 'page_view'
            WHEN 1 THEN 'click'
            WHEN 2 THEN 'form_submit'
            WHEN 3 THEN 'purchase'
            ELSE 'logout'
        END,
        '/page/' || ((id % 50) + 1),
        'session_' || ((id % 2000) + 1),
        datetime('2023-01-01', '+' || (id % 365) || ' days', '+' || (id % 24) || ' hours'),
        'Mozilla/5.0',
        '192.168.' || ((id % 255) + 1) || '.' || ((id % 255) + 1),
        CASE (id % 5)
            WHEN 0 THEN 'USA'
            WHEN 1 THEN 'UK'
            WHEN 2 THEN 'Canada'
            WHEN 3 THEN 'Germany'
            ELSE 'France'
        END,
        'City' || ((id % 100) + 1),
        CASE (id % 3)
            WHEN 0 THEN 'desktop'
            WHEN 1 THEN 'mobile'
            ELSE 'tablet'
        END,
        CASE (id % 4)
            WHEN 0 THEN 'Chrome'
            WHEN 1 THEN 'Firefox'
            WHEN 2 THEN 'Safari'
            ELSE 'Edge'
        END
    FROM (
        WITH RECURSIVE cnt(id) AS (
            SELECT 1
            UNION ALL
            SELECT id + 1 FROM cnt WHERE id < 10000
        )
        SELECT id FROM cnt
    );

    -- Insert 8000 page views
    INSERT INTO page_views (page_view_id, user_id, page_url, referrer, timestamp, time_on_page)
    SELECT 
        id,
        ((id - 1) % 1000) + 1,
        '/page/' || ((id % 50) + 1),
        CASE (id % 3)
            WHEN 0 THEN 'https://google.com'
            WHEN 1 THEN 'https://facebook.com'
            ELSE NULL
        END,
        datetime('2023-01-01', '+' || (id % 365) || ' days', '+' || (id % 24) || ' hours'),
        (id % 300) + 10
    FROM (
        WITH RECURSIVE cnt(id) AS (
            SELECT 1
            UNION ALL
            SELECT id + 1 FROM cnt WHERE id < 8000
        )
        SELECT id FROM cnt
    );
    """


def get_task_schema_and_data(task_type: str) -> tuple[str, str]:
    """
    Get schema and seed data for a specific task type.
    
    Args:
        task_type: Task type (index-advisor, query-rewriter, schema-normalizer)
        
    Returns:
        Tuple of (schema_sql, seed_data_sql)
    """
    if task_type == "index-advisor":
        return get_ecommerce_schema(), get_ecommerce_seed_data()
    elif task_type == "query-rewriter":
        return get_ecommerce_schema(), get_ecommerce_seed_data()
    elif task_type == "schema-normalizer":
        return get_analytics_schema(), get_analytics_seed_data()
    else:
        raise ValueError(f"Unknown task type: {task_type}")
