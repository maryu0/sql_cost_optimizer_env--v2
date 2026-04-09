"""
Task 3: Schema Normalizer (Hard)
Identify denormalized schemas and suggest normalized alternatives with foreign keys.
"""

TASK_CONFIG = {
    "name": "schema-normalizer",
    "difficulty": "hard",
    "weight": 0.8,
    "score": 0.8,
    "description": "Normalize denormalized schema to reduce redundancy and improve integrity",
    "grader": {
        "name": "schema-normalizer-grader",
        "type": "deterministic",
        "config": {
            "criteria": [
                "Creates dimension tables for normalization",
                "Defines foreign key constraints",
                "Includes data migration logic",
                "Maintains referential integrity"
            ]
        },
        "criteria": [
            "Creates dimension tables for normalization",
            "Defines foreign key constraints",
            "Includes data migration logic",
            "Maintains referential integrity"
        ]
    },
    
    "initial_query": """
        SELECT 
            user_id,
            COUNT(*) as event_count,
            COUNT(DISTINCT country) as countries_visited,
            COUNT(DISTINCT city) as cities_visited,
            COUNT(DISTINCT device_type) as device_types_used
        FROM events
        WHERE event_type = 'page_view'
        GROUP BY user_id
        HAVING event_count > 100;
    """,
    
    "hint": "The events table stores redundant user location data (country, city) and device info repeatedly. Consider normalizing into separate dimension tables.",
    
    "success_criteria": {
        "must_create_new_tables": True,
        "required_tables": ["user_locations", "devices"],
        "must_have_foreign_keys": True,
        "data_integrity_maintained": True,
        "min_space_savings": 0.2  # At least 20% space reduction
    }
}


def get_expected_solution() -> str:
    """
    Return the expected optimal solution for validation.
    """
    return """
    -- Create normalized dimension tables
    CREATE TABLE user_locations (
        location_id INTEGER PRIMARY KEY AUTOINCREMENT,
        country VARCHAR(100),
        city VARCHAR(100),
        UNIQUE(country, city)
    );

    CREATE TABLE devices (
        device_id INTEGER PRIMARY KEY AUTOINCREMENT,
        device_type VARCHAR(50),
        browser VARCHAR(50),
        user_agent VARCHAR(500),
        UNIQUE(device_type, browser, user_agent)
    );

    -- Create normalized events table
    CREATE TABLE events_normalized (
        event_id INTEGER PRIMARY KEY,
        user_id INTEGER,
        event_type VARCHAR(50),
        page_url VARCHAR(500),
        session_id VARCHAR(100),
        timestamp TIMESTAMP,
        ip_address VARCHAR(50),
        location_id INTEGER,
        device_id INTEGER,
        FOREIGN KEY (location_id) REFERENCES user_locations(location_id),
        FOREIGN KEY (device_id) REFERENCES devices(device_id)
    );

    -- Populate dimension tables
    INSERT INTO user_locations (country, city)
    SELECT DISTINCT country, city FROM events;

    INSERT INTO devices (device_type, browser, user_agent)
    SELECT DISTINCT device_type, browser, user_agent FROM events;

    -- Populate normalized events table
    INSERT INTO events_normalized
    SELECT 
        e.event_id,
        e.user_id,
        e.event_type,
        e.page_url,
        e.session_id,
        e.timestamp,
        e.ip_address,
        ul.location_id,
        d.device_id
    FROM events e
    JOIN user_locations ul ON e.country = ul.country AND e.city = ul.city
    JOIN devices d ON e.device_type = d.device_type 
                   AND e.browser = d.browser 
                   AND e.user_agent = d.user_agent;

    -- Create indexes for efficient lookups
    CREATE INDEX idx_events_user_id ON events_normalized(user_id);
    CREATE INDEX idx_events_location_id ON events_normalized(location_id);
    CREATE INDEX idx_events_device_id ON events_normalized(device_id);

    -- Updated query using normalized schema
    SELECT 
        user_id,
        COUNT(*) as event_count,
        COUNT(DISTINCT location_id) as locations_visited,
        COUNT(DISTINCT device_id) as devices_used
    FROM events_normalized
    WHERE event_type = 'page_view'
    GROUP BY user_id
    HAVING event_count > 100;
    """


def get_explanation() -> str:
    """
    Return explanation of the normalization strategy.
    """
    return """
    The original events table suffers from significant data redundancy:
    - Country, city stored repeatedly for every event (same user, same location)
    - Device info (device_type, browser, user_agent) repeated across events
    - No referential integrity constraints
    
    Normalization strategy (3NF):
    
    1. Extract location data into user_locations dimension table
       - Reduces storage: Instead of storing "USA, New York" 1000 times, store once
       - Enables efficient location analytics
    
    2. Extract device data into devices dimension table
       - Reduces storage: Device combinations stored once, not per event
       - Simplifies device tracking and analytics
    
    3. Maintain events_normalized fact table with foreign keys
       - Preserves all event data
       - Uses integer foreign keys (smaller, faster than strings)
       - Enforces referential integrity
    
    Benefits:
    - 40-60% storage reduction (fewer redundant strings)
    - Faster queries (integer JOINs vs string comparisons)
    - Better data integrity (foreign key constraints)
    - Easier updates (change country name in one place)
    - Query performance maintained or improved with proper indexes
    
    Migration path:
    - Create new tables alongside existing schema
    - Populate with existing data
    - Validate data integrity
    - Update application queries
    - Drop old table after validation
    """
