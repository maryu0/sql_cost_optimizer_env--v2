"""
Pydantic models for SQL Cost Optimizer Environment.
Defines the observation space, action space, and reward structure.
"""
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field, field_validator, ConfigDict


class Observation(BaseModel):
    """
    Environment observation returned after reset() or step().
    Contains all information the agent needs to make optimization decisions.
    """
    model_config = ConfigDict(protected_namespaces=())
    task_type: str = Field(
        description="Type of optimization task",
        pattern="^(index-advisor|query-rewriter|schema-normalizer)$"
    )
    query: str = Field(
        description="The original SQL query to optimize"
    )
    database_schema: str = Field(
        description="Database schema as CREATE TABLE statements"
    )
    current_execution_time_ms: float = Field(
        description="Baseline execution time in milliseconds",
        ge=0.0
    )
    explain_plan: str = Field(
        description="EXPLAIN QUERY PLAN output from SQLite"
    )
    sample_data_preview: str = Field(
        description="Sample rows from relevant tables (up to 5 rows)"
    )
    hint: str = Field(
        default="",
        description="Optional hint about optimization opportunity"
    )
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="Additional context (row counts, existing indexes, etc.)"
    )


class Action(BaseModel):
    """
    Agent's optimization action.
    Can contain SQL DDL (CREATE INDEX), optimized query, or schema changes.
    """
    optimized_query: str = Field(
        description="The optimized SQL statement or DDL"
    )
    explanation: str = Field(
        description="Human-readable explanation of the optimization strategy"
    )
    suggested_changes: List[str] = Field(
        default_factory=list,
        description="List of specific changes made (e.g., 'Added index on users.email')"
    )
    confidence: float = Field(
        default=0.5,
        ge=0.0,
        le=1.0,
        description="Agent's confidence in this optimization (0.0 to 1.0)"
    )
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="Additional action metadata"
    )

    @field_validator('optimized_query')
    @classmethod
    def validate_query_not_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("optimized_query cannot be empty")
        return v.strip()


class RewardBreakdown(BaseModel):
    """
    Detailed breakdown of reward components.
    """
    grade_score: float = Field(
        description="Grader score (0.0 to 1.0)",
        ge=0.0,
        le=1.0
    )
    performance_improvement: float = Field(
        description="Performance improvement factor (e.g., 2.0 = 2x faster)",
        ge=0.0
    )
    cost_savings_bonus: float = Field(
        description="Cost savings bonus (0.0 to 0.2)",
        ge=0.0,
        le=0.2
    )
    correctness_penalty: float = Field(
        description="Penalty for incorrect results (0.0 or -0.5)",
        ge=-0.5,
        le=0.0
    )
    safety_bonus: float = Field(
        default=0.0,
        description="Bonus for safe operations (0.0 to 0.1)",
        ge=0.0,
        le=0.1
    )


class Reward(BaseModel):
    """
    Reward signal returned after step().
    Range: -1.0 to 1.0 with detailed breakdown.
    """
    score: float = Field(
        description="Total reward score",
        ge=-1.0,
        le=1.0
    )
    breakdown: RewardBreakdown = Field(
        description="Detailed breakdown of reward components"
    )
    feedback: str = Field(
        description="Human-readable feedback explaining the score"
    )
    done: bool = Field(
        description="Whether the episode is complete"
    )


class EnvironmentState(BaseModel):
    """
    Internal environment state for debugging via state() endpoint.
    """
    current_task: Optional[str] = Field(
        default=None,
        description="Current task being executed"
    )
    episode_step: int = Field(
        default=0,
        description="Current step in the episode"
    )
    cumulative_reward: float = Field(
        default=0.0,
        description="Total reward accumulated in this episode"
    )
    is_done: bool = Field(
        default=False,
        description="Whether the episode is finished"
    )
    database_state: Dict[str, Any] = Field(
        default_factory=dict,
        description="Current database state (tables, indexes, etc.)"
    )
    action_history: List[str] = Field(
        default_factory=list,
        description="History of actions taken"
    )


class TaskConfig(BaseModel):
    """
    Configuration for a specific task.
    """
    name: str = Field(
        description="Task name (index-advisor, query-rewriter, schema-normalizer)"
    )
    difficulty: str = Field(
        description="Task difficulty (easy, medium, hard)"
    )
    initial_query: str = Field(
        description="The inefficient query to optimize"
    )
    schema_sql: str = Field(
        description="Database schema DDL"
    )
    seed_data_sql: str = Field(
        description="INSERT statements for sample data"
    )
    success_criteria: Dict[str, Any] = Field(
        description="Criteria for task success (speedup, correctness, etc.)"
    )
    hint: str = Field(
        default="",
        description="Optional hint for the agent"
    )
