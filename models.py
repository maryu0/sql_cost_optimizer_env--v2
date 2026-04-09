# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Data models for SQL Cost Optimizer OpenEnv environment."""

from typing import Any, Dict, List

from openenv.core.env_server.types import Action, Observation
from pydantic import ConfigDict, Field, field_validator


class MyAction(Action):
    """Action schema used by the SQL optimization environment."""

    optimized_query: str = Field(description="Optimized SQL query or DDL script")
    explanation: str = Field(description="Short rationale for the proposed optimization")
    suggested_changes: List[str] = Field(
        default_factory=list,
        description="Concrete changes introduced by the action",
    )
    confidence: float = Field(
        default=0.5,
        ge=0.0,
        le=1.0,
        description="Model confidence in this optimization",
    )
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Extra action metadata")

    @field_validator("optimized_query")
    @classmethod
    def validate_query_not_empty(cls, value: str) -> str:
        query = value.strip()
        if not query:
            raise ValueError("optimized_query cannot be empty")
        return query


class MyObservation(Observation):
    """Observation returned from the SQL optimization environment."""

    model_config = ConfigDict(protected_namespaces=())

    task_type: str = Field(
        description="Task category currently being evaluated",
        pattern="^(index-advisor|query-rewriter|schema-normalizer)$",
    )
    query: str = Field(description="Original SQL query to optimize")
    database_schema: str = Field(description="Database schema DDL")
    current_execution_time_ms: float = Field(ge=0.0, description="Execution time in milliseconds")
    explain_plan: str = Field(description="EXPLAIN QUERY PLAN output")
    sample_data_preview: str = Field(description="Sample rows from relevant tables")
    hint: str = Field(default="", description="Current hint or grader feedback")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional context")
