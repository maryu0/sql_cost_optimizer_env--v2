# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""Client for SQL Cost Optimizer OpenEnv environment."""

from typing import Dict

from openenv.core import EnvClient
from openenv.core.client_types import StepResult
from openenv.core.env_server.types import State

from .models import MyAction, MyObservation


class MyEnv(
    EnvClient[MyAction, MyObservation, State]
):
    """
    Client for the SQL Cost Optimizer environment.

    This client maintains a persistent WebSocket connection to the environment server,
    enabling efficient multi-step interactions with lower latency.
    Each client instance has its own dedicated environment session on the server.

    Example:
        >>> # Connect to a running server
        >>> with MyEnv(base_url="http://localhost:8000") as client:
        ...     result = client.reset()
        ...     print(result.observation.echoed_message)
        ...
        ...     result = client.step(MyAction(message="Hello!"))
        ...     print(result.observation.echoed_message)

    Example with Docker:
        >>> # Automatically start container and connect
        >>> client = MyEnv.from_docker_image("my_env-env:latest")
        >>> try:
        ...     result = client.reset()
        ...     result = client.step(MyAction(message="Test"))
        ... finally:
        ...     client.close()
    """

    def _step_payload(self, action: MyAction) -> Dict:
        """
        Convert MyAction to JSON payload for step message.

        Args:
            action: MyAction instance

        Returns:
            Dictionary representation suitable for JSON encoding
        """
        return {
            "optimized_query": action.optimized_query,
            "explanation": action.explanation,
            "suggested_changes": action.suggested_changes,
            "confidence": action.confidence,
            "metadata": action.metadata,
        }

    def _parse_result(self, payload: Dict) -> StepResult[MyObservation]:
        """
        Parse server response into StepResult[MyObservation].

        Args:
            payload: JSON response data from server

        Returns:
            StepResult with MyObservation
        """
        obs_data = payload.get("observation", {})
        observation = MyObservation(
            task_type=obs_data.get("task_type", "index-advisor"),
            query=obs_data.get("query", ""),
            database_schema=obs_data.get("database_schema", ""),
            current_execution_time_ms=obs_data.get("current_execution_time_ms", 0.0),
            explain_plan=obs_data.get("explain_plan", ""),
            sample_data_preview=obs_data.get("sample_data_preview", ""),
            hint=obs_data.get("hint", ""),
            done=payload.get("done", False),
            reward=payload.get("reward"),
            metadata=obs_data.get("metadata", {}),
        )

        return StepResult(
            observation=observation,
            reward=payload.get("reward"),
            done=payload.get("done", False),
        )

    def _parse_state(self, payload: Dict) -> State:
        """
        Parse server response into State object.

        Args:
            payload: JSON response from state request

        Returns:
            State object with episode_id and step_count
        """
        return State(
            episode_id=payload.get("episode_id"),
            step_count=payload.get("step_count", 0),
        )
