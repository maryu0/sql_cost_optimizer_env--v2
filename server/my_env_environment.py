# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"""OpenEnv wrapper for the migrated SQL Cost Optimizer environment."""

from typing import Any, Dict, Optional
from uuid import uuid4

from openenv.core.env_server.interfaces import Environment
from openenv.core.env_server.types import State

try:
    from ..models import MyAction, MyObservation
except ImportError:
    from models import MyAction, MyObservation

from src.environment import SQLOptimizerEnv
from src.models import Action as SQLEnvAction


def _strict_score(value: float) -> float:
    """Clamp score into strict validator-safe range."""
    return max(0.01, min(0.99, float(value)))


class MyEnvironment(Environment):
    """Environment adapter that exposes SQLOptimizerEnv via OpenEnv server types."""

    SUPPORTS_CONCURRENT_SESSIONS: bool = True

    def __init__(self):
        self._state = State(episode_id=str(uuid4()), step_count=0)
        self._env = SQLOptimizerEnv()

    def reset(self, task_name: Optional[str] = None, seed: Optional[int] = None) -> MyObservation:
        """Reset with optional task and seed passthrough."""
        self._state = State(episode_id=str(uuid4()), step_count=0)
        obs = self._env.reset(task_name=task_name, seed=seed)
        return MyObservation(
            task_type=obs.task_type,
            query=obs.query,
            database_schema=obs.database_schema,
            current_execution_time_ms=obs.current_execution_time_ms,
            explain_plan=obs.explain_plan,
            sample_data_preview=obs.sample_data_preview,
            hint=obs.hint,
            done=False,
            reward=0.01,
            metadata=obs.metadata,
        )

    def step(self, action: MyAction) -> MyObservation:  # type: ignore[override]
        """Run one optimization step and convert output to OpenEnv observation schema."""
        self._state.step_count += 1

        try:
            env_action = SQLEnvAction(
                optimized_query=action.optimized_query,
                explanation=action.explanation,
                suggested_changes=action.suggested_changes,
                confidence=action.confidence,
                metadata=action.metadata,
            )

            obs, reward, done, info = self._env.step(env_action)
            combined_metadata: Dict[str, Any] = dict(obs.metadata)
            combined_metadata.update(
                {
                    "grade_score": _strict_score(float(info.get("grade_score", 0.01))),
                    "grade_feedback": str(info.get("grade_feedback", "")),
                    "cost_report": info.get("cost_report", {}),
                    "episode_step": self._state.step_count,
                }
            )

            return MyObservation(
                task_type=obs.task_type,
                query=obs.query,
                database_schema=obs.database_schema,
                current_execution_time_ms=obs.current_execution_time_ms,
                explain_plan=obs.explain_plan,
                sample_data_preview=obs.sample_data_preview,
                hint=str(info.get("grade_feedback", obs.hint)),
                done=bool(done),
                reward=_strict_score(float(reward.score)),
                metadata=combined_metadata,
            )
        except Exception as exc:
            return MyObservation(
                task_type=self._env.current_task or "index-advisor",
                query=self._env.current_task_config["initial_query"] if self._env.current_task_config else "",
                database_schema=self._env.current_task_config.get("schema_sql", "") if self._env.current_task_config else "",
                current_execution_time_ms=max(0.0, float(self._env.baseline_time_ms)),
                explain_plan=f"Error during step: {exc}",
                sample_data_preview="",
                hint=f"Execution error: {exc}",
                done=True,
                reward=0.01,
                metadata={"error": str(exc), "episode_step": self._state.step_count},
            )

    @property
    def state(self) -> State:
        return self._state

    def close(self):
        self._env.close()
