"""
OpenEnv-compliant SQL Cost Optimizer Environment.
Implements reset(), step(), state(), and close() methods.
"""
import random
from typing import Optional, Tuple, Dict, Any
from src.models import Observation, Action, Reward, EnvironmentState, TaskConfig
from src.utils.db_executor import DatabaseExecutor
from src.utils.cost_calculator import CostCalculator
from src.utils.seed_data import get_task_schema_and_data
from src.rewards import RewardCalculator
from src.graders import get_grader
from src.tasks import task1_index_advisor, task2_query_rewriter, task3_schema_normalizer


class SQLOptimizerEnv:
    """
    OpenEnv environment for SQL query optimization.
    
    Agents learn to optimize SQL queries through:
    - Index suggestions (easy)
    - Query rewriting (medium)
    - Schema normalization (hard)
    """

    def __init__(self):
        """Initialize the environment."""
        self.db: Optional[DatabaseExecutor] = None
        self.cost_calculator = CostCalculator()
        self.reward_calculator = RewardCalculator()
        
        self.current_task: Optional[str] = None
        self.current_task_config: Optional[Dict[str, Any]] = None
        self.episode_step: int = 0
        self.cumulative_reward: float = 0.0
        self.is_done: bool = False
        
        self.baseline_time_ms: float = 0.0
        self.baseline_results: list = []
        self.baseline_cost: Dict[str, Any] = {}
        
        self.action_history: list = []

        # Task configurations
        self.tasks = {
            "index-advisor": task1_index_advisor.TASK_CONFIG,
            "query-rewriter": task2_query_rewriter.TASK_CONFIG,
            "schema-normalizer": task3_schema_normalizer.TASK_CONFIG
        }

    def reset(self, task_name: Optional[str] = None, seed: Optional[int] = None) -> Observation:
        """
        Reset environment to initial state.
        
        Args:
            task_name: Specific task to load (or random if None)
            seed: Random seed for reproducibility
            
        Returns:
            Initial observation
        """
        # Set random seed if provided
        if seed is not None:
            random.seed(seed)

        # Select task
        if task_name is None:
            task_name = random.choice(list(self.tasks.keys()))
        elif task_name not in self.tasks:
            raise ValueError(f"Unknown task: {task_name}. Available: {list(self.tasks.keys())}")

        self.current_task = task_name
        self.current_task_config = self.tasks[task_name]
        
        # Reset episode state
        self.episode_step = 0
        self.cumulative_reward = 0.0
        self.is_done = False
        self.action_history = []

        # Initialize database
        if self.db:
            self.db.close()
        self.db = DatabaseExecutor()

        # Load schema and seed data
        schema_sql, seed_sql = get_task_schema_and_data(task_name)
        self.db.execute_schema(schema_sql)
        self.db.execute_seed_data(seed_sql)

        # Execute baseline query and measure performance
        initial_query = self.current_task_config["initial_query"].strip()
        self.baseline_results, self.baseline_time_ms = self.db.execute_query_timed(initial_query)

        # Get EXPLAIN plan
        explain_plan = self.db.get_explain_plan(initial_query)

        # Get table info
        table_info = self.db.get_table_info()

        # Get sample data preview
        sample_data = self._get_sample_data_preview(table_info)

        # Calculate baseline cost
        total_rows = sum(info["row_count"] for info in table_info.values())
        self.baseline_cost = self.cost_calculator.estimate_query_cost(
            execution_time_ms=self.baseline_time_ms,
            rows_scanned=total_rows,
            rows_returned=len(self.baseline_results),
            table_size_mb=total_rows * 0.001,  # Rough estimate
            has_index=False
        )

        # Create initial observation
        observation = Observation(
            task_type=task_name,
            query=initial_query,
            database_schema=schema_sql,
            current_execution_time_ms=self.baseline_time_ms,
            explain_plan=explain_plan,
            sample_data_preview=sample_data,
            hint=self.current_task_config.get("hint", ""),
            metadata={
                "table_info": table_info,
                "baseline_cost_usd": self.baseline_cost["total_cost_usd"],
                "result_row_count": len(self.baseline_results),
                "episode_step": self.episode_step
            }
        )

        return observation

    def step(self, action: Action) -> Tuple[Observation, Reward, bool, Dict[str, Any]]:
        """
        Execute action and return next observation, reward, done status, and info.
        
        Args:
            action: Agent's optimization action
            
        Returns:
            Tuple of (observation, reward, done, info)
        """
        self.episode_step += 1
        self.action_history.append(action.optimized_query)

        # Execute optimized query/DDL
        optimized_results = []
        optimized_time_ms = 0.0
        has_errors = False
        error_message = ""

        try:
            # For index-advisor: Execute DDL then re-run original query
            if self.current_task == "index-advisor":
                # Execute CREATE INDEX statements
                self.db.cursor.executescript(action.optimized_query)
                self.db.conn.commit()
                
                # Re-run original query to measure improvement
                original_query = self.current_task_config["initial_query"].strip()
                optimized_results, optimized_time_ms = self.db.execute_query_timed(original_query)
            
            # For query-rewriter: Execute new query directly
            elif self.current_task == "query-rewriter":
                optimized_results, optimized_time_ms = self.db.execute_query_timed(
                    action.optimized_query
                )
            
            # For schema-normalizer: Execute normalization script
            elif self.current_task == "schema-normalizer":
                self.db.cursor.executescript(action.optimized_query)
                self.db.conn.commit()
                
                # Try to execute a test query on normalized schema
                # For now, just use execution time of script
                optimized_time_ms = self.baseline_time_ms * 0.8  # Assume improvement
                optimized_results = self.baseline_results  # Assume same results

        except Exception as e:
            has_errors = True
            error_message = str(e)
            optimized_time_ms = self.baseline_time_ms * 2  # Penalty
            optimized_results = []

        # Check results equivalence
        results_match = self.db.check_results_equivalent(
            self.baseline_results,
            optimized_results
        ) if not has_errors else False

        # Calculate optimized cost
        table_info = self.db.get_table_info()
        total_rows = sum(info["row_count"] for info in table_info.values())
        has_index = len([idx for info in table_info.values() for idx in info.get("indexes", [])]) > 0
        
        optimized_cost = self.cost_calculator.estimate_query_cost(
            execution_time_ms=optimized_time_ms,
            rows_scanned=total_rows // 2 if has_index else total_rows,
            rows_returned=len(optimized_results),
            table_size_mb=total_rows * 0.001,
            has_index=has_index
        )

        # Grade the action
        grader = get_grader(self.current_task)
        
        if self.current_task == "index-advisor":
            grade_score, grade_feedback = grader.grade(
                action.optimized_query,
                self.db,
                self.baseline_time_ms,
                optimized_time_ms
            )
        elif self.current_task == "query-rewriter":
            grade_score, grade_feedback = grader.grade(
                action.optimized_query,
                self.baseline_results,
                optimized_results,
                self.baseline_time_ms,
                optimized_time_ms
            )
        elif self.current_task == "schema-normalizer":
            grade_score, grade_feedback = grader.grade(
                action.optimized_query,
                self.db
            )
        else:
            grade_score = 0.01
            grade_feedback = "Unknown task type"

        # Calculate reward
        reward = self.reward_calculator.calculate_reward(
            grade_score=grade_score,
            baseline_time_ms=self.baseline_time_ms,
            optimized_time_ms=optimized_time_ms,
            baseline_cost_usd=self.baseline_cost["total_cost_usd"],
            optimized_cost_usd=optimized_cost["total_cost_usd"],
            results_match=results_match,
            has_errors=has_errors
        )

        self.cumulative_reward += reward.score
        self.is_done = reward.done or self.episode_step >= 5  # Max 5 attempts

        # Create next observation
        explain_plan = self.db.get_explain_plan(
            self.current_task_config["initial_query"].strip()
        ) if not has_errors else "Error: " + error_message

        sample_data = self._get_sample_data_preview(table_info)

        observation = Observation(
            task_type=self.current_task,
            query=self.current_task_config["initial_query"].strip(),
            database_schema=self.current_task_config.get("schema_sql", ""),
            current_execution_time_ms=optimized_time_ms,
            explain_plan=explain_plan,
            sample_data_preview=sample_data,
            hint=grade_feedback,
            metadata={
                "table_info": table_info,
                "baseline_cost_usd": self.baseline_cost["total_cost_usd"],
                "optimized_cost_usd": optimized_cost["total_cost_usd"],
                "speedup_factor": self.baseline_time_ms / optimized_time_ms if optimized_time_ms > 0 else 1.0,
                "results_match": results_match,
                "has_errors": has_errors,
                "error_message": error_message,
                "episode_step": self.episode_step
            }
        )

        # Info dict
        info = {
            "grade_score": grade_score,
            "grade_feedback": grade_feedback,
            "baseline_time_ms": self.baseline_time_ms,
            "optimized_time_ms": optimized_time_ms,
            "cost_report": self.cost_calculator.generate_cost_report(
                self.baseline_cost,
                optimized_cost,
                self.baseline_time_ms / optimized_time_ms if optimized_time_ms > 0 else 1.0
            )
        }

        return observation, reward, self.is_done, info

    def state(self) -> Dict[str, Any]:
        """
        Return current environment state for debugging.
        
        Returns:
            Dictionary with current state
        """
        return {
            "current_task": self.current_task,
            "episode_step": self.episode_step,
            "cumulative_reward": self.cumulative_reward,
            "is_done": self.is_done,
            "database_state": self.db.get_table_info() if self.db else {},
            "action_history": self.action_history,
            "baseline_time_ms": self.baseline_time_ms
        }

    def close(self):
        """Clean up resources."""
        if self.db:
            self.db.close()
            self.db = None

    def _get_sample_data_preview(self, table_info: Dict[str, Any]) -> str:
        """Get sample data preview from relevant tables."""
        previews = []
        for table_name in list(table_info.keys())[:3]:  # Limit to 3 tables
            sample = self.db.get_sample_data(table_name, limit=3)
            previews.append(f"{table_name}:\n{sample}")
        
        return "\n\n".join(previews)

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
