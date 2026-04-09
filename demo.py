"""Simple demo script for the SQL Cost Optimizer environment."""

from src.environment import SQLOptimizerEnv
from src.models import Action


def main() -> None:
    env = SQLOptimizerEnv()
    try:
        observation = env.reset(task_name="index-advisor", seed=42)
        print("Task:", observation.task_type)
        print("Query:", observation.query)

        action = Action(
            optimized_query="CREATE INDEX idx_orders_status ON orders(status);",
            explanation="Demo index suggestion",
            suggested_changes=["Add index on orders.status"],
            confidence=0.8,
        )
        next_observation, reward, done, info = env.step(action)
        print("Reward:", reward.score)
        print("Done:", done)
        print("Grade:", info.get("grade_score"))
        print("Next hint:", next_observation.hint)
    finally:
        env.close()


if __name__ == "__main__":
    main()
