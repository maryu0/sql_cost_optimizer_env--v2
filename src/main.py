"""
FastAPI wrapper for SQL Cost Optimizer Environment.
Provides REST API endpoints for Hugging Face Spaces deployment.
"""
import sys
import os

# Force UTF-8 encoding on Windows
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

from fastapi import FastAPI, HTTPException, Body
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional, Dict, Any
from dotenv import load_dotenv

from src.environment import SQLOptimizerEnv
from src.models import Action, Observation, Reward

load_dotenv()

# Initialize FastAPI app
app = FastAPI(
    title="SQL Cost Optimizer Environment",
    description="OpenEnv environment for learning SQL query optimization",
    version="1.0.0"
)

# Global environment instance
env: Optional[SQLOptimizerEnv] = None


class ResetRequest(BaseModel):
    """Request model for /reset endpoint."""
    task_name: Optional[str] = None
    seed: Optional[int] = None


class StepRequest(BaseModel):
    """Request model for /step endpoint."""
    action: Action


@app.on_event("startup")
async def startup_event():
    """Initialize environment on startup."""
    global env
    env = SQLOptimizerEnv()
    print("✅ SQL Cost Optimizer Environment initialized")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown."""
    global env
    if env:
        env.close()
    print("👋 SQL Cost Optimizer Environment closed")


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "name": "SQL Cost Optimizer Environment",
        "version": "1.0.0",
        "status": "running",
        "tasks": ["index-advisor", "query-rewriter", "schema-normalizer"],
        "endpoints": ["/reset", "/step", "/state", "/health"]
    }


@app.get("/health")
async def health_check():
    """Health check endpoint for Hugging Face Spaces."""
    global env
    if env is None:
        raise HTTPException(status_code=503, detail="Environment not initialized")
    
    return {
        "status": "healthy",
        "environment": "ready"
    }


@app.post("/reset")
async def reset(request: Optional[ResetRequest] = Body(None)) -> Dict[str, Any]:
    """
    Reset the environment to initial state.

    Args:
        request: Reset request with optional task_name and seed

    Returns:
        Initial observation
    """
    global env
    if env is None:
        raise HTTPException(status_code=503, detail="Environment not initialized")

    try:
        task_name = request.task_name if request else None
        seed = request.seed if request else None
        observation = env.reset(task_name=task_name, seed=seed)
        return {
            "observation": observation.model_dump(),
            "info": {
                "task": env.current_task,
                "episode_step": env.episode_step
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Reset failed: {str(e)}")


@app.post("/step")
async def step(request: StepRequest) -> Dict[str, Any]:
    """
    Execute an action in the environment.
    
    Args:
        request: Step request with action
        
    Returns:
        Observation, reward, done status, and info
    """
    global env
    if env is None:
        raise HTTPException(status_code=503, detail="Environment not initialized")
    
    if env.current_task is None:
        raise HTTPException(
            status_code=400,
            detail="Environment not reset. Call /reset first."
        )
    
    try:
        observation, reward, done, info = env.step(request.action)
        return {
            "observation": observation.model_dump(),
            "reward": reward.model_dump(),
            "done": done,
            "info": info
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Step failed: {str(e)}")


@app.get("/state")
async def get_state() -> Dict[str, Any]:
    """
    Get current environment state for debugging.
    
    Returns:
        Current environment state
    """
    global env
    if env is None:
        raise HTTPException(status_code=503, detail="Environment not initialized")
    
    return env.state()


@app.get("/tasks")
async def list_tasks():
    """
    List all available tasks.
    
    Returns:
        List of task configurations
    """
    global env
    if env is None:
        raise HTTPException(status_code=503, detail="Environment not initialized")
    
    return {
        "tasks": [
            {
                "name": task_name,
                "difficulty": config["difficulty"],
                "description": config["description"],
                "weight": config.get("weight", 0.5),
                "score": config.get("weight", 0.5),
                "grader": {
                    **config.get("grader", {
                        "name": f"{task_name}-grader",
                        "config": {"criteria": []},
                    }),
                    "score": config.get("weight", 0.5)
                }
            }
            for task_name, config in env.tasks.items()
        ]
    }


@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler."""
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "detail": str(exc)
        }
    )


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
