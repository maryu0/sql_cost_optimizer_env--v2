---
title: SQL Cost Optimizer Env v2
emoji: 🚀
colorFrom: blue
colorTo: green
sdk: docker
pinned: false
app_port: 8000
tags:
  - openenv
  - sql
  - fastapi
---

# SQL Cost Optimizer Environment

This repository provides an OpenEnv-compatible SQL optimization benchmark with three tasks:
- `index-advisor`
- `query-rewriter`
- `schema-normalizer`

The environment simulates a real task people actually do: improving SQL performance and schema quality while preserving correctness.

## Environment Description

Agents receive a slow SQL query, schema, sample data, and an EXPLAIN plan. They must propose an optimization action such as an index, rewritten SQL, or normalized schema migration.

## Action Space

`MyAction` includes:
- `optimized_query`: SQL or DDL to execute
- `explanation`: short rationale
- `suggested_changes`: list of changes made
- `confidence`: float in `[0.0, 1.0]`
- `metadata`: extra fields

## Observation Space

`MyObservation` includes:
- `task_type`
- `query`
- `database_schema`
- `current_execution_time_ms`
- `explain_plan`
- `sample_data_preview`
- `hint`
- `metadata`

## Setup

Install dependencies and run validation:

```bash
uv sync
openenv validate
python inference.py
```

## Required Environment Variables

Set these in your Hugging Face Space or local environment:
- `API_BASE_URL`
- `MODEL_NAME`
- `HF_TOKEN`

## Demo Script

Run the demo script to exercise one environment step:

```bash
python demo.py
```

## Docker and Deployment

Build locally:

```bash
docker build -f Dockerfile -t sql-cost-optimizer-env-v2 .
```

Deploy to Hugging Face Spaces by pushing the repo root with `Dockerfile`, `openenv.yaml`, `inference.py`, `requirements.txt`, and `README.md` present.

## Project Structure

```
my_env/
├── .dockerignore         # Docker build exclusions
├── __init__.py            # Module exports
├── README.md              # This file
├── openenv.yaml           # OpenEnv manifest
├── pyproject.toml         # Project metadata and dependencies
├── uv.lock                # Locked dependencies (generated)
├── client.py              # MyEnv client
├── models.py              # Action and Observation models
└── server/
    ├── __init__.py        # Server module exports
    ├── my_env_environment.py  # Core environment logic
    ├── app.py             # FastAPI application (HTTP + WebSocket endpoints)
    └── Dockerfile         # Container image definition
```
