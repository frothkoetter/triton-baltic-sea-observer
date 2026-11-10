"""Public entry — re-exports pipeline API."""

from src.pipeline import PipelineResult, execute_sql_insert, run_pipeline

__all__ = ["PipelineResult", "execute_sql_insert", "run_pipeline"]
