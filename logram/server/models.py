from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class ProjectSummary(BaseModel):
    name: str
    document_count: int
    run_count: int


class InputSummary(BaseModel):
    project: str | None = None
    input_id: str
    run_count: int
    last_run_timestamp: float | None


class StepMeta(BaseModel):
    id: str
    name: str
    status: str | None = None
    duration: float = 0.0
    parent_id: str | None = None
    logic_hash: str | None = None


class RunDetail(BaseModel):
    run_id: str
    project: str | None = None
    input_id: str | None = None
    version_id: str | None = None
    status: str | None = None
    tags: list[str] = Field(default_factory=list)
    metrics: dict[str, Any] = Field(default_factory=dict)
    resource_time_saved: float = 0.0
    wait_time_saved: float = 0.0
    roi_ratio: float = 0.0
    total_prompt_tokens: int = 0
    total_completion_tokens: int = 0
    created_at: float | None = None
    updated_at: float | None = None


class StepNode(BaseModel):
    id: str
    run_id: str
    parent_step_id: str | None = None
    name: str
    status: str | None = None
    duration: float = 0.0
    logic_hash: str | None = None
    timestamp: float | None = None


class StepGroup(BaseModel):
    id: str
    run_id: str
    parent_step_id: str | None = None
    name: str
    status: str | None = None
    duration: float = 0.0
    logic_hash: str | None = None
    timestamp: float | None = None
    step_ids: list[str] = Field(default_factory=list)
    count: int = 0
    status_counts: dict[str, int] = Field(default_factory=dict)


class StepDetail(BaseModel):
    step_id: str
    run_id: str
    parent_step_id: str | None = None
    name: str
    status: str | None = None
    duration: float = 0.0
    timestamp: float | None = None
    started_at: float | None = None
    finished_at: float | None = None
    logic_hash: str | None = None
    prompt_tokens: int = 0
    completion_tokens: int = 0
    inputs: Any = None
    output: Any = None
    error: Any = None
    source_code: str | None = None
    resolved_globals: Any = None


class GraphResponse(BaseModel):
    run_id: str
    steps: list[StepNode]
    groups: list[StepGroup] = Field(default_factory=list)


class CompareChange(BaseModel):
    step_name: str
    run_id_a_step_id: str | None = None
    run_id_b_step_id: str | None = None
    logic_hash_a: str | None = None
    logic_hash_b: str | None = None


class CompareResponse(BaseModel):
    run_id_a: str
    run_id_b: str
    identical_steps: list[str]
    logic_hash_changed: list[CompareChange]
    output_changed: list[CompareChange]
    only_in_a: list[str]
    only_in_b: list[str]


class DiffRecursiveFunction(BaseModel):
    qualname: str
    logic_hash: str
    source_code: str | None = None
    resolved_globals: Any = None


class DiffLogicSnapshot(BaseModel):
    logic_hash: str | None = None
    source_code: str | None = None
    resolved_globals: Any = None
    called_functions: dict[str, str] = Field(default_factory=dict)
    recursive_functions: list[DiffRecursiveFunction] = Field(default_factory=list)


class DiffStepSide(BaseModel):
    step_id: str
    parent_step_id: str | None = None
    name: str
    status: str | None = None
    duration: float = 0.0
    logic_hash: str | None = None
    inputs: Any = None
    output: Any = None
    error: Any = None
    logic: DiffLogicSnapshot | None = None


class DiffStepRow(BaseModel):
    name: str
    a: DiffStepSide | None = None
    b: DiffStepSide | None = None
    identical: bool = False
    output_changed: bool = False
    output_similarity: float | None = None


class RunDiffSummary(BaseModel):
    prompts_modified: int = 0
    bboxes_added: int = 0
    cost_delta_percent: float | None = None
    changed_steps: int = 0
    similarity_score: float | None = None


class RunDiffResponse(BaseModel):
    run_id_a: str
    run_id_b: str
    steps: list[DiffStepRow]
    summary: RunDiffSummary = Field(default_factory=RunDiffSummary)


class RunLineageNode(BaseModel):
    run_id: str
    project: str | None = None
    input_id: str | None = None
    version_id: str | None = None
    status: str | None = None
    created_at: float | None = None
    replay_ratio: float = 0.0


class RunLineageEdge(BaseModel):
    source_run_id: str
    target_run_id: str
    edge_type: str = "linear"  # linear | fork | version-link


class RunLineageResponse(BaseModel):
    focus_run_id: str
    nodes: list[RunLineageNode]
    edges: list[RunLineageEdge]


class StatsResponse(BaseModel):
    project: str | None = None
    runs: int
    total_prompt_tokens: int
    total_completion_tokens: int
    total_tokens: int
    total_usd_spent: float
    wait_time_saved: float
    resource_time_saved: float
    roi_wait_vs_resource: float
