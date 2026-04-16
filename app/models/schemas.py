from typing import Any

from pydantic import BaseModel, Field


class RunInitResponse(BaseModel):
    run_id: str


class ExtractRequest(BaseModel):
    datasets: list[str] = Field(default_factory=list)


class ExtractDatasetResult(BaseModel):
    dataset: str
    status: str
    rows: int = 0
    output_file: str | None = None
    error: str | None = None


class ExtractResponse(BaseModel):
    run_id: str
    results: list[ExtractDatasetResult]


class AnalyzeRequest(BaseModel):
    question: str
    include_raw_preview: bool = False


class AgentOutput(BaseModel):
    agent: str
    content: dict[str, Any]


class AnalyzeResponse(BaseModel):
    run_id: str
    available_datasets: list[str]
    profiles: dict[str, Any]
    agent_outputs: list[AgentOutput]
    consensus: dict[str, Any]
    final_decision: str
    result_file: str


class ErrorResponse(BaseModel):
    detail: str


class SalesforceQueryRequest(BaseModel):
    soql: str
    query_all: bool = False
    max_pages: int = 50
