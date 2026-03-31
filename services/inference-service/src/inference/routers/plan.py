import asyncio
from collections import defaultdict, deque

from fastapi import APIRouter, HTTPException
from openai import OpenAIError
from pydantic import BaseModel

from inference.client import get_client
from inference.config import GENERATION_MODEL
from inference.graph_client import KeywordWithChunks, StudyGraph, fetch_study_graph
from inference.prompts import load_system_prompt

router = APIRouter(prefix="/plan", tags=["plan"])

_system_prompt = load_system_prompt("keyword_summary")


class PlanRequest(BaseModel):
    uuids: list[str]
    top_k: int = 5


class PlanResponse(BaseModel):
    plan: str


def _topological_sort(keywords: list[dict], edges: list[dict]) -> list[dict]:
    uuid_to_kw = {k["uuid"]: k for k in keywords}
    in_degree = {k["uuid"]: 0 for k in keywords}
    adjacency: dict[str, list[str]] = defaultdict(list)
    for edge in edges:
        src, tgt = edge["source"], edge["target"]
        adjacency[src].append(tgt)
        in_degree[tgt] = in_degree.get(tgt, 0) + 1
    queue = deque(uuid for uuid, deg in in_degree.items() if deg == 0)
    ordered = []
    while queue:
        node = queue.popleft()
        ordered.append(uuid_to_kw[node])
        for neighbor in adjacency[node]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)
    return ordered if len(ordered) == len(keywords) else keywords


async def _summarize_keyword(
    kw: KeywordWithChunks, client, system_prompt: str
) -> tuple[str, str]:
    passages = "\n\n".join(f"[{i + 1}] {c.text}" for i, c in enumerate(kw.chunks))
    user_input = f"Keyword: {kw.display_name}\n\nSource passages:\n{passages}"
    try:
        response = await client.responses.create(
            model=GENERATION_MODEL,
            instructions=system_prompt,
            input=user_input,
        )
    except OpenAIError as e:
        raise HTTPException(
            status_code=502, detail=f"Generation failed for '{kw.display_name}': {e}"
        )
    return kw.uuid, response.output_text


@router.post("", response_model=PlanResponse)
async def generate_plan(request: PlanRequest) -> PlanResponse:
    graph: StudyGraph = await fetch_study_graph(request.uuids, top_k=request.top_k)

    client = await get_client()
    results = await asyncio.gather(
        *[_summarize_keyword(kw, client, _system_prompt) for kw in graph.keyword_chunks]
    )
    summary_by_uuid = dict(results)

    ordered_keywords = _topological_sort(graph.keywords, graph.edges)

    sections = []
    for kw in ordered_keywords:
        summary = summary_by_uuid.get(kw["uuid"], "")
        if summary:
            sections.append(f"## {kw['display_name']}\n{summary}")

    return PlanResponse(plan="\n\n".join(sections))
