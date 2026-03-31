from bookbuddy_models import Keyword
from fastapi import APIRouter, HTTPException
from neo4j.exceptions import Neo4jError
from pydantic import BaseModel

from graph.db import get_driver

router = APIRouter(prefix="/keywords", tags=["keywords"])


@router.get("")
async def list_keywords() -> list[Keyword]:
    driver = await get_driver()
    try:
        async with driver.session() as session:
            result = await session.run(
                """
                MATCH (k:Keyword)
                RETURN
                    k.uuid AS uuid,
                    k.created_at AS created_at,
                    k.updated_at AS updated_at,
                    k.display_name AS display_name,
                    k.openalex_id AS openalex_id,
                    k.cited_by_count AS cited_by_count,
                    k.works_count AS works_count
                """
            )
            records = await result.data()
    except Neo4jError as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch keywords: {e}")
    return [
        Keyword(
            uuid=r["uuid"],
            created_at=r["created_at"].to_native(),
            updated_at=r["updated_at"].to_native(),
            openalex_id=r["openalex_id"],
            display_name=r["display_name"],
            works_count=r["works_count"],
            cited_by_count=r["cited_by_count"],
        )
        for r in records
    ]


class StudyGraphRequest(BaseModel):
    uuids: list[str]
    top_k: int = 5


class ChunkInfo(BaseModel):
    uuid: str
    text: str


class KeywordWithChunks(BaseModel):
    uuid: str
    display_name: str
    chunks: list[ChunkInfo]


class StudyGraphResponse(BaseModel):
    keywords: list[dict]
    edges: list[dict]
    keyword_chunks: list[KeywordWithChunks]


@router.post("/study-graph", response_model=StudyGraphResponse)
async def get_study_graph(request: StudyGraphRequest) -> StudyGraphResponse:
    driver = await get_driver()
    try:
        async with driver.session() as session:
            subgraph_result = await (await session.run(
                """
                MATCH (seed:Keyword) WHERE seed.uuid IN $uuids
                WITH collect(seed) AS seeds

                UNWIND seeds AS a
                UNWIND seeds AS b
                WITH seeds, a, b WHERE a <> b
                OPTIONAL MATCH path = shortestPath((a)-[:IS_PREREQUISITE_FOR*]->(b))

                WITH seeds, collect(DISTINCT [n IN CASE WHEN path IS NULL THEN [] ELSE nodes(path) END | n]) AS all_paths
                UNWIND all_paths AS path_nodes
                UNWIND path_nodes AS n
                WITH seeds, collect(DISTINCT n) AS path_nodes

                WITH seeds + path_nodes AS combined
                UNWIND combined AS n
                WITH collect(DISTINCT n) AS final_nodes

                UNWIND final_nodes AS src
                OPTIONAL MATCH (src)-[:IS_PREREQUISITE_FOR]->(tgt)
                WHERE tgt IN final_nodes

                RETURN
                    [n IN final_nodes | {uuid: n.uuid, display_name: n.display_name}] AS keywords,
                    collect(DISTINCT CASE WHEN tgt IS NULL THEN null ELSE {source: src.uuid, target: tgt.uuid} END) AS edges
                """,
                {"uuids": request.uuids},
            )).single()

            keywords = subgraph_result["keywords"]
            edges = [e for e in subgraph_result["edges"] if e is not None]
            keyword_uuids = [k["uuid"] for k in keywords]

            chunk_records = await (await session.run(
                """
                UNWIND $keyword_uuids AS kw_uuid
                MATCH (k:Keyword {uuid: kw_uuid})-[r:MENTIONED_IN]->(c:Chunk)
                WITH k, c, r.weight AS weight
                ORDER BY k.uuid, weight DESC
                WITH k.uuid AS keyword_uuid, collect(c)[0..$top_k] AS chunks
                RETURN keyword_uuid, [c IN chunks | {uuid: c.uuid, text: c.text}] AS chunks
                """,
                {"keyword_uuids": keyword_uuids, "top_k": request.top_k},
            )).data()

    except Neo4jError as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch study graph: {e}")

    chunks_by_kw = {r["keyword_uuid"]: r["chunks"] for r in chunk_records}
    keyword_chunks = [
        KeywordWithChunks(
            uuid=k["uuid"],
            display_name=k["display_name"],
            chunks=[ChunkInfo(**c) for c in chunks_by_kw.get(k["uuid"], [])],
        )
        for k in keywords
    ]
    return StudyGraphResponse(keywords=keywords, edges=edges, keyword_chunks=keyword_chunks)


@router.post("", status_code=201)
async def create_keyword(keyword: Keyword) -> Keyword:
    driver = await get_driver()
    try:
        async with driver.session() as session:
            await session.run(
                """
                MERGE (k:Keyword {name: $name})
                ON CREATE SET k.aliases = $aliases
                ON MATCH SET k.aliases = $aliases
                """,
                name=keyword.name,
                aliases=keyword.aliases,
            )
    except Neo4jError as e:
        raise HTTPException(status_code=500, detail=f"Failed to create keyword: {e}")
    return keyword
