from fastapi import APIRouter, HTTPException
from openai import OpenAIError
from pydantic import BaseModel

from inference.client import get_client
from inference.config import GENERATION_MODEL, GENERATION_PROMPT
from inference.prompts import load_system_prompt

router = APIRouter(prefix="/generate", tags=["generation"])

_system_prompt = load_system_prompt(GENERATION_PROMPT)


class GenerationRequest(BaseModel):
    input: str


class GenerationResponse(BaseModel):
    output: str


@router.post("", response_model=GenerationResponse)
async def generate(request: GenerationRequest):

    client = await get_client()
    try:
        response = await client.responses.create(
            model=GENERATION_MODEL,
            instructions=_system_prompt,
            input=request.input,
        )
    except OpenAIError as e:
        raise HTTPException(status_code=502, detail=f"Generation failed: {e}")

    return GenerationResponse(output=response.output_text)
