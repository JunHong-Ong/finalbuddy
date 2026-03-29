from bookbuddy_models import Chunk, Document
from langchain_text_splitters import RecursiveCharacterTextSplitter
from transformers import AutoTokenizer

from processing.chunker import BaseChunker
from processing.config import CHUNK_OVERLAP, CHUNK_SIZE, TOKENIZER_MODEL

_tokenizer = AutoTokenizer.from_pretrained(TOKENIZER_MODEL)

_splitter = RecursiveCharacterTextSplitter.from_huggingface_tokenizer(
    _tokenizer,
    separators=["\n\n", ". ", ""],
    chunk_size=CHUNK_SIZE,
    chunk_overlap=CHUNK_OVERLAP,
)


class RecursiveChunker(BaseChunker):
    def split(self, document: Document) -> list[tuple[str, list[str]]]:
        results: list[tuple[str, list[str]]] = []
        current_section = ""
        accumulated: list[str] = []

        def flush() -> None:
            if accumulated:
                text = "\n".join(accumulated)
                chunks = _splitter.split_text(text)
                if chunks:
                    results.append((current_section, chunks))
                accumulated.clear()

        for segment in document.segments:
            for element in sorted(segment.elements, key=lambda e: e.order):
                if element.type == "section":
                    flush()
                    current_section = element.content
                elif element.type == "text":
                    accumulated.append(element.content)

        flush()
        return results

    def map(self, document: Document, raw: list[tuple[str, list[str]]]) -> list[Chunk]:
        chunks: list[Chunk] = []
        for section_title, texts in raw:
            for index, text in enumerate(texts):
                chunks.append(
                    Chunk(
                        document_id=document.uuid,
                        section_title=section_title,
                        chunk_index=index,
                        text=text,
                    )
                )
        return chunks
