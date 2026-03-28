# BookBuddy

A GraphRAG application that helps university students review, revise, and study academic topics. Students upload course materials (PDFs, slides, notes) and BookBuddy builds a knowledge graph from them, enabling grounded, context-aware Q&A backed by their own documents.

## Services

### `ingestion-service`
Accepts raw file uploads (PDF, DOCX, PPTX, plain text) and converts them into a standardised internal Document object. Responsible for format detection, text extraction.

**Key responsibilities:**
- File format detection and parsing
- Text extraction and layout normalisation

---

### `processing-service`
Prepares the content for graph storage and LLM use. Splits documents into semantically coherent chunks and applies cleaning/normalisation passes.

**Key responsibilities:**
- Text chunking (sentence-aware, overlap-preserving)
- Text cleaning and deduplication

---

### `extraction-service`
Consumes individual chunk and runs an entity extraction pipeline over the text. Returns structured entities.

**Key responsibilities:**
- Named entity recognition (NER)
- Taxonomy/ontology mapping (e.g. classifying entity types relevant to academic domains)

---

### `graph-service`
The application layer sitting in front of Neo4j. Owns all reads and writes to the knowledge graph. Persist entities and relationships, and exposes a query API used by `query-service` at retrieval time.

**Key responsibilities:**
- Persisting nodes and edges from extraction results
- Subgraph retrieval and traversal for RAG context
- Graph schema management and migrations
- Exposing a HTTP API for graph queries

> **Note:** Neo4j itself is a dependency run as a container — `graph-service` is the application logic wrapping it, not the database process.

---

### `inference-service`
Serves a language model via an OpenAI-compatible HTTP API. Backed by either [vLLM](https://github.com/vllm-project/vllm) or [Ollama](https://ollama.com). All other services that need LLM completions or embeddings call this service exclusively, keeping model-specific logic in one place.

**Key responsibilities:**
- Chat completions endpoint
- Embeddings endpoint
- Model loading
- OpenAI-compatible API surface (`/v1/chat/completions`, `/v1/embeddings`)

---

### `query-service`
The user-facing RAG orchestrator. Accepts a natural language question, retrieves the most relevant subgraph context from `graph-service`, assembles a grounded prompt, and calls `inference-service` to generate a response. This is where the GraphRAG retrieval strategy lives.

**Key responsibilities:**
- Query understanding and decomposition
- Subgraph retrieval (entity linking + graph traversal)
- Prompt construction with retrieved context
- Streaming response delivery to the client
- Session / conversation history management

---

## Repository Structure

```
bookbuddy/
├── services/
│   ├── ingestion-service/       # File → Document
│   │   ├── src/
│   │   ├── tests/
│   │   └── Dockerfile
│   ├── processing-service/      # Document → Chunks
│   │   ├── src/
│   │   ├── tests/
│   │   └── Dockerfile
│   ├── extraction-service/      # Chunk → Entities + Relationships
│   │   ├── src/
│   │   ├── tests/
│   │   └── Dockerfile
│   ├── inference-service/       # LLM serving (vLLM / Ollama)
│   │   └── config/
│   ├── graph-service/           # Neo4j application layer
│   │   ├── src/
│   │   ├── tests/
│   │   └── Dockerfile
│   └── query-service/           # GraphRAG orchestration + student API
│       ├── src/
│       ├── tests/
│       └── Dockerfile
├── shared/
│   ├── models/                  # Shared Pydantic schemas (Document, Chunk, Entity…)
│   └── messaging/               # Message broker helpers and event definitions
├── infrastructure/
│   ├── docker-compose.yml       # Full stack (all services + Neo4j + RabbitMQ)
│   ├── docker-compose.dev.yml   # Dev overrides (hot reload, local ports)
│   └── k8s/                     # Kubernetes manifests
├── docs/
│   ├── architecture.md          # Detailed architecture decisions
│   └── api/                     # OpenAPI / AsyncAPI specs
├── scripts/
│   ├── setup.sh                 # First-time environment setup
│   └── seed.sh                  # Load sample documents for local dev
├── .github/
│   └── workflows/               # CI/CD pipelines
├── Makefile                     # Common dev commands
└── README.md
```