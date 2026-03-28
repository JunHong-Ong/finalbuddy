from inference.config import settings


def load_system_prompt(name: str) -> str:
    """Load a system prompt by name from the configured prompts directory."""
    path = settings.prompts_dir / f"{name}.md"
    if not path.exists():
        raise FileNotFoundError("System prompt not found")
    return path.read_text(encoding="utf-8").strip()
