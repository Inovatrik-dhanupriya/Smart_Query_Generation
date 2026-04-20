"""Parse Gemini `generate_content` responses into text / JSON slices."""
from __future__ import annotations


def text_from_generate_response(response) -> str:
    """Normalize Gemini generate_content response to a single string (may be empty)."""
    t = getattr(response, "text", None)
    if isinstance(t, str) and t.strip():
        return t.strip()
    chunks: list[str] = []
    for c in getattr(response, "candidates", None) or []:
        content = getattr(c, "content", None)
        if not content:
            continue
        for p in getattr(content, "parts", None) or []:
            pt = getattr(p, "text", None)
            if isinstance(pt, str) and pt:
                chunks.append(pt)
    return "\n".join(chunks).strip()


def json_slice_from_text(s: str) -> str:
    """If the model wrapped JSON in prose/markdown, take the outermost {...} block."""
    s = s.strip()
    if not s:
        return s
    l, r = s.find("{"), s.rfind("}")
    if l >= 0 and r > l:
        return s[l : r + 1]
    return s
