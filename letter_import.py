"""letter_import.py - reads a saved price letter (PDF or image) with Claude and returns structured rows."""
import os
import base64
import json
import anthropic

MODEL = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-5")

EXTRACT_TOOL = {
    "name": "record_price_letter",
    "description": "Record every product line and the sales contact printed on this price letter.",
    "input_schema": {
        "type": "object",
        "properties": {
            "sales_person_name": {"type": "string"},
            "sales_person_phone": {"type": "string"},
            "sales_person_email": {"type": "string"},
            "rows": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "product": {"type": "string", "description": "Product name exactly as printed"},
                        "description": {"type": "string"},
                        "um": {"type": "string", "description": "Unit of measure as printed, e.g. GAL, LB"},
                        "package_type": {"type": "string"},
                        "final_price": {"type": "number", "description": "Final price per unit as printed"},
                        "shipping": {"type": "number", "description": "Shipping/freight added per unit if shown, else 0"},
                        "packaging": {"type": "number", "description": "Packaging added per unit if shown, else 0"},
                    },
                    "required": ["product", "um", "final_price"],
                },
            },
        },
        "required": ["rows"],
    },
}


SUPPORTED_TYPES = {
    ".pdf": "application/pdf",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
}


def media_type_for(filename: str):
    """Return the media type for a supported letter file, or None."""
    ext = os.path.splitext(filename or "")[1].lower()
    return SUPPORTED_TYPES.get(ext)


def extract_price_letter(file_bytes: bytes, media_type: str = "application/pdf") -> dict:
    """Send the letter (PDF or image) to Claude and return {'sales_person_*': ..., 'rows': [...]}."""
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY is not set.")

    client = anthropic.Anthropic(api_key=api_key)
    resp = client.messages.create(
        model=MODEL,
        max_tokens=4000,
        tools=[EXTRACT_TOOL],
        tool_choice={"type": "auto"},
        messages=[{
            "role": "user",
            "content": [
                {"type": "document" if media_type == "application/pdf" else "image", "source": {
                    "type": "base64",
                    "media_type": media_type,
                    "data": base64.b64encode(file_bytes).decode(),
                }},
                {"type": "text", "text": (
                    "This is one of our own outgoing price letters. Extract every product row "
                    "in order. Copy product names and units exactly as printed. Numbers only, no $ signs. "
                    "If a value is not on the letter, use 0 or an empty string - never guess. "
                    "Return the result by calling the record_price_letter tool."
                )},
            ],
        }],
    )
    for block in resp.content:
        if block.type == "tool_use" and block.name == "record_price_letter":
            return block.input

    # Fallback: model answered in text - pull out the JSON object.
    text = "".join(getattr(b, "text", "") for b in resp.content)
    start, end = text.find("{"), text.rfind("}")
    if start != -1 and end > start:
        try:
            data = json.loads(text[start:end + 1])
            if isinstance(data, dict) and isinstance(data.get("rows"), list):
                return data
        except ValueError:
            pass
    raise RuntimeError("Claude did not return structured data. Try the upload again.")