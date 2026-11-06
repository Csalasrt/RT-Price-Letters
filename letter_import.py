"""letter_import.py - reads a saved price letter PDF with Claude and returns structured rows."""
import os
import base64
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


def extract_price_letter(pdf_bytes: bytes) -> dict:
    """Send the PDF to Claude and return {'sales_person_*': ..., 'rows': [...]}."""
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY is not set.")

    client = anthropic.Anthropic(api_key=api_key)
    resp = client.messages.create(
        model=MODEL,
        max_tokens=4000,
        tools=[EXTRACT_TOOL],
        tool_choice={"type": "tool", "name": "record_price_letter"},
        messages=[{
            "role": "user",
            "content": [
                {"type": "document", "source": {
                    "type": "base64",
                    "media_type": "application/pdf",
                    "data": base64.b64encode(pdf_bytes).decode(),
                }},
                {"type": "text", "text": (
                    "This is one of our own outgoing price letters. Extract every product row "
                    "in order. Copy product names and units exactly as printed. Numbers only, no $ signs. "
                    "If a value is not on the letter, use 0 or an empty string - never guess."
                )},
            ],
        }],
    )
    for block in resp.content:
        if block.type == "tool_use":
            return block.input
    raise RuntimeError("Claude did not return structured data.")