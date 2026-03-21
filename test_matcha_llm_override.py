"""
Quick test: send a prompt to Matcha with an explicit LLM override
via the inference_server field.
"""

import pytest
import requests

from matcha_client import call_matcha

LLM_OVERRIDE = 68  # GPT-5 Mini


def test_llm_override():
    prompt = "Say hello and tell me which model you are."

    print(f"Calling call_matcha with inference_server={LLM_OVERRIDE!r}")
    print()

    try:
        reply = call_matcha(prompt, inference_server=LLM_OVERRIDE)
    except requests.RequestException as exc:
        pytest.skip(f"Matcha integration unavailable in this environment: {exc}")

    print(f"Reply:\n{reply}")


if __name__ == "__main__":
    test_llm_override()
