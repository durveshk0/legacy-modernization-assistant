"""
Simple test for local LLM (Gemma2b) via Ollama API
"""

import requests

OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL = "gemma:2b"

def ask_llm(prompt: str, model: str = MODEL) -> str:
    try:
        response = requests.post(OLLAMA_URL, json={
            "model": model,
            "prompt": prompt,
            "stream": False
        })
        return response.json().get("response", "").strip()
    except Exception as e:
        return f"[LLM error: {e}]"

if __name__ == "__main__":
    test_prompt = """
    You are a helpful assistant.
    Convert these legacy column names into modern snake_case:
    ['EMP_ID', 'FIRSTNME', 'LASTNAME', 'DOB', 'SALARY']
    Respond as a JSON list of strings.
    """
    
    result = ask_llm(test_prompt)
    print("AI Response:")
    print(result)
