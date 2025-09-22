"""
AS/400 Legacy -> Modern transformation backend (prototype)
Extended to support CSV, TSV, Pipe-delimited, Fixed-width, Excel (.xls/.xlsx), DB2 exports, Green-screen logs
File: as400_migration_backend.py
"""

from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel
import pandas as pd
import io
import re
import json
import sqlite3
from typing import List, Optional, Any
import requests


app = FastAPI(title="AS400 Modernizer Prototype")

# ---------------------- Local LLM Config ----------------------
OLLAMA_URL = "http://localhost:11434/api/generate"
LOCAL_MODEL = "gemma:2b"

def ask_llm(prompt: str, model: str = LOCAL_MODEL) -> str:
    try:
        response = requests.post(OLLAMA_URL, json={
            "model": model,
            "prompt": prompt,
            "stream": False
        })
        return response.json().get("response", "").strip()
    except Exception as e:
        return f"[LLM error: {e}]"

# ---------------------- Utilities ----------------------

def detect_encoding(b: bytes) -> str:
    for enc in ("utf-8", "cp037", "cp500", "latin1"):
        try:
            b.decode(enc)
            return enc
        except Exception:
            pass
    return "latin1"

def detect_format(text: str, hint: Optional[str] = None) -> str:
    if hint:
        return hint.lower()
    sample = "\n".join(text.splitlines()[:20])
    if '\t' in sample:
        return "tsv"
    comma_count = sample.count(",")
    pipe_count = sample.count("|")
    if comma_count > 3:
        return "csv"
    if pipe_count > 0 and pipe_count > comma_count:
        return "pipe"
    lines = [l for l in text.splitlines() if l.strip()]
    if len(lines) >= 3:
        lengths = [len(l) for l in lines[:50]]
        if max(lengths) - min(lengths) < 5 and comma_count == 0:
            return "fixed"
    return "raw"

def try_decode_bytes(b: bytes) -> str:
    enc = detect_encoding(b)
    try:
        return b.decode(enc)
    except Exception:
        return b.decode("latin1", errors="replace")

def normalize_colname(name: str) -> str:
    name = name.strip()
    name = re.sub(r"[^0-9a-zA-Z_]+", "_", name)
    name = name.strip("_")
    if name == "":
        return "col"
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    name = name.replace("-", "_")
    return name.lower()

def camel_case(s: str) -> str:
    parts = re.split(r"[_\s]+", s)
    return parts[0].lower() + "".join(p.title() for p in parts[1:]) if parts else s

def suggest_mappings(columns: List[str]) -> List[dict]:
    suggestions = []
    for c in columns:
        suggested = normalize_colname(c)
        suggestions.append({
            "original": c,
            "snake_case": suggested,
            "camelCase": camel_case(suggested),
            "human": suggested.replace("_", " ").title(),
        })
    return suggestions

def infer_sql_type(series: pd.Series) -> str:
    s = series.dropna().astype(str)
    if len(s) == 0:
        return "TEXT"
    sample = s.sample(min(len(s), 200)) if len(s) > 200 else s

    def is_int(x):
        try:
            if x.strip() == "":
                return False
            int(x)
            return True
        except Exception:
            return False

    def is_float(x):
        try:
            float(x)
            return True
        except Exception:
            return False

    ints = sample.map(is_int).sum()
    if ints / len(sample) > 0.9:
        return "INTEGER"

    floats = sample.map(is_float).sum()
    if floats / len(sample) > 0.9:
        return "REAL"

    try:
        parsed = pd.to_datetime(sample, errors="coerce", dayfirst=False)
        if parsed.notna().sum() / len(sample) > 0.7:
            return "TIMESTAMP"
    except Exception:
        pass

    return "TEXT"

def quote_sql_value(v: Any) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, (int, float)):
        return str(v)
    s = str(v)
    s = s.replace("'", "''")
    return f"'{s}'"

def generate_create_table_sql(table_name: str, df: pd.DataFrame) -> str:
    cols = []
    for col in df.columns:
        t = infer_sql_type(df[col])
        cols.append(f'"{col}" {t}')
    cols_sql = ",\n  ".join(cols)
    return f'CREATE TABLE IF NOT EXISTS "{table_name}" (\n  {cols_sql}\n);'

def generate_insert_statements(table_name: str, df: pd.DataFrame, limit: int = 100) -> str:
    rows = df.head(limit).to_dict(orient="records")
    stmts = []
    for r in rows:
        col_names = ", ".join([f'"{c}"' for c in r.keys()])
        col_vals = ", ".join([quote_sql_value(v) for v in r.values()])
        stmts.append(f'INSERT INTO "{table_name}" ({col_names}) VALUES ({col_vals});')
    return "\n".join(stmts)

def parse_text_to_df(text: str, fmt: str, fwf_widths: Optional[List[int]] = None) -> pd.DataFrame:
    buf = io.StringIO(text)
    if fmt == "csv":
        try:
            return pd.read_csv(buf, dtype=str)
        except Exception:
            buf.seek(0)
            return pd.read_csv(buf, dtype=str, engine="python")
    if fmt == "tsv":
        return pd.read_csv(buf, sep="\t", dtype=str)
    if fmt == "pipe":
        return pd.read_csv(buf, sep="|", dtype=str)
    if fmt == "fixed":
        if fwf_widths:
            return pd.read_fwf(buf, widths=fwf_widths, dtype=str)
        return pd.read_fwf(buf, dtype=str, infer_nrows=200)
    # raw / green-screen parsing
    lines = text.splitlines()
    rows = [re.split(r"\s{2,}", l.strip()) for l in lines if l.strip()]
    if len(rows) >= 1 and len(rows[0]) > 1:
        df = pd.DataFrame(rows)
        header = rows[0]
        if all(re.search(r"[A-Za-z]", h or "") for h in header):
            df = pd.DataFrame(rows[1:], columns=header)
        return df.astype(str)
    return pd.DataFrame({"raw_line": [l for l in lines if l.strip()]})

def parse_file_to_df(file: UploadFile, fmt_hint: Optional[str] = None) -> pd.DataFrame:
    filename = file.filename.lower() if file.filename else ''
    content = file.file.read()
    text = try_decode_bytes(content)
    # Excel support
    if filename.endswith(('.xls', '.xlsx')):
        file.file.seek(0)
        return pd.read_excel(file.file, dtype=str)
    fmt = detect_format(text, fmt_hint)
    return parse_text_to_df(text, fmt)

# ---------------------- API Models ----------------------

class GenerateAPIRequest(BaseModel):
    table_name: str
    columns: List[dict]

# ---------------------- API Endpoints ----------------------

@app.post("/upload-file")
async def upload_file(
    file: UploadFile = File(...),
    format_hint: Optional[str] = Form(None),
    table_name: Optional[str] = Form(None),
    fwf_widths: Optional[str] = Form(None)
):
    df = parse_file_to_df(file, format_hint)

    original_columns = list(df.columns)
    normalized_columns = [normalize_colname(c) for c in original_columns]
    df.columns = normalized_columns

    mappings = suggest_mappings(original_columns)
    if not table_name:
        table_name = normalize_colname(file.filename or "legacy_table")

    create_sql = generate_create_table_sql(table_name, df)
    insert_preview = generate_insert_statements(table_name, df, limit=20)

    # --- AI-assisted suggestions ---
    ai_prompt = f"""
    Here are legacy column names: {original_columns}.
    Suggest modern, human-readable names in snake_case.
    Respond as a JSON list of strings.
    """
    ai_suggestions = ask_llm(ai_prompt)

    result = {
        "filename": file.filename,
        "detected_format": format_hint or 'auto-detected',
        "encoding_used": detect_encoding(file.file.read() or b""),  # read again for encoding
        "preview_rows": df.head(10).to_dict(orient="records"),
        "columns": normalized_columns,
        "mappings": mappings,
        "ai_suggested_columns": ai_suggestions,
        "create_table_sql": create_sql,
        "sample_insert_sql": insert_preview,
    }
    return JSONResponse(result)

@app.post("/ai-suggest")
async def ai_suggest(task: str, context: Optional[dict] = None):
    """
    Generic AI endpoint: pass a task string + optional context.
    Example: {"task": "summarize business rules", "context": {...}}
    """
    prompt = f"Task: {task}\nContext: {json.dumps(context, indent=2)}"
    suggestion = ask_llm(prompt)
    return {"task": task, "suggestion": suggestion}

@app.post("/generate-api")
async def generate_api_snippet(req: GenerateAPIRequest):
    cols = req.columns
    fields = "\n".join([
        f"    {c['name']}: {'int' if c['type']=='INTEGER' else 'float' if c['type']=='REAL' else 'str'}"
        for c in cols
    ])

    template = f"""
from fastapi import FastAPI
from pydantic import BaseModel
import sqlite3
from typing import List

app = FastAPI()
DB = 'generated.db'

class Item(BaseModel):
{fields}

conn = sqlite3.connect(DB)
conn.execute('''{generate_create_table_sql(req.table_name, pd.DataFrame({{c['name']: [] for c in cols}}))}''')
conn.commit()
conn.close()

@app.post(f'/{req.table_name}/', response_model=Item)
def create_item(item: Item):
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    data = item.dict()
    columns = ', '.join(data.keys())
    values = ', '.join(['?' for _ in data])
    cur.execute(f'INSERT INTO "{req.table_name}" ({{columns}}) VALUES ({{values}})', tuple(data.values()))
    conn.commit()
    conn.close()
    return item

@app.get(f'/{req.table_name}/', response_model=List[Item])
def list_items():
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    cur.execute(f'SELECT * FROM "{req.table_name}"')
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    conn.close()
    return [Item(**dict(zip(cols, r))) for r in rows]
"""
    return PlainTextResponse(template)


@app.get("/")
async def root():
    return {"msg": "AS/400 Modernizer prototype. POST /upload-file to start."}

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8000, reload=True)

