"""
api.py — Report Defence FastAPI Backend
========================================
Endpoints:
  Auth:
    POST /auth/register
    POST /auth/login
    GET  /auth/me
    POST /auth/supabase
    POST /auth/google
    PATCH /auth/upgrade-role

  Operator:
    POST /upload-report
    POST /generate-letters
    GET  /download/{job_id}/{filename}
    GET  /download-zip/{job_id}
    POST /bureau-response

  Clients:
    GET    /clients
    POST   /clients
    GET    /clients/{id}
    PATCH  /clients/{id}
    DELETE /clients/{id}
    GET    /clients/{id}/history
    GET    /clients/{id}/letters

  Portal:
    GET /portal/overview
    GET /portal/letters

  Error reporting:
    POST /report-error
    GET  /errors

  Health:
    GET /health

Run with:
  uvicorn api:app --reload --port 8000
"""

from __future__ import annotations

import io
import json
import os
import sys
import uuid
import zipfile
from datetime import datetime, timedelta
from pathlib import Path

import jwt
from fastapi import Depends, FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from passlib.context import CryptContext
from pydantic import BaseModel, EmailStr

# ── Path setup ────────────────────────────────────────────────────────────────
_HERE = Path(__file__).parent
sys.path.insert(0, str(_HERE))

from original_parser import (
    BUREAU_RESPONSE_DELETED,
    BUREAU_RESPONSE_FRIVOLOUS,
    BUREAU_RESPONSE_NO_RESPONSE,
    BUREAU_RESPONSE_REINSERTION,
    BUREAU_RESPONSE_UNABLE,
    BUREAU_RESPONSE_UPDATED,
    BUREAU_RESPONSE_VERIFIED,
    build_bureau_response_letter,
    build_dispute_letter_engine,
    build_furnisher_letter_engine,
    build_report,
)
from letter_generator import build_identityiq_letters, promote_first_dispute, write_pdf

# ── Config ────────────────────────────────────────────────────────────────────
SECRET_KEY    = os.getenv("JWT_SECRET", "report-defence-dev-secret-change-in-prod")
JWT_ALG       = "HS256"
JWT_EXPIRE_H  = 24 * 7
OPERATOR_CODE = os.getenv("OPERATOR_CODE", "RD-OPERATOR-2024")

UPLOAD_DIR  = Path(os.getenv("UPLOAD_DIR",  "/tmp/rd_uploads"))
LETTERS_DIR = Path(os.getenv("LETTERS_DIR", "/tmp/rd_letters"))
DB_FILE     = Path(os.getenv("DB_FILE",     "/tmp/rd_db.json"))

UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
LETTERS_DIR.mkdir(parents=True, exist_ok=True)

# ── Database ──────────────────────────────────────────────────────────────────
def _load_db() -> dict:
    if DB_FILE.exists():
        try:
            return json.loads(DB_FILE.read_text())
        except Exception:
            pass
    return {"users": {}, "clients": {}, "jobs": {}, "errors": []}

def _save_db(db: dict) -> None:
    DB_FILE.write_text(json.dumps(db, indent=2, default=str))

# ── Password hashing ──────────────────────────────────────────────────────────
_pwd = CryptContext(schemes=["bcrypt"], deprecated="auto")

def hash_password(plain: str) -> str:
    return _pwd.hash(plain)

def verify_password(plain: str, hashed: str) -> bool:
    return _pwd.verify(plain, hashed)

# ── JWT ───────────────────────────────────────────────────────────────────────
def create_token(user_id: str, role: str) -> str:
    payload = {
        "sub": user_id,
        "role": role,
        "exp": datetime.utcnow() + timedelta(hours=JWT_EXPIRE_H),
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=JWT_ALG)

def decode_token(token: str) -> dict:
    try:
        return jwt.decode(token, SECRET_KEY, algorithms=[JWT_ALG])
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

# ── Auth dependency ───────────────────────────────────────────────────────────
bearer = HTTPBearer(auto_error=False)

def get_current_user(creds: HTTPAuthorizationCredentials | None = Depends(bearer)) -> dict:
    if not creds:
        raise HTTPException(status_code=401, detail="Not authenticated")
    payload = decode_token(creds.credentials)
    db = _load_db()
    user = db["users"].get(payload["sub"])
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
    return user

def require_operator(user: dict = Depends(get_current_user)) -> dict:
    if user.get("role") != "operator":
        raise HTTPException(status_code=403, detail="Operator access required")
    return user

def _safe_user(u: dict) -> dict:
    return {k: v for k, v in u.items() if k != "password"}

# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(title="Report Defence API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://reportdefence.com",
        "https://www.reportdefence.com",
        "https://4d383886-de3b-4f5a-906b-e80750cf3c2c.lovableproject.com",
        "https://credit-shield-ui.lovable.app",
        "https://id-preview--4d383886-de3b-4f5a-906b-e80750cf3c2c.lovable.app",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =============================================================================
# AUTH
# =============================================================================

class RegisterBody(BaseModel):
    full_name: str
    email: EmailStr
    password: str
    role: str = "client"
    operator_code: str = ""

class LoginBody(BaseModel):
    email: EmailStr
    password: str

class SupabaseAuthBody(BaseModel):
    access_token: str
    email: str
    full_name: str = ""


@app.post("/auth/register", status_code=201)
def register(body: RegisterBody):
    db = _load_db()
    if body.role == "operator" and body.operator_code != OPERATOR_CODE:
        raise HTTPException(status_code=403, detail="Invalid operator code")
    for u in db["users"].values():
        if u["email"] == body.email:
            raise HTTPException(status_code=409, detail="Email already registered")
    user_id = str(uuid.uuid4())
    db["users"][user_id] = {
        "id":         user_id,
        "full_name":  body.full_name,
        "email":      body.email,
        "password":   hash_password(body.password),
        "role":       body.role,
        "created_at": datetime.utcnow().isoformat(),
        "client_ids": [],
    }
    _save_db(db)
    token = create_token(user_id, body.role)
    return {"token": token, "user": _safe_user(db["users"][user_id])}


@app.post("/auth/login")
def login(body: LoginBody):
    db = _load_db()
    user = next((u for u in db["users"].values() if u["email"] == body.email), None)
    if not user or not verify_password(body.password, user["password"]):
        raise HTTPException(status_code=401, detail="Invalid email or password")
    token = create_token(user["id"], user["role"])
    return {"token": token, "user": _safe_user(user)}


@app.get("/auth/me")
def me(user: dict = Depends(get_current_user)):
    return _safe_user(user)


@app.post("/auth/supabase")
def supabase_login(body: SupabaseAuthBody):
    if not body.email:
        raise HTTPException(status_code=400, detail="Email required")
    db   = _load_db()
    user = next((u for u in db["users"].values() if u["email"] == body.email), None)
    if not user:
        user_id = str(uuid.uuid4())
        db["users"][user_id] = {
            "id":            user_id,
            "full_name":     body.full_name or body.email,
            "email":         body.email,
            "password":      "",
            "role":          "client",
            "created_at":    datetime.utcnow().isoformat(),
            "client_ids":    [],
            "auth_provider": "google",
        }
        _save_db(db)
        user = db["users"][user_id]
    token = create_token(user["id"], user["role"])
    return {
        "access_token": token,
        "token":        token,
        "user": {
            "id":        user["id"],
            "full_name": user["full_name"],
            "email":     user["email"],
            "role":      user["role"],
        }
    }


@app.post("/auth/google")
def google_login(body: SupabaseAuthBody):
    return supabase_login(body)


@app.patch("/auth/upgrade-role")
def upgrade_role(body: dict, user: dict = Depends(get_current_user)):
    operator_code = body.get("operator_code", "")
    if operator_code != OPERATOR_CODE:
        raise HTTPException(status_code=403, detail="Invalid operator code")
    db = _load_db()
    db["users"][user["id"]]["role"] = "operator"
    _save_db(db)
    token = create_token(user["id"], "operator")
    return {
        "access_token": token,
        "token":        token,
        "user": {
            "id":        user["id"],
            "full_name": user["full_name"],
            "email":     user["email"],
            "role":      "operator",
        }
    }


# =============================================================================
# UPLOAD & ANALYZE
# =============================================================================

@app.post("/upload-report")
async def upload_report(
    file: UploadFile = File(...),
    consumer_name: str = Form(...),
    client_id: str = Form(...),
    source: str = Form("identityiq"),
    user: dict = Depends(require_operator),
):
    db = _load_db()
    client = db["clients"].get(client_id)
    if not client:
        raise HTTPException(status_code=404, detail="Client not found")

    job_id   = str(uuid.uuid4())
    pdf_path = UPLOAD_DIR / f"{job_id}.pdf"
    content  = await file.read()
    pdf_path.write_bytes(content)

    try:
        result = build_report(str(pdf_path))
    except Exception as e:
        pdf_path.unlink(missing_ok=True)
        raise HTTPException(status_code=422, detail=f"Parser error: {e}")

    neg        = result["negatives_by_bureau"]
    letters_in = result["letter_input_engine"]

    attacks = []
    for bureau, atk_list in result["legal_detection_engine"].items():
        for atk in atk_list:
            attacks.append({
                "attack_type":       atk["attack_type"],
                "bureau":            bureau,
                "account_name":      atk["accounts"][0]["name"] if atk["accounts"] else "",
                "severity":          _severity_label(atk["attack_type"]),
                "recommended_round": _round_for(atk["attack_type"]),
                "laws":              atk.get("strategy_tags", []),
                "reason":            atk.get("reason", ""),
            })

    scores        = _extract_scores(result)
    letter_groups = []
    for bureau, groups in letters_in.items():
        for group_key, items in groups.items():
            if not items:
                continue
            r1 = [i for i in items if i.get("recommended_round") == "round_1"]
            r2 = [i for i in items if i.get("recommended_round") == "round_2"]
            if r1:
                letter_groups.append({"bureau": bureau, "group": group_key, "round": "round_1", "account_count": len(r1)})
            if r2:
                letter_groups.append({"bureau": bureau, "group": group_key, "round": "round_2", "account_count": len(r2)})

    negatives_out = {}
    for bureau, accts in neg.items():
        negatives_out[bureau] = [
            {
                "name":               a.get("name", ""),
                "account_number":     a.get("account_number", ""),
                "negative_type":      a.get("negative_type", ""),
                "attack_type":        a.get("attack_type", ""),
                "dofd_estimated":     a.get("dofd_estimated"),
                "fcra_expiration":    a.get("fcra_expiration"),
                "late_payment_codes": a.get("late_payment_codes", []),
                "balance":            a.get("balance", ""),
                "status":             a.get("status", ""),
                "payment_status":     a.get("payment_status", ""),
            }
            for a in accts
        ]

    response_data = {
        "job_id":               job_id,
        "consumer_name":        consumer_name,
        "report_date":          result.get("report_date", ""),
        "source":               source,
        "scores":               scores,
        "negatives_by_bureau":  negatives_out,
        "personal_info_issues": result.get("personal_info_issues", []),
        "attacks":              attacks,
        "attack_count":         len(attacks),
        "letter_groups":        letter_groups,
        "inquiry_attacks":      result.get("inquiry_attacks", []),
    }

    db["jobs"][job_id] = {
        "job_id":               job_id,
        "client_id":            client_id,
        "operator_id":          user["id"],
        "consumer_name":        consumer_name,
        "source":               source,
        "report_date":          result.get("report_date", ""),
        "pdf_path":             str(pdf_path),
        "created_at":           datetime.utcnow().isoformat(),
        "scores":               scores,
        "attack_count":         len(attacks),
        "letters_generated":    False,
        "letter_files":         [],
        "negatives_by_bureau":  negatives_out,
        "personal_info_issues": result.get("personal_info_issues", []),
        "letter_input_engine":  _serialize_letter_input(letters_in),
        "attacks":              attacks,
    }

    if "job_ids" not in client:
        client["job_ids"] = []
    client["job_ids"].append(job_id)
    _save_db(db)
    return response_data


# =============================================================================
# GENERATE LETTERS
# =============================================================================

@app.post("/generate-letters")
def generate_letters(body: dict, user: dict = Depends(require_operator)):
    job_id        = body.get("job_id", "")
    consumer_name = body.get("consumer_name", "")
    db  = _load_db()
    job = db["jobs"].get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    consumer_name = consumer_name or job["consumer_name"]
    letter_input  = promote_first_dispute(job.get("letter_input_engine", {}))
    pi_issues     = job.get("personal_info_issues", [])

    bureau_letters = build_dispute_letter_engine(
        letter_input,
        consumer_name=consumer_name,
        report_date=job["report_date"],
        personal_info=None,
        personal_info_issues=pi_issues or None,
    )
    furnisher_letters = build_furnisher_letter_engine(
        letter_input,
        consumer_name=consumer_name,
        report_date=job["report_date"],
    )

    client_slug  = _safe_slug(consumer_name)
    job_dir      = LETTERS_DIR / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    letter_files = []

    for bureau, groups in bureau_letters.items():
        for group_key, rounds in groups.items():
            for round_key, text in rounds.items():
                if not text.strip():
                    continue
                rn    = round_key.replace("round_", "")
                fname = f"{client_slug}_{bureau}_{group_key}_round{rn}.pdf"
                write_pdf(text, str(job_dir / fname))
                letter_files.append({"filename": fname, "bureau": bureau, "group": group_key, "round": round_key, "type": "bureau", "download_url": f"/download/{job_id}/{fname}"})

    for furnisher_name, rounds in furnisher_letters.items():
        for round_key, text in rounds.items():
            if not text.strip():
                continue
            rn    = round_key.replace("round_", "")
            slug  = _safe_slug(furnisher_name)[:40]
            fname = f"{client_slug}_furnisher_{slug}_round{rn}.pdf"
            write_pdf(text, str(job_dir / fname))
            letter_files.append({"filename": fname, "furnisher": furnisher_name, "bureau": "all", "group": "collections_chargeoffs", "round": round_key, "type": "furnisher", "download_url": f"/download/{job_id}/{fname}"})

    job["letters_generated"] = True
    job["letter_files"]      = letter_files
    job["letters_at"]        = datetime.utcnow().isoformat()
    _save_db(db)
    return {"job_id": job_id, "letters": letter_files, "download_zip_url": f"/download-zip/{job_id}"}


# =============================================================================
# FILE DOWNLOAD
# =============================================================================

@app.get("/download/{job_id}/{filename}")
def download_letter(job_id: str, filename: str, user: dict = Depends(get_current_user)):
    path = LETTERS_DIR / job_id / filename
    if not path.exists():
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(str(path), media_type="application/pdf", filename=filename)


@app.get("/download-zip/{job_id}")
def download_zip(job_id: str, user: dict = Depends(get_current_user)):
    job_dir = LETTERS_DIR / job_id
    if not job_dir.exists():
        raise HTTPException(status_code=404, detail="No letters found for this job")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for pdf in job_dir.glob("*.pdf"):
            zf.write(pdf, pdf.name)
    buf.seek(0)
    return StreamingResponse(buf, media_type="application/zip", headers={"Content-Disposition": f"attachment; filename=letters_{job_id[:8]}.zip"})


# =============================================================================
# BUREAU RESPONSE LETTERS
# =============================================================================

class BureauResponseBody(BaseModel):
    job_id:           str
    bureau:           str
    response_type:    str
    response_date:    str
    accounts:         list[str] = []
    frivolous_reason: str = ""


@app.post("/bureau-response")
def bureau_response(body: BureauResponseBody, user: dict = Depends(require_operator)):
    db  = _load_db()
    job = db["jobs"].get(body.job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    neg   = job.get("negatives_by_bureau", {})
    accts = [a for a in neg.get(body.bureau, []) if not body.accounts or a["account_number"] in body.accounts]
    result = build_bureau_response_letter(
        response_type=body.response_type, bureau=body.bureau, accounts=accts,
        consumer_name=job["consumer_name"], response_date=body.response_date,
        report_date=job["report_date"], frivolous_reason=body.frivolous_reason,
    )

    letter_text = result.get("letter", "")
    job_dir     = LETTERS_DIR / body.job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    fname = f"{_safe_slug(job['consumer_name'])}_{body.bureau}_{body.response_type}_response.pdf"
    write_pdf(letter_text, str(job_dir / fname))

    if "response_history" not in job:
        job["response_history"] = []
    job["response_history"].append({"bureau": body.bureau, "response_type": body.response_type, "response_date": body.response_date, "accounts": body.accounts, "letter_file": fname, "logged_at": datetime.utcnow().isoformat()})
    _save_db(db)
    return {"letter_text": letter_text, "next_steps": result.get("next_steps", ""), "pdf_url": f"/download/{body.job_id}/{fname}", "response_type": body.response_type, "bureau": body.bureau}


# =============================================================================
# CLIENT MANAGEMENT
# =============================================================================

class ClientCreateBody(BaseModel):
    full_name:       str
    email:           str = ""
    phone:           str = ""
    address:         str = ""
    city:            str = ""
    state:           str = ""
    zip_code:        str = ""
    date_of_birth:   str = ""
    notes:           str = ""
    create_login:    bool = False
    client_password: str = ""


@app.get("/clients")
def list_clients(user: dict = Depends(require_operator)):
    db = _load_db()
    return [_client_summary(c, db) for c in db["clients"].values() if c.get("operator_id") == user["id"]]


@app.post("/clients", status_code=201)
def create_client(body: ClientCreateBody, user: dict = Depends(require_operator)):
    db        = _load_db()
    client_id = str(uuid.uuid4())
    client = {
        "id": client_id, "operator_id": user["id"], "full_name": body.full_name,
        "email": body.email, "phone": body.phone, "address": body.address,
        "city": body.city, "state": body.state, "zip_code": body.zip_code,
        "date_of_birth": body.date_of_birth, "notes": body.notes,
        "created_at": datetime.utcnow().isoformat(), "job_ids": [], "user_id": None,
    }
    if body.create_login and body.email and body.client_password:
        if next((u for u in db["users"].values() if u["email"] == body.email), None):
            raise HTTPException(status_code=409, detail="Email already registered")
        cuid = str(uuid.uuid4())
        db["users"][cuid] = {"id": cuid, "full_name": body.full_name, "email": body.email, "password": hash_password(body.client_password), "role": "client", "client_id": client_id, "created_at": datetime.utcnow().isoformat()}
        client["user_id"] = cuid
    db["clients"][client_id] = client
    _save_db(db)
    return client


@app.get("/clients/{client_id}")
def get_client(client_id: str, user: dict = Depends(get_current_user)):
    db     = _load_db()
    client = db["clients"].get(client_id)
    if not client:
        raise HTTPException(status_code=404, detail="Client not found")
    if user["role"] == "client":
        if user.get("client_id") != client_id:
            raise HTTPException(status_code=403, detail="Access denied")
    elif client.get("operator_id") != user["id"]:
        raise HTTPException(status_code=403, detail="Access denied")
    return _client_detail(client, db)


@app.patch("/clients/{client_id}")
def update_client(client_id: str, body: dict, user: dict = Depends(require_operator)):
    db     = _load_db()
    client = db["clients"].get(client_id)
    if not client or client.get("operator_id") != user["id"]:
        raise HTTPException(status_code=404, detail="Client not found")
    for k, v in body.items():
        if k in {"full_name", "email", "phone", "address", "city", "state", "zip_code", "date_of_birth", "notes"}:
            client[k] = v
    client["updated_at"] = datetime.utcnow().isoformat()
    _save_db(db)
    return client


@app.delete("/clients/{client_id}", status_code=204)
def delete_client(client_id: str, user: dict = Depends(require_operator)):
    db     = _load_db()
    client = db["clients"].get(client_id)
    if not client or client.get("operator_id") != user["id"]:
        raise HTTPException(status_code=404, detail="Client not found")
    del db["clients"][client_id]
    _save_db(db)


@app.get("/clients/{client_id}/history")
def client_history(client_id: str, user: dict = Depends(get_current_user)):
    db     = _load_db()
    client = db["clients"].get(client_id)
    if not client:
        raise HTTPException(status_code=404, detail="Client not found")
    if user["role"] == "client" and user.get("client_id") != client_id:
        raise HTTPException(status_code=403, detail="Access denied")
    return [_job_summary(db["jobs"][jid]) for jid in client.get("job_ids", []) if jid in db["jobs"]]


@app.get("/clients/{client_id}/letters")
def client_letters(client_id: str, user: dict = Depends(get_current_user)):
    db     = _load_db()
    client = db["clients"].get(client_id)
    if not client:
        raise HTTPException(status_code=404, detail="Client not found")
    if user["role"] == "client" and user.get("client_id") != client_id:
        raise HTTPException(status_code=403, detail="Access denied")
    all_letters = []
    for jid in client.get("job_ids", []):
        job = db["jobs"].get(jid)
        if job and job.get("letter_files"):
            for lf in job["letter_files"]:
                all_letters.append({**lf, "job_id": jid, "report_date": job["report_date"]})
    return all_letters


# =============================================================================
# ERROR REPORTING
# =============================================================================

class ErrorReportBody(BaseModel):
    job_id:       str
    error_type:   str
    description:  str
    account_name: str = ""
    bureau:       str = ""


@app.post("/report-error", status_code=201)
def report_error(body: ErrorReportBody, user: dict = Depends(get_current_user)):
    db        = _load_db()
    ticket_id = f"ERR-{uuid.uuid4().hex[:8].upper()}"
    db["errors"].append({"ticket_id": ticket_id, "job_id": body.job_id, "error_type": body.error_type, "description": body.description, "account_name": body.account_name, "bureau": body.bureau, "reported_by": user["id"], "created_at": datetime.utcnow().isoformat(), "status": "open"})
    _save_db(db)
    return {"received": True, "ticket_id": ticket_id}


@app.get("/errors")
def list_errors(user: dict = Depends(require_operator)):
    return _load_db().get("errors", [])


# =============================================================================
# CLIENT PORTAL
# =============================================================================

@app.get("/portal/overview")
def portal_overview(user: dict = Depends(get_current_user)):
    if user["role"] != "client":
        raise HTTPException(status_code=403, detail="Client only")
    client_id = user.get("client_id")
    if not client_id:
        raise HTTPException(status_code=404, detail="No client record linked")
    db     = _load_db()
    client = db["clients"].get(client_id)
    if not client:
        raise HTTPException(status_code=404, detail="Client not found")
    jobs       = [db["jobs"][jid] for jid in client.get("job_ids", []) if jid in db["jobs"]]
    latest_job = max(jobs, key=lambda j: j["created_at"]) if jobs else None
    return {"client": {k: v for k, v in client.items() if k != "operator_id"}, "total_jobs": len(jobs), "latest_job": _job_summary(latest_job) if latest_job else None, "all_jobs": [_job_summary(j) for j in jobs]}


@app.get("/portal/letters")
def portal_letters(user: dict = Depends(get_current_user)):
    if user["role"] != "client":
        raise HTTPException(status_code=403, detail="Client only")
    db          = _load_db()
    client      = db["clients"].get(user.get("client_id"), {})
    all_letters = []
    for jid in client.get("job_ids", []):
        job = db["jobs"].get(jid)
        if job and job.get("letter_files"):
            for lf in job["letter_files"]:
                all_letters.append({**lf, "job_id": jid, "report_date": job["report_date"]})
    return all_letters


# =============================================================================
# HEALTH CHECK
# =============================================================================

@app.get("/health")
def health():
    return {"status": "ok", "version": "1.0.0", "service": "Report Defence API"}


# =============================================================================
# HELPERS
# =============================================================================

def _severity_label(attack_type: str) -> str:
    if attack_type in {"same_account_number_same_balance", "collector_original_creditor_pattern", "duplicate_account_number", "bankruptcy_included_still_active", "obsolete_account_7yr_limit"}: return "critical"
    if attack_type in {"cross_bureau_payment_history_date_conflict", "potential_re_aging", "dofd_unknown_verification_required", "absent_bureau_reporting_inconsistency", "cross_bureau_balance_conflict", "cross_bureau_account_status_conflict", "student_loan_duplicate_tradeline"}: return "high"
    if attack_type in {"late_payment_history_dispute", "cross_bureau_payment_status_conflict", "cross_bureau_credit_limit_conflict", "charge_off_balance_inflated", "paid_collection_still_derogatory"}: return "medium"
    return "low"

def _round_for(attack_type: str) -> str:
    return "round_2" if attack_type in {"same_account_number_same_balance", "collector_original_creditor_pattern", "collector_original_creditor_self_declared", "multi_furnisher_same_balance", "cross_bureau_balance_conflict", "cross_bureau_payment_status_conflict", "cross_bureau_furnisher_identity_shift", "cross_bureau_account_status_conflict"} else "round_1"

def _extract_scores(result: dict) -> dict:
    return {"transunion": 0, "experian": 0, "equifax": 0}

def _safe_slug(s: str) -> str:
    import re as _re
    return _re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")

def _serialize_letter_input(letter_input: dict) -> dict:
    result = {}
    for bureau, groups in letter_input.items():
        result[bureau] = {}
        for group, items in groups.items():
            result[bureau][group] = [{k: v for k, v in item.items() if isinstance(v, (str, int, float, bool, list, dict, type(None)))} for item in items]
    return result

def _client_summary(client: dict, db: dict) -> dict:
    jobs   = [j for j in [db["jobs"].get(jid) for jid in client.get("job_ids", [])] if j]
    latest = max(jobs, key=lambda j: j["created_at"]) if jobs else None
    return {"id": client["id"], "full_name": client["full_name"], "email": client.get("email", ""), "created_at": client["created_at"], "total_jobs": len(jobs), "last_report": latest["report_date"] if latest else None, "last_scores": latest["scores"] if latest else None, "attack_count": latest["attack_count"] if latest else 0, "has_letters": latest["letters_generated"] if latest else False, "status": _client_status(jobs)}

def _client_detail(client: dict, db: dict) -> dict:
    jobs   = [j for j in [db["jobs"].get(jid) for jid in client.get("job_ids", [])] if j]
    latest = max(jobs, key=lambda j: j["created_at"]) if jobs else None
    return {**client, "total_jobs": len(jobs), "latest_job": _job_summary(latest) if latest else None, "all_jobs": [_job_summary(j) for j in sorted(jobs, key=lambda j: j["created_at"], reverse=True)]}

def _job_summary(job: dict | None) -> dict | None:
    if not job: return None
    return {"job_id": job["job_id"], "report_date": job["report_date"], "source": job["source"], "created_at": job["created_at"], "scores": job["scores"], "attack_count": job["attack_count"], "letters_generated": job["letters_generated"], "letter_count": len(job.get("letter_files", [])), "response_history": job.get("response_history", [])}

def _client_status(jobs: list) -> str:
    if not jobs: return "new"
    latest = max(jobs, key=lambda j: j["created_at"])
    if not latest["letters_generated"]: return "analyzed"
    if latest.get("response_history"): return "response_received"
    return "letters_sent"
