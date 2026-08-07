"""
REPORT DEFENCE — FastAPI backend (Supabase edition)
====================================================

Required env vars in Railway:
  SUPABASE_URL          — e.g. https://ivtigtxdesfjbuzxqohe.supabase.co
  SUPABASE_SERVICE_KEY  — service_role key (NOT anon key)
  ADMIN_PASSWORD        — password for the initial admin user
  JWT_SECRET            — secret for signing JWT tokens
"""

import os, uuid, json, hashlib, hmac, time, traceback, shutil, re
from datetime import datetime, timedelta, timezone
from typing import Optional, List
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Depends, UploadFile, File, Form, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel
import jwt as pyjwt
from supabase import create_client, Client

# ─── Shared detection helper ──────────────────────────────────────────────
# Runs the SAME detection chain as original_parser.build_report on a plain
# negatives_by_bureau dict, returning a real letter_input_engine with proper
# attack_types + secondary_flags. The IdentityIQ connector intake paths and
# the /generate-letters rebuild gate previously produced only the placeholder
# attack_type="requires_basic_verification", which forced every account into
# the generic dispute paragraph. This restores the specialized branches
# (collector_original_creditor_self_declared, closed_with_balance, dofd, etc.).
def _compute_letter_input(negatives_by_bureau: dict, report_date: str = "") -> dict:
    from original_parser import (
        build_dofd_engine, build_legal_detection_engine,
        build_attack_scoring_engine, build_strategy_engine,
        build_letter_input_engine,
    )
    enriched = build_dofd_engine(negatives_by_bureau or {}, report_date or "")
    lde      = build_legal_detection_engine(
        enriched, None, report_date=report_date or "", client_state=""
    )
    strat    = build_strategy_engine(build_attack_scoring_engine(lde))
    return build_letter_input_engine(strat, enriched)


def _serialize_letter_input(lie: dict) -> dict:
    """Keep only JSON-serializable values so the structure can be stored."""
    out = {}
    for b, groups in (lie or {}).items():
        out[b] = {}
        for grp, items in groups.items():
            out[b][grp] = [
                {k: v for k, v in item.items()
                 if isinstance(v, (str, int, float, bool, list, dict, type(None)))}
                for item in items
            ]
    return out


def _resolve_letter_input(result: dict, negatives: dict) -> dict:
    """Prefer the letter_input_engine the connector already built (it runs the
    full original_parser pipeline, including a base_tradeline_engine so
    cross-bureau attacks fire). Only if that came back empty (connector hit its
    own except branch) do we recompute from negatives as a backstop."""
    lie = result.get("letter_input_engine") or {}
    has_items = any(
        len(items) > 0 for groups in lie.values() for items in groups.values()
    )
    if has_items:
        return _serialize_letter_input(lie)
    return _serialize_letter_input(
        _compute_letter_input(negatives, result.get("report_date", ""))
    )


# ─── Rate Limiting ────────────────────────────────────────────
# In-memory store — resets on deploy, sufficient for brute force protection.
# Key: sha256(endpoint:ip:email) → list of Unix timestamps
_rl_store: dict[str, list[float]] = {}

def _rl_key(endpoint: str, ip: str, email: str) -> str:
    """Deterministic key combining endpoint + IP + email."""
    raw = f"{endpoint}:{ip}:{email.lower().strip()}"
    return hashlib.sha256(raw.encode()).hexdigest()

def _get_client_ip(request: Request) -> str:
    """Extract real client IP, handling Railway/proxy X-Forwarded-For."""
    xff = request.headers.get("X-Forwarded-For", "")
    if xff:
        return xff.split(",")[0].strip()
    return request.client.host if request.client else "unknown"

def check_rate_limit(
    request: Request,
    endpoint: str,
    email: str = "",
    max_requests: int = 5,
    window_seconds: int = 900,  # 15 minutes default
) -> None:
    """
    Raise HTTP 429 if the (endpoint, ip, email) key exceeds max_requests
    within the rolling window_seconds window.

    Fails open: if anything goes wrong internally, the request is allowed.
    """
    try:
        ip  = _get_client_ip(request)
        key = _rl_key(endpoint, ip, email) if email else _rl_key(endpoint, ip, ip)
        now = time.time()

        hits = _rl_store.get(key, [])
        # Evict hits outside the rolling window
        hits = [t for t in hits if now - t < window_seconds]

        if len(hits) >= max_requests:
            retry_after = int(window_seconds - (now - hits[0]))
            raise HTTPException(
                status_code=429,
                detail=f"Too many requests. Please wait {retry_after // 60 + 1} minute(s) before trying again.",
                headers={"Retry-After": str(retry_after)},
            )

        hits.append(now)
        _rl_store[key] = hits
    except HTTPException:
        raise
    except Exception:
        pass  # fail open — never block legitimate users due to internal errors


# ─── Environment ──────────────────────────────────────────────
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
JWT_SECRET   = os.environ.get("JWT_SECRET", "change-me-in-production")
ADMIN_EMAIL  = "artugz16996@gmail.com"
ADMIN_PASS   = os.environ.get("ADMIN_PASSWORD", "")

UPLOAD_DIR = "/tmp/rd_uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

# ─── Supabase client (service role — bypasses RLS) ────────────
sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ─── Helpers ──────────────────────────────────────────────────

def hash_password(pw: str) -> str:
    return hashlib.sha256(pw.encode()).hexdigest()

def verify_password(pw: str, hashed: str) -> bool:
    return hmac.compare_digest(hash_password(pw), hashed)

def create_token(user_id: str, role: str) -> str:
    return pyjwt.encode(
        {"sub": user_id, "role": role, "exp": datetime.now(timezone.utc) + timedelta(days=7)},
        JWT_SECRET, algorithm="HS256",
    )

def decode_token(token: str) -> dict:
    return pyjwt.decode(token, JWT_SECRET, algorithms=["HS256"])

# ─── Auth dependency ─────────────────────────────────────────

async def get_current_user(request: Request):
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(401, "Missing token")
    try:
        payload = decode_token(auth[7:])
    except Exception:
        raise HTTPException(401, "Invalid token")
    res = sb.table("api_users").select("*").eq("id", payload["sub"]).execute()
    if not res.data or len(res.data) == 0:
        raise HTTPException(401, "User not found")
    return res.data[0]

# ─── Startup: ensure admin exists ─────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    if ADMIN_PASS:
        existing = sb.table("api_users").select("id").eq("email", ADMIN_EMAIL).execute()
        if not existing.data or len(existing.data) == 0:
            sb.table("api_users").insert({
                "email": ADMIN_EMAIL,
                "full_name": "Arturo",
                "hashed_password": hash_password(ADMIN_PASS),
                "role": "operator",
                "auth_provider": "email",
            }).execute()
            print(f"✅ Admin user created: {ADMIN_EMAIL}")
        else:
            print(f"ℹ️  Admin user already exists: {ADMIN_EMAIL}")
    yield

app = FastAPI(title="Report Defence API", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ═══════════════════════════════════════════════════════════════
#  AUTH ROUTES
# ═══════════════════════════════════════════════════════════════

class RegisterBody(BaseModel):
    full_name: str
    email: str
    password: str
    role: str = "client"
    operator_code: Optional[str] = None

class LoginBody(BaseModel):
    email: str
    password: str

class SupabaseAuthBody(BaseModel):
    access_token: str
    email: str
    full_name: str

class UpgradeRoleBody(BaseModel):
    operator_code: str

OPERATOR_CODE = os.environ.get("OPERATOR_CODE", "RD-OPERATOR-2024")

def user_response(u: dict):
    return {
        "id": u["id"],
        "full_name": u["full_name"],
        "email": u["email"],
        "role": u["role"],
    }


@app.get("/debug/chromium")
async def debug_chromium():
    """Temporary endpoint to find Chromium path on Railway."""
    import subprocess, shutil, glob, os
    results = {}
    
    # which commands
    for cmd in ["chromium", "chromium-browser", "google-chrome", "google-chrome-stable"]:
        path = shutil.which(cmd)
        results[f"which_{cmd}"] = path
    
    # glob search in common nix paths
    nix_paths = glob.glob("/nix/store/*/bin/chromium*")
    results["nix_glob"] = nix_paths[:5]
    
    # find chromium anywhere
    for search_path in ["/nix", "/usr", "/opt", "/snap", "/home"]:
        try:
            out = subprocess.check_output(
                ["find", search_path, "-name", "chromium*", "-type", "f"],
                timeout=10, stderr=subprocess.DEVNULL
            ).decode()
            results[f"find_{search_path}"] = out.strip().split("\n")[:5]
        except Exception as e:
            results[f"find_{search_path}"] = str(e)
    
    # check playwright cache
    pw_cache = glob.glob("/root/.cache/ms-playwright/*/chrome*/chrome*")
    results["playwright_cache"] = pw_cache[:5]
    
    # check /app/.venv playwright
    venv_pw = glob.glob("/app/.venv/lib/*/site-packages/playwright/driver/*")
    results["venv_playwright"] = venv_pw[:5]
    
    # check PATH
    results["PATH"] = os.environ.get("PATH", "")
    
    # ls /usr/bin chromium
    try:
        out = subprocess.check_output(["ls", "/usr/bin/"], timeout=5).decode()
        chrome_bins = [x for x in out.split() if "chrom" in x.lower()]
        results["usr_bin_chrome"] = chrome_bins
    except Exception as e:
        results["usr_bin_chrome"] = str(e)

    return results

@app.post("/auth/register")
async def register(body: RegisterBody, request: Request):
    # Rate limit: 5 attempts per 15 minutes per (IP + email)
    check_rate_limit(request, "auth/register", email=body.email, max_requests=5, window_seconds=900)
    existing = sb.table("api_users").select("id").eq("email", body.email).execute()
    if existing.data and len(existing.data) > 0:
        raise HTTPException(409, "Email already registered")
    # Only admin email or explicit operator_code + operator role can be operator
    if body.email == ADMIN_EMAIL:
        role = "operator"
    elif body.operator_code == OPERATOR_CODE and body.role == "operator":
        role = "operator"
    else:
        role = "client"
    res = sb.table("api_users").insert({
        "email": body.email,
        "full_name": body.full_name,
        "hashed_password": hash_password(body.password),
        "role": role,
        "auth_provider": "email",
    }).execute()
    u = res.data[0]
    return {"access_token": create_token(u["id"], u["role"]), "user": user_response(u)}

@app.post("/auth/login")
async def login(body: LoginBody, request: Request):
    # Rate limit: 5 attempts per 15 minutes per (IP + email)
    check_rate_limit(request, "auth/login", email=body.email, max_requests=5, window_seconds=900)
    res = sb.table("api_users").select("*").eq("email", body.email).execute()
    if not res.data or len(res.data) == 0 or not verify_password(body.password, res.data[0]["hashed_password"]):
        raise HTTPException(401, "Invalid credentials")
    u = res.data[0]
    return {"access_token": create_token(u["id"], u["role"]), "user": user_response(u)}

@app.post("/auth/supabase")
async def auth_supabase(body: SupabaseAuthBody):
    existing = sb.table("api_users").select("*").eq("email", body.email).execute()
    if existing.data and len(existing.data) > 0:
        u = existing.data[0]
    else:
        res = sb.table("api_users").insert({
            "email": body.email,
            "full_name": body.full_name,
            "hashed_password": hash_password(str(uuid.uuid4())),
            "role": "client",
            "auth_provider": "google",
        }).execute()
        u = res.data[0]
    token = create_token(u["id"], u["role"])
    return {"access_token": token, "token": token, "user": user_response(u)}

@app.get("/auth/me")
async def auth_me(user=Depends(get_current_user)):
    return {
        **user_response(user),
        "created_at": user["created_at"],
        "client_ids": user.get("client_ids", []),
        "auth_provider": user.get("auth_provider", "email"),
    }

@app.patch("/auth/upgrade-role")
async def upgrade_role(body: UpgradeRoleBody, user=Depends(get_current_user)):
    if body.operator_code != OPERATOR_CODE:
        raise HTTPException(403, "Invalid operator code")
    sb.table("api_users").update({"role": "operator"}).eq("id", user["id"]).execute()
    user["role"] = "operator"
    return {"access_token": create_token(user["id"], "operator"), "token": create_token(user["id"], "operator"), "user": user_response(user)}

# ═══════════════════════════════════════════════════════════════
#  CLIENT ROUTES
# ═══════════════════════════════════════════════════════════════

class ClientCreate(BaseModel):
    full_name: str
    email: str = ""
    phone: str = ""
    address: str = ""
    city: str = ""
    state: str = ""
    zip_code: str = ""
    date_of_birth: str = ""
    notes: str = ""

class ClientUpdate(BaseModel):
    full_name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip_code: Optional[str] = None
    date_of_birth: Optional[str] = None
    notes: Optional[str] = None

def enrich_client(c: dict) -> dict:
    """Add computed fields to a client dict."""
    job_ids = c.get("job_ids") or []
    jobs = []
    if job_ids:
        res = sb.table("api_jobs").select("*").in_("job_id", job_ids).execute()
        jobs = res.data or []
    latest = max(jobs, key=lambda j: j["created_at"], default=None) if jobs else None
    total_attacks = sum(j.get("attack_count", 0) for j in jobs)
    has_letters = any(j.get("letters_generated") for j in jobs)
    latest_job_summary = {
        "job_id": latest["job_id"],
        "report_date": latest.get("report_date", ""),
        "source": latest.get("source", ""),
        "created_at": latest["created_at"],
        "scores": latest.get("scores", {}),
        "attack_count": latest.get("attack_count", 0),
        "letters_generated": latest.get("letters_generated", False),
    } if latest else None
    return {
        "id": c["id"],
        "full_name": c["full_name"],
        "email": c.get("email", ""),
        "created_at": c["created_at"],
        "total_jobs": len(jobs),
        "last_report": latest.get("report_date", "") if latest else "",
        "last_scores": latest.get("scores", {}) if latest else None,
        "latest_job": latest_job_summary,
        "attack_count": total_attacks,
        "has_letters": has_letters,
        "status": "analyzed" if jobs else "active",
    }

@app.get("/clients")
async def list_clients(user=Depends(get_current_user)):
    # Operators see all clients, clients see only their own
    if user.get("role") == "operator":
        res = sb.table("api_clients").select("*").order("created_at", desc=True).execute()
    else:
        res = sb.table("api_clients").select("*").eq("operator_id", user["id"]).order("created_at", desc=True).execute()
    return [enrich_client(c) for c in (res.data or [])]

@app.post("/clients", status_code=201)
async def create_client(body: ClientCreate, user=Depends(get_current_user)):
    data = body.model_dump()
    data["operator_id"] = user["id"]
    res = sb.table("api_clients").insert(data).execute()
    c = res.data[0]
    return {**c, "job_ids": c.get("job_ids", []), "user_id": c.get("user_id")}

@app.get("/clients/{client_id}")
async def get_client(client_id: str, user=Depends(get_current_user)):
    res = sb.table("api_clients").select("*").eq("id", client_id).execute()
    if not res.data or len(res.data) == 0:
        raise HTTPException(404, "Client not found")
    c = res.data[0]
    job_ids = c.get("job_ids") or []
    jobs = []
    if job_ids:
        jr = sb.table("api_jobs").select("*").in_("job_id", job_ids).execute()
        jobs = jr.data or []
    jobs.sort(key=lambda j: j["created_at"], reverse=True)
    latest = jobs[0] if jobs else None
    all_jobs_summary = [{
        "job_id": j["job_id"],
        "report_date": j.get("report_date", ""),
        "source": j.get("source", ""),
        "created_at": j["created_at"],
        "scores": j.get("scores", {}),
        "attack_count": j.get("attack_count", 0),
        "letters_generated": j.get("letters_generated", False),
        "letter_count": j.get("letter_count", 0),
        "response_history": j.get("response_history", []),
    } for j in jobs]
    return {
        **c,
        "total_jobs": len(jobs),
        "latest_job": all_jobs_summary[0] if all_jobs_summary else None,
        "all_jobs": all_jobs_summary,
    }

@app.patch("/clients/{client_id}")
async def update_client(client_id: str, body: ClientUpdate, user=Depends(get_current_user)):
    updates = {k: v for k, v in body.model_dump().items() if v is not None}
    if not updates:
        raise HTTPException(400, "No fields to update")
    sb.table("api_clients").update(updates).eq("id", client_id).execute()
    return await get_client(client_id, user)

@app.delete("/clients/{client_id}")
async def delete_client(client_id: str, user=Depends(get_current_user)):
    sb.table("api_clients").delete().eq("id", client_id).eq("operator_id", user["id"]).execute()
    return {"ok": True}

@app.get("/clients/{client_id}/history")
async def client_history(client_id: str, user=Depends(get_current_user)):
    res = sb.table("api_clients").select("job_ids").eq("id", client_id).execute()
    if not res.data or len(res.data) == 0:
        raise HTTPException(404, "Client not found")
    job_ids = res.data[0].get("job_ids") or []
    if not job_ids:
        return []
    jr = sb.table("api_jobs").select("*").in_("job_id", job_ids).order("created_at", desc=True).execute()
    return [{
        "job_id": j["job_id"],
        "report_date": j.get("report_date", ""),
        "source": j.get("source", ""),
        "created_at": j["created_at"],
        "scores": j.get("scores", {}),
        "attack_count": j.get("attack_count", 0),
        "letters_generated": j.get("letters_generated", False),
        "letter_count": j.get("letter_count", 0),
        "response_history": j.get("response_history", []),
    } for j in (jr.data or [])]

@app.get("/clients/{client_id}/letters")
async def client_letters(client_id: str, user=Depends(get_current_user)):
    res = sb.table("api_clients").select("job_ids").eq("id", client_id).execute()
    if not res.data or len(res.data) == 0:
        return []
    job_ids = res.data[0].get("job_ids") or []
    if not job_ids:
        return []
    jr = sb.table("api_jobs").select("job_id, letter_files, letters_generated").in_("job_id", job_ids).execute()
    letters = []
    for j in (jr.data or []):
        for lf in (j.get("letter_files") or []):
            letters.append({**lf, "job_id": j["job_id"]})
    return letters

# ═══════════════════════════════════════════════════════════════
#  JOB / REPORT ROUTES
# ═══════════════════════════════════════════════════════════════

@app.get("/jobs/{job_id}")
async def get_job(job_id: str, user=Depends(get_current_user)):
    res = sb.table("api_jobs").select("*").eq("job_id", job_id).execute()
    if not res.data or len(res.data) == 0:
        raise HTTPException(404, "Job not found")
    return res.data[0]

@app.post("/upload-report")
async def upload_report(
    file: UploadFile = File(...),
    consumer_name: str = Form(...),
    client_id: str = Form(...),
    source: str = Form("identityiq"),
    user=Depends(get_current_user),
):
    job_id = str(uuid.uuid4())
    pdf_path = os.path.join(UPLOAD_DIR, f"{job_id}.pdf")
    with open(pdf_path, "wb") as f:
        shutil.copyfileobj(file.file, f)

    # ── Run parser ──
    from original_parser import build_report
    result = build_report(pdf_path)

    scores = result.get("scores", {"transunion": 0, "experian": 0, "equifax": 0})

    # Build attacks flat list with severity
    scoring = result.get("attack_scoring_engine", {})
    attacks = []
    for bureau, bureau_attacks in scoring.items():
        for atk in bureau_attacks:
            for acc in atk.get("accounts", []):
                attacks.append({
                    "attack_type":    atk.get("attack_type", ""),
                    "bureau":         bureau,
                    "severity":       atk.get("priority", "medium"),
                    "reason":         atk.get("reason", ""),
                    "account_name":   acc.get("name", ""),
                    "account_number": acc.get("account_number", ""),
                })

    negatives = result.get("negatives_by_bureau", {})
    attack_count = len(attacks)

    # Build inventory with all fields including new ones
    inventory_out = {}
    for bureau, accts in result.get("inventory_by_bureau", {}).items():
        inventory_out[bureau] = [
            {
                "name":                 a.get("name", ""),
                "account_number":       a.get("account_number", ""),
                "account_type":         a.get("account_type", ""),
                "account_type_detail":  a.get("account_type_detail", ""),
                "bureau_code":          a.get("bureau_code", ""),
                "status":               a.get("status", ""),
                "monthly_payment":      a.get("monthly_payment", ""),
                "payment_status":       a.get("payment_status", ""),
                "balance":              a.get("balance", ""),
                "no_of_months":         a.get("no_of_months", ""),
                "high_credit":          a.get("high_credit", ""),
                "credit_limit":         a.get("credit_limit", ""),
                "past_due":             a.get("past_due", ""),
                "date_opened":          a.get("date_opened", ""),
                "date_last_active":     a.get("date_last_active", ""),
                "date_of_last_payment": a.get("date_of_last_payment", ""),
                "last_reported":        a.get("last_reported", ""),
                "comments":             a.get("comments", ""),
                "late_payment_codes":   a.get("late_payment_codes", []),
                "payment_history":      a.get("payment_history", []),
                "has_30_in_history":    a.get("has_30_in_history", False),
                "has_60_in_history":    a.get("has_60_in_history", False),
                "has_90_in_history":    a.get("has_90_in_history", False),
                "has_co_in_history":    a.get("has_co_in_history", False),
                "block_id":             a.get("block_id", ""),
                "possible_duplicate_group": a.get("possible_duplicate_group", ""),
            }
            for a in accts
        ]

    # Serialize letter_input_engine
    letters_in = result.get("letter_input_engine", {})
    letter_input_serialized = {}
    for b, groups in letters_in.items():
        letter_input_serialized[b] = {}
        for grp, items in groups.items():
            letter_input_serialized[b][grp] = [
                {k: v for k, v in item.items()
                 if isinstance(v, (str, int, float, bool, list, dict, type(None)))}
                for item in items
            ]

    job_data = {
        "job_id": job_id,
        "client_id": client_id,
        "operator_id": user["id"],
        "consumer_name": consumer_name,
        "source": source,
        "report_date": result.get("report_date", ""),
        "pdf_path": pdf_path,
        "scores": scores,
        "attack_count": attack_count,
        "letters_generated": False,
        "letter_files": [],
        "negatives_by_bureau": negatives,
        "inventory_by_bureau": inventory_out,
        "personal_info": result.get("personal_info", {}),
        "personal_info_issues": result.get("personal_info_issues", []),
        "letter_input_engine": letter_input_serialized,
        "attacks": attacks,
        "inquiries": result.get("inquiries", []),
        "inquiry_attacks": result.get("inquiry_attacks", []),
        "response_history": [],
    }
    sb.table("api_jobs").insert(job_data).execute()

    # Update client's job_ids array
    client_res = sb.table("api_clients").select("job_ids").eq("id", client_id).execute()
    if client_res.data and len(client_res.data) > 0:
        current_ids = client_res.data[0].get("job_ids") or []
        current_ids.append(job_id)
        sb.table("api_clients").update({"job_ids": current_ids}).eq("id", client_id).execute()

    return {
        "job_id": job_id,
        "consumer_name": consumer_name,
        "report_date": result.get("report_date", ""),
        "source": source,
        "scores": scores,
        "negatives_by_bureau": negatives,
        "attack_count": attack_count,
        "attacks": attacks,
        "letter_groups": letter_input_serialized,
        "personal_info_issues": result.get("personal_info_issues", []),
        "inventory_by_bureau": inventory_out,
        "inquiries": result.get("inquiries", []),
        "inquiry_attacks": result.get("inquiry_attacks", []),
    }


# ═══════════════════════════════════════════════════════════════
#  IDENTITYIQ DIRECT CONNECT
# ═══════════════════════════════════════════════════════════════

class ConnectIdentityIQBody(BaseModel):
    client_id: str
    username: str
    password: str
    ssn_last4: str

@app.post("/connect-identityiq")
async def connect_identityiq(body: ConnectIdentityIQBody, user=Depends(get_current_user)):
    """
    Pull credit report directly from IdentityIQ using client credentials.
    Authenticates, fetches the JSON report, parses it, and stores the job
    in the same format as /upload-report.
    """
    import asyncio
    from functools import partial

    # Validate client exists
    client_res = sb.table("api_clients").select("*").eq("id", body.client_id).execute()
    if not client_res.data:
        raise HTTPException(404, "Client not found")
    client_data = client_res.data[0]
    consumer_name = client_data.get("name", "")

    job_id = str(uuid.uuid4())

    # Store pending job
    sb.table("api_jobs").insert({
        "job_id":      job_id,
        "client_id":   body.client_id,
        "operator_id": user["id"],
        "consumer_name": consumer_name,
        "source":      "identityiq_json",
        "status":      "pending",
        "error":       None,
    }).execute()

    # Run in background
    async def _run():
        try:
            print(f"[connect-identityiq] Starting job={job_id} user={body.username}")
            from identityiq_playwright import pull_and_parse as pw_pull_and_parse
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                partial(pw_pull_and_parse, body.username, body.password, body.ssn_last4)
            )

            scores = result.get("scores", {})
            negatives = result.get("negatives_by_bureau", {})
            attack_count = result.get("attack_count", 0)

            # Build inventory_out (same format as upload-report)
            inventory_out = {}
            for bureau, accts in result.get("inventory_by_bureau", {}).items():
                inventory_out[bureau] = [
                    {
                        "block_id":             a.get("block_id", ""),
                        "possible_duplicate_group": a.get("possible_duplicate_group", ""),
                        "name":                 a.get("name", ""),
                        "account_number":       a.get("account_number", ""),
                        "account_type":         a.get("account_type", ""),
                        "account_type_detail":  a.get("account_type_detail", ""),
                        "bureau_code":          a.get("bureau_code", ""),
                        "status":               a.get("status", ""),
                        "monthly_payment":      a.get("monthly_payment", ""),
                        "payment_status":       a.get("payment_status", ""),
                        "balance":              a.get("balance", ""),
                        "no_of_months":         a.get("no_of_months", ""),
                        "high_credit":          a.get("high_credit", ""),
                        "credit_limit":         a.get("credit_limit", ""),
                        "past_due":             a.get("past_due", ""),
                        "date_opened":          a.get("date_opened", ""),
                        "date_last_active":     a.get("date_last_active", ""),
                        "date_of_last_payment": a.get("date_of_last_payment", ""),
                        "last_reported":        a.get("last_reported", ""),
                        "comments":             a.get("comments", ""),
                        "late_payment_codes":   a.get("late_payment_codes", []),
                        "payment_history":      a.get("payment_history", []),
                        "has_30_in_history":    a.get("has_30_in_history", False),
                        "has_60_in_history":    a.get("has_60_in_history", False),
                        "has_90_in_history":    a.get("has_90_in_history", False),
                        "has_co_in_history":    a.get("has_co_in_history", False),
                    }
                    for a in accts
                ]

            # Serialize letter_input_engine
            letters_in = result.get("letter_input_engine", {})
            letter_input_serialized = {}
            for b, groups in letters_in.items():
                letter_input_serialized[b] = {}
                for grp, items in groups.items():
                    letter_input_serialized[b][grp] = [
                        {k: v for k, v in item.items()
                         if isinstance(v, (str, int, float, bool, list, dict, type(None)))}
                        for item in (items if isinstance(items, list) else [])
                    ]

            # Update job as completed
            sb.table("api_jobs").update({
                "status":              "completed",
                "scores":              scores,
                "attack_count":        attack_count,
                "negatives_by_bureau": negatives,
                "inventory_by_bureau": inventory_out,
                "personal_info":       result.get("personal_info", {}),
                "personal_info_issues": result.get("personal_info_issues", []),
                "attacks":             result.get("attacks", []),
                "inquiries":           result.get("inquiries", []),
                "inquiry_attacks":     result.get("inquiry_attacks", []),
                "letter_input_engine": letter_input_serialized,
                "letters_generated":   False,
                "letter_files":        [],
                "response_history":    [],
                "report_date":         result.get("report_date", ""),
                "source":              "identityiq_json",
            }).eq("job_id", job_id).execute()

            print(f"[connect-identityiq] Completed job={job_id} attacks={attack_count}")
            # Add job to client
            client_res2 = sb.table("api_clients").select("job_ids").eq("id", body.client_id).execute()
            if client_res2.data:
                current_ids = client_res2.data[0].get("job_ids") or []
                current_ids.append(job_id)
                sb.table("api_clients").update({"job_ids": current_ids}).eq("id", body.client_id).execute()

        except Exception as e:
            import traceback
            err_detail = traceback.format_exc()
            print(f"[connect-identityiq] FAILED job={job_id}: {e}\n{err_detail}")
            sb.table("api_jobs").update({
                "status": "failed",
                "error":  f"{str(e)}\n\n{err_detail}",
            }).eq("job_id", job_id).execute()

    asyncio.create_task(_run())

    return {
        "job_id":        job_id,
        "consumer_name": consumer_name,
        "source":        "identityiq_json",
        "status":        "pending",
    }


# ═══════════════════════════════════════════════════════════════
#  IDENTITYIQ JSON PARSE (frontend fetches JSON, backend parses)
# ═══════════════════════════════════════════════════════════════

class ParseIdentityIQBody(BaseModel):
    client_id: str
    raw_json: str  # The JSONP string fetched by the browser

@app.post("/parse-identityiq-json")
async def parse_identityiq_json_endpoint(body: ParseIdentityIQBody, user=Depends(get_current_user)):
    """
    Receives the raw JSONP string fetched by the browser from IdentityIQ,
    parses it, and stores the job — same format as /upload-report.
    
    This avoids the Imperva WAF that blocks server-side requests.
    The browser fetches the JSON (passes WAF), sends it here for parsing.
    """
    import asyncio
    from functools import partial

    # Validate client
    client_res = sb.table("api_clients").select("*").eq("id", body.client_id).execute()
    if not client_res.data:
        raise HTTPException(404, "Client not found")
    client_data  = client_res.data[0]
    consumer_name = client_data.get("name", "")

    job_id = str(uuid.uuid4())

    # Store pending job immediately
    sb.table("api_jobs").insert({
        "job_id":       job_id,
        "client_id":    body.client_id,
        "operator_id":  user["id"],
        "consumer_name": consumer_name,
        "source":       "identityiq_json",
        "status":       "pending",
        "error":        None,
    }).execute()

    async def _run():
        try:
            print(f"[parse-identityiq-json] Starting job={job_id} client={body.client_id}")
            from identityiq_connector import parse_identityiq_json
            import json as _json

            # Strip JSONP wrapper if present
            raw = body.raw_json.strip()
            if raw.startswith("JSON_CALLBACK("):
                raw = raw[len("JSON_CALLBACK("):]
                if raw.endswith(")"):
                    raw = raw[:-1]
            elif raw.startswith("(") and raw.endswith(")"):
                raw = raw[1:-1]

            if not raw:
                raise ValueError("Empty JSON received from IdentityIQ. The browser fetch may have failed.")

            data   = _json.loads(raw)
            result = parse_identityiq_json(data)

            scores       = result.get("scores", {})
            negatives    = result.get("negatives_by_bureau", {})
            attack_count = result.get("attack_count", 0)

            # Build inventory_out
            inventory_out = {}
            for bureau, accts in result.get("inventory_by_bureau", {}).items():
                inventory_out[bureau] = [
                    {
                        "block_id":             a.get("block_id", ""),
                        "possible_duplicate_group": a.get("possible_duplicate_group", ""),
                        "name":                 a.get("name", ""),
                        "account_number":       a.get("account_number", ""),
                        "account_type":         a.get("account_type", ""),
                        "account_type_detail":  a.get("account_type_detail", ""),
                        "bureau_code":          a.get("bureau_code", ""),
                        "status":               a.get("status", ""),
                        "monthly_payment":      a.get("monthly_payment", ""),
                        "payment_status":       a.get("payment_status", ""),
                        "balance":              a.get("balance", ""),
                        "no_of_months":         a.get("no_of_months", ""),
                        "high_credit":          a.get("high_credit", ""),
                        "credit_limit":         a.get("credit_limit", ""),
                        "past_due":             a.get("past_due", ""),
                        "date_opened":          a.get("date_opened", ""),
                        "date_last_active":     a.get("date_last_active", ""),
                        "date_of_last_payment": a.get("date_of_last_payment", ""),
                        "last_reported":        a.get("last_reported", ""),
                        "comments":             a.get("comments", ""),
                        "late_payment_codes":   a.get("late_payment_codes", []),
                        "payment_history":      a.get("payment_history", []),
                        "has_30_in_history":    a.get("has_30_in_history", False),
                        "has_60_in_history":    a.get("has_60_in_history", False),
                        "has_90_in_history":    a.get("has_90_in_history", False),
                        "has_co_in_history":    a.get("has_co_in_history", False),
                    }
                    for a in accts
                ]

            # Update job as completed
            sb.table("api_jobs").update({
                "status":              "completed",
                "scores":              scores,
                "attack_count":        attack_count,
                "negatives_by_bureau": negatives,
                "inventory_by_bureau": inventory_out,
                "personal_info":       result.get("personal_info", {}),
                "personal_info_issues": result.get("personal_info_issues", []),
                "attacks":             result.get("attacks", []),
                "inquiries":           result.get("inquiries", []),
                "inquiry_attacks":     result.get("inquiry_attacks", []),
                "letter_input_engine": _resolve_letter_input(result, negatives),
                "letters_generated":   False,
                "letter_files":        [],
                "response_history":    [],
                "report_date":         result.get("report_date", ""),
                "source":              "identityiq_json",
            }).eq("job_id", job_id).execute()

            # Add job to client
            client_res2 = sb.table("api_clients").select("job_ids").eq("id", body.client_id).execute()
            if client_res2.data:
                current_ids = client_res2.data[0].get("job_ids") or []
                current_ids.append(job_id)
                sb.table("api_clients").update({"job_ids": current_ids}).eq("id", body.client_id).execute()

            print(f"[parse-identityiq-json] Completed job={job_id} attacks={attack_count}")

        except Exception as e:
            import traceback
            err_detail = traceback.format_exc()
            print(f"[parse-identityiq-json] FAILED job={job_id}: {e}")
            sb.table("api_jobs").update({
                "status": "failed",
                "error":  f"{str(e)}\n\n{err_detail}",
            }).eq("job_id", job_id).execute()

    asyncio.create_task(_run())

    return {
        "job_id":        job_id,
        "consumer_name": consumer_name,
        "source":        "identityiq_json",
        "status":        "pending",
    }

# ═══════════════════════════════════════════════════════════════
#  LETTER GENERATION
# ═══════════════════════════════════════════════════════════════

class GenerateLettersBody(BaseModel):
    job_id: str
    consumer_name: str
    bureau: Optional[str] = None
    category: Optional[str] = None
    round: Optional[str] = "round_1"
    selected_accounts: Optional[list] = None
    variation_seed: Optional[int] = 0  # increment on each Regenerate press
    bureau_response_text: Optional[str] = None  # paste bureau's investigation response for R2/R3

@app.post("/generate-letters")
async def generate_letters(body: GenerateLettersBody, user=Depends(get_current_user)):
    res = sb.table("api_jobs").select("*").eq("job_id", body.job_id).execute()
    if not res.data or len(res.data) == 0:
        raise HTTPException(404, "Job not found")
    job = res.data[0]

    from original_parser import build_dispute_letter_engine

    letter_input = job.get("letter_input_engine", {})
    consumer_name = body.consumer_name
    report_date = job.get("report_date", "")

    # ── Migrate legacy jobs that stored "collections_chargeoffs" as a single key ──
    # Jobs uploaded before the split (collections / charge_offs) have the old key.
    # We split them in-memory so the engine always receives the correct structure.
    for _bureau, _groups in letter_input.items():
        if "collections_chargeoffs" in _groups:
            old_items = _groups.pop("collections_chargeoffs")
            _groups.setdefault("collections", [])
            _groups.setdefault("charge_offs", [])
            for _item in old_items:
                neg = _item.get("negative_type", "")
                if neg in ("charge_off", "charge_off_deficiency"):
                    _groups["charge_offs"].append(_item)
                else:
                    _groups["collections"].append(_item)

    # ── Helpers ──────────────────────────────────────────────────────────────
    import re as _re

    def _norm_acct(s: str) -> str:
        """Normalize account number: lowercase, collapse any run of X/x/* into *."""
        return _re.sub(r"[xX*]+", "*", (s or "").strip().lower())

    cat_map = {
        "Collections":               "collections",
        "Charge Offs":               "charge_offs",
        "Late Payments":             "late_payments",
        "Repossessions":             "repossessions",
        "Bankruptcies":              "bankruptcies",
        "Child Support":             "child_support",
        "Other Derogatory":          "other_derogatory",
        # Legacy labels kept for backward compatibility
        "Collections & Chargeoffs":  "collections",
        "Collections & Charge Offs": "collections",
    }

    def _negative_type_from_account(acc: dict) -> str:
        """Classify a negative account into a letter-engine category key."""
        status  = acc.get("status", "").lower()
        payment = acc.get("payment_status", "").lower()
        name    = acc.get("name", "").lower()
        debt_buyers = ["lvnv", "midland", "portfolio", "cavalry", "resurgent",
                       "aldous", "jefferson", "asset acceptance", "springoak",
                       "jeffcapsys", "jeffersncp"]
        if any(k in name for k in debt_buyers):
            return "collections"
        if "collection" in payment or "chargeoff" in payment or "charge off" in payment:
            return "collections"
        if "late" in payment and any(str(n) in payment for n in [30,60,90,120,150,180]):
            return "late_payments"
        if acc.get("has_30_in_history") or acc.get("has_60_in_history") or acc.get("has_90_in_history"):
            return "late_payments"
        if "derogatory" in status:
            return "charge_offs"
        return "other_derogatory"

    def _build_letter_input_from_negatives(negatives_by_bureau: dict) -> dict:
        """
        Fallback: build a minimal letter_input_engine from negatives_by_bureau
        when the stored letter_input_engine is empty (happens when original_parser
        was unavailable during the job's analysis run).

        Field names must match what build_dispute_letter_engine/_account_reason expect:
          - furnisher_name  (not "name")
          - account_number
          - negative_type   singular: "collection", "charge_off", "late_payment"
          - attack_type     use "requires_basic_verification" as safe default
          - recommended_round
          + all optional date/balance fields _account_reason uses
        """
        # PRIMARY: run the real detection chain so letters get specific
        # attack_types instead of the generic placeholder. report_date is
        # captured from the enclosing /generate-letters scope.
        try:
            _real = _compute_letter_input(negatives_by_bureau, report_date)
            if any(len(items) > 0
                   for groups in _real.values() for items in groups.values()):
                return _real
        except Exception as _e:
            print(f"[generate-letters] detection rebuild failed, "
                  f"falling back to basic verification: {_e}")
        # FALLBACK (only reached if detection produced nothing):
        # Map from category key → singular negative_type value the engine expects
        _CAT_TO_NEG_TYPE = {
            "collections":      "collection",
            "charge_offs":      "charge_off",
            "late_payments":    "late_payment",
            "repossessions":    "repossession",
            "bankruptcies":     "bankruptcy",
            "child_support":    "child_support",
            "other_derogatory": "derogatory",
        }

        result = {}
        for bureau, accounts in negatives_by_bureau.items():
            result[bureau] = {
                "collections":      [],
                "charge_offs":      [],
                "late_payments":    [],
                "repossessions":    [],
                "bankruptcies":     [],
                "child_support":    [],
                "other_derogatory": [],
            }
            for acc in accounts:
                cat      = _negative_type_from_account(acc)
                neg_type = _CAT_TO_NEG_TYPE.get(cat, "derogatory")
                acct_num = acc.get("account_number", "")
                entry = {
                    # ── Required by _account_reason ───────────────────────────
                    "furnisher_name":        acc.get("name", ""),
                    "account_number":        acct_num,
                    "masked_account_number": acct_num.replace("*", "X"),
                    "negative_type":         neg_type,
                    "attack_type":           "requires_basic_verification",
                    "laws":                  ["15 USC 1681i(a)", "15 USC 1681e(b)"],
                    "recommended_round":     "round_1",
                    "recommended_methods":   ["bureau_dispute"],
                    "secondary_flags":       [],
                    # ── Date / balance fields ─────────────────────────────────
                    "balance":               acc.get("balance", ""),
                    "past_due":              acc.get("past_due", ""),
                    "payment_status":        acc.get("payment_status", ""),
                    "status":                acc.get("status", ""),
                    "high_credit":           acc.get("high_credit", ""),
                    "credit_limit":          acc.get("credit_limit", ""),
                    "monthly_payment":       acc.get("monthly_payment", ""),
                    "late_payment_codes":    acc.get("late_payment_codes", []),
                    "date_opened":           acc.get("date_opened", ""),
                    "date_last_active":      acc.get("date_last_active", ""),
                    "date_of_last_payment":  acc.get("date_of_last_payment", ""),
                    "last_reported":         acc.get("last_reported", ""),
                    # ── DOFD fields (empty — not computed in fallback) ────────
                    "dofd_estimated":            None,
                    "dofd_confidence":           "unknown",
                    "fcra_expiration":           None,
                    "days_until_expiration":     None,
                    "is_obsolete":               False,
                    "re_aging_flag":             False,
                    "re_aging_gap_days":         None,
                    "dofd_verification_required": False,
                    "dla_suspected_refresh":     False,
                    # ── Extra context ─────────────────────────────────────────
                    "comments":              acc.get("comments", ""),
                    "block_id":              acc.get("block_id", ""),
                }
                result[bureau][cat].append(entry)
        return result

    # ── Check if letter_input_engine is usable ────────────────────────────────
    # Case 1: completely empty (original_parser failed during analysis)
    #   → rebuild everything from negatives_by_bureau
    # Case 2: partially populated — some bureaus missing (common with PDF uploads
    #   where the parser couldn't detect Equifax columns from the PDF layout)
    #   → merge in the missing bureaus from negatives_by_bureau
    negatives_stored = job.get("negatives_by_bureau", {})

    _lie_has_data = any(
        any(len(items) > 0 for items in groups.values())
        for groups in letter_input.values()
    )
    if not _lie_has_data:
        # Completely empty — rebuild everything
        print(f"[generate-letters] letter_input_engine empty for job={body.job_id}, "
              f"rebuilding from negatives_by_bureau")
        letter_input = _build_letter_input_from_negatives(negatives_stored)
    else:
        # Partially populated — check if any bureau has negatives but no letter_input
        # This happens when the PDF parser missed bureau columns during parsing.
        for _bureau, _neg_accts in negatives_stored.items():
            if not _neg_accts:
                continue
            _lie_bureau = letter_input.get(_bureau, {})
            _lie_bureau_has_data = any(len(v) > 0 for v in _lie_bureau.values()) if _lie_bureau else False
            if not _lie_bureau_has_data:
                print(f"[generate-letters] bureau={_bureau} missing from letter_input_engine "
                      f"for job={body.job_id}, merging from negatives_by_bureau")
                _bureau_input = _build_letter_input_from_negatives({_bureau: _neg_accts})
                letter_input[_bureau] = _bureau_input.get(_bureau, {})

    # ── Filter by bureau/category/selected_accounts if provided ──────────────
    if body.bureau and body.category and body.selected_accounts:
        bureau   = body.bureau.lower()
        category = cat_map.get(body.category, body.category.lower().replace(" ", "_"))

        # Build normalized sets for fuzzy account-number matching
        selected_nums_raw  = {a.get("account_number", "") for a in body.selected_accounts}
        selected_nums_norm = {_norm_acct(n) for n in selected_nums_raw}

        filtered_input = {bureau: {category: []}}
        items = letter_input.get(bureau, {}).get(category, [])
        for item in items:
            item_acct = item.get("account_number", "")
            if item_acct in selected_nums_raw or _norm_acct(item_acct) in selected_nums_norm:
                item["recommended_round"] = body.round or "round_1"
                filtered_input[bureau][category].append(item)

        # If still empty after filtering, it means the UI sent accounts that are
        # categorized differently in the engine (e.g. charge_off vs collection).
        # Try all categories for that bureau and pick the ones that match by acct#.
        if not filtered_input[bureau][category]:
            print(f"[generate-letters] No items matched in {bureau}/{category}, "
                  f"trying cross-category match")
            for cat_key, cat_items in letter_input.get(bureau, {}).items():
                for item in cat_items:
                    item_acct = item.get("account_number", "")
                    if item_acct in selected_nums_raw or _norm_acct(item_acct) in selected_nums_norm:
                        item["recommended_round"] = body.round or "round_1"
                        # Put them in the requested category (user's intent wins)
                        filtered_input[bureau][category].append(item)

        letter_input_to_use = filtered_input
    else:
        letter_input_to_use = letter_input

    # Parse bureau response if provided (R2/R3 targeted letters)
    bureau_response_parsed = None
    if body.bureau_response_text and body.bureau_response_text.strip():
        try:
            from original_parser import parse_bureau_response
            bureau_response_parsed = parse_bureau_response(body.bureau_response_text)
            print(f"[generate-letters] Parsed bureau response: "
                  f"{bureau_response_parsed.get('outcome_summary', {})}")
        except Exception as _e:
            print(f"[generate-letters] Failed to parse bureau response: {_e}")

    # ── e-OSCAR verification gate: regenerate until every letter passes ───────
    # Critical checks (ascii / forbidden_phrases / fidelity) MUST pass — a letter
    # that fails one is never delivered. length / structure / overlap are INFO
    # only (a multi-account letter legitimately exceeds the word window) and do
    # not block. On each failed attempt we bump variation_seed to reshuffle the
    # opening templates and closers, up to _EOSCAR_MAX_ATTEMPTS. Anything still
    # failing after that is excluded from the response and reported in "blocked".
    from original_parser import validate_eoscar_compliance

    _EOSCAR_MAX_ATTEMPTS = 5

    def _eoscar_check(text: str) -> dict:
        v = validate_eoscar_compliance(text, letter_type="bureau_dispute")
        c = v["checks"]
        critical_ok = (
            c["ascii"]["pass"]
            and not c["forbidden_phrases"]["found"]
            and c.get("fidelity_to_report", {}).get("pass", True)
        )
        return {
            "critical_ok": critical_ok,
            "passed":      v["passed"],
            "score":       v["score"],
            "ascii":       c["ascii"]["pass"],
            "forbidden":   c["forbidden_phrases"]["found"],
            "warnings":    v.get("warnings", []),
        }

    seed = body.variation_seed or 0
    dispute_letters = {}
    for attempt in range(_EOSCAR_MAX_ATTEMPTS):
        dispute_letters = build_dispute_letter_engine(
            letter_input_to_use,
            consumer_name=consumer_name,
            report_date=report_date,
            variation_seed=seed,
            target_round=body.round or "round_1",
            bureau_response_parsed=bureau_response_parsed,
        )
        any_fail = False
        for b, groups in dispute_letters.items():
            for grp, rounds in groups.items():
                for rnd, text in rounds.items():
                    if not _eoscar_check(text)["critical_ok"]:
                        any_fail = True
        if not any_fail:
            break
        print(f"[generate-letters] e-OSCAR critical fail on attempt "
              f"{attempt + 1}/{_EOSCAR_MAX_ATTEMPTS}, regenerating (seed={seed + 1})")
        seed += 1

    # Flatten letters for response — deliver only letters that pass criticals.
    letters_out = []
    blocked     = []
    letter_text = ""
    for b, groups in dispute_letters.items():
        for grp, rounds in groups.items():
            for rnd, text in rounds.items():
                chk = _eoscar_check(text)
                if not chk["critical_ok"]:
                    # Never deliver a non-compliant letter.
                    blocked.append({
                        "bureau": b, "category": grp, "round": rnd,
                        "reason": "eoscar_critical_fail",
                        "forbidden": chk["forbidden"],
                        "ascii": chk["ascii"],
                    })
                    print(f"[generate-letters] BLOCKED {b}/{grp}/{rnd}: "
                          f"forbidden={chk['forbidden']} ascii={chk['ascii']}")
                    continue
                letters_out.append({
                    "bureau": b,
                    "category": grp,
                    "round": rnd,
                    "text": text,
                    "eoscar": chk,   # verification summary for the operator
                })
                letter_text = text  # last one for simple preview

    sb.table("api_jobs").update({
        "letters_generated": True,
    }).eq("job_id", body.job_id).execute()

    return {
        "letter_text": letter_text,
        "letters": letters_out,
        "blocked": blocked,
        "job_id": body.job_id,
    }

# ═══════════════════════════════════════════════════════════════
#  CIR  +  PROGRESS  (pegar este bloque en api.py, ej. justo ANTES
#  de la sección "PORTAL (client-facing)")
#  No requiere cambios en la base ni en otros endpoints.
#  Usa datos que /upload-report y /connect-identityiq ya guardan.
# ═══════════════════════════════════════════════════════════════
import re as _cir_re

_CIR_BUR = ("transunion", "experian", "equifax")

_NEG_LABEL = {
    "collection": "Collection", "paid_collection": "Paid Collection",
    "charge_off": "Charge-off", "charge_off_deficiency": "Charge-off",
    "late_payment": "Late Payment", "repossession": "Repossession",
    "bankruptcy": "Bankruptcy", "student_loan": "Student Loan",
    "child_support": "Child Support", "derogatory": "Derogatory",
}
_ATTACK_TITLE = {
    "dofd_unknown_verification_required": "Missing Date of First Delinquency",
    "balance_exceeds_credit_limit": "Balance exceeds credit limit",
    "closed_with_balance": "Closed account reporting a balance",
    "same_account_number_same_balance": "Same account number & balance in separate blocks",
    "multi_furnisher_same_balance": "Same balance across multiple furnishers",
    "duplicate_account_number": "Duplicate account number",
    "collector_original_creditor_self_declared": "Collector self-declares original creditor",
    "re_aging": "Re-aging of delinquency date",
    "obsolete_account": "Obsolete account still reporting",
    "cross_bureau_account_status_conflict": "Account status conflicts across bureaus",
    "cross_bureau_payment_status_conflict": "Payment status conflicts across bureaus",
    "medical_debt_accuracy": "Medical debt accuracy",
}
_PI_TITLE = {
    "ssn_inconsistency": "Divergent SSN across bureaus",
    "dob_inconsistency": "Date of birth reported differently",
    "name_inconsistency": "Name reported differently across bureaus",
    "unknown_former_name": "Unrecognized former name",
    "unrecognized_aka": "Unrecognized alias (AKA)",
    "current_address_inconsistency": "Inconsistent current address",
    "multiple_previous_addresses": "Multiple previous addresses",
}
_CIR_DISCLAIMER = (
    "This document is an analysis of the content of the credit report, not the reinvestigation "
    "a bureau performs under 15 U.S.C. section 1681i. Report Defence does not guarantee results or "
    "the removal of accurate information, does not negotiate debt, and does not provide legal advice. "
    "The consumer signs and mails all letters."
)
_MIXED_FILE_TYPES = {"ssn_inconsistency", "dob_inconsistency", "name_inconsistency",
                     "unknown_former_name", "current_address_inconsistency",
                     "multiple_previous_addresses", "unrecognized_aka"}


def _cir_int(v):
    try:
        return int(v)
    except Exception:
        return 0

def _cir_rating(s):
    s = _cir_int(s)
    if s <= 0:
        return ""
    if s < 580:
        return "Poor"
    if s < 670:
        return "Fair"
    if s < 740:
        return "Good"
    if s < 800:
        return "Very Good"
    return "Exceptional"

def _cir_mask(a):
    d = _cir_re.sub(r"\D", "", a or "")
    return ("XXXX" + d[-4:]) if len(d) >= 4 else (a or "")

def _cir_fp(n):
    name = _cir_re.sub(r"[^a-z0-9]", "", (n.get("name", "") or "").lower())[:14]
    acct = _cir_re.sub(r"\D", "", (n.get("account_number", "") or ""))[-4:]
    return f"{name}:{acct}"


def compose_cir(job: dict, round_num: int = 1) -> dict:
    """Build the CIR (Credit Investigation Report) content from a stored job."""
    scores = job.get("scores", {}) or {}
    negs = job.get("negatives_by_bureau", {}) or {}
    lie = job.get("letter_input_engine", {}) or {}
    attacks = job.get("attacks", []) or []
    pii = job.get("personal_info_issues", []) or []

    counts = {b: len(negs.get(b, []) or []) for b in _CIR_BUR}
    total = sum(counts.values())

    group_counts = {}
    for _b, groups in lie.items():
        for g, items in (groups or {}).items():
            group_counts[g] = group_counts.get(g, 0) + len(items or [])

    findings = []
    for b, items in negs.items():
        for n in (items or []):
            findings.append({
                "bureau": b,
                "type": _NEG_LABEL.get(n.get("negative_type"), "Derogatory"),
                "furnisher": n.get("name", ""),
                "account": _cir_mask(n.get("account_number", "")),
                "balance": n.get("balance", ""),
            })

    risks, seen = [], set()
    for a in attacks:
        at = a.get("attack_type", "")
        if at and at not in seen:
            seen.add(at)
            risks.append({
                "severity": a.get("severity", "medium"),
                "category": "Reporting",
                "title": _ATTACK_TITLE.get(at, at.replace("_", " ").title()),
                "description": a.get("reason", ""),
            })
    mixed = False
    for i in pii:
        t = i.get("type")
        if t in _MIXED_FILE_TYPES:
            mixed = True
        risks.append({
            "severity": i.get("severity", "medium"),
            "category": "Personal info",
            "title": _PI_TITLE.get(t, (t or "").replace("_", " ").title()),
            "description": i.get("description", ""),
        })

    parts = []
    if group_counts.get("collections"):
        parts.append(f"{group_counts['collections']} collections")
    if group_counts.get("charge_offs"):
        parts.append(f"{group_counts['charge_offs']} charge-offs")
    if group_counts.get("late_payments"):
        parts.append(f"{group_counts['late_payments']} late payments")
    breakdown = ", ".join(parts) if parts else "several derogatory items"
    summary = (f"The analysis identified {total} negative findings distributed as follows: "
               f"TransUnion {counts['transunion']}, Experian {counts['experian']}, "
               f"Equifax {counts['equifax']} - {breakdown}.")
    if mixed:
        summary += (" Signals of a possible mixed file were also detected (divergent identity "
                    "information across bureaus), which strengthen the accuracy disputes under the "
                    "FCRA (15 U.S.C. section 1681e(b) / 1681i).")

    strategy = [f"Round {round_num}: accuracy disputes to all reporting bureaus on the {total} "
                "listed accounts (15 U.S.C. section 1681i / 1681e(b))."]
    top = [r["title"] for r in risks if r["category"] == "Reporting"][:4]
    if top:
        strategy.append("Prioritize the detected angles: " + ", ".join(top) + ".")
    if mixed:
        strategy.append("Personal-information update letters for the mixed file, attaching ID + "
                        "proof of address.")
    strategy.append("Wait 30 days from the Return Receipt; whatever survives advances to the next "
                    "round via method-of-verification.")

    return {
        "client": job.get("consumer_name", ""),
        "source": job.get("source", ""),
        "report_date": job.get("report_date", ""),
        "round": round_num,
        "scores": {b: {"score": _cir_int(scores.get(b)), "rating": _cir_rating(scores.get(b))}
                   for b in _CIR_BUR},
        "counts": counts,
        "total_findings": total,
        "executive_summary": summary,
        "findings": findings,
        "risks": risks,
        "strategy": strategy,
        "disclaimer": _CIR_DISCLAIMER,
    }


def compute_progress(prev: dict, now: dict) -> dict:
    """Compare two rounds (previous vs current job) -> deletions + score deltas."""
    sp = prev.get("scores", {}) or {}
    sn = now.get("scores", {}) or {}

    def _fps(job):
        s = {}
        for b, items in (job.get("negatives_by_bureau", {}) or {}).items():
            for n in (items or []):
                s[(b, _cir_fp(n))] = n
        return s

    P, N = _fps(prev), _fps(now)

    def _row(n, b):
        return {"bureau": b, "furnisher": n.get("name", ""),
                "type": _NEG_LABEL.get(n.get("negative_type"), "Derogatory"),
                "account": _cir_mask(n.get("account_number", ""))}

    deleted = [_row(n, b) for (b, f), n in P.items() if (b, f) not in N]
    survived = [_row(n, b) for (b, f), n in P.items() if (b, f) in N]
    new = [_row(n, b) for (b, f), n in N.items() if (b, f) not in P]

    return {
        "client": now.get("consumer_name", ""),
        "scores_prev": {b: _cir_int(sp.get(b)) for b in _CIR_BUR},
        "scores_now": {b: _cir_int(sn.get(b)) for b in _CIR_BUR},
        "deltas": {b: _cir_int(sn.get(b)) - _cir_int(sp.get(b)) for b in _CIR_BUR},
        "deleted": deleted,
        "survived": survived,
        "new_or_reinserted": new,
        "deleted_count": len(deleted),
        "survived_count": len(survived),
    }


def _client_jobs_ordered(client_id: str) -> list:
    """Return the client's full jobs sorted oldest -> newest."""
    cr = sb.table("api_clients").select("job_ids").eq("id", client_id).execute()
    if not cr.data:
        return []
    ids = cr.data[0].get("job_ids") or []
    if not ids:
        return []
    jr = sb.table("api_jobs").select("*").in_("job_id", ids).execute()
    jobs = jr.data or []
    jobs.sort(key=lambda j: j.get("created_at", ""))
    return jobs


@app.get("/cir/{job_id}")
async def get_cir(job_id: str, user=Depends(get_current_user)):
    res = sb.table("api_jobs").select("*").eq("job_id", job_id).execute()
    if not res.data or len(res.data) == 0:
        raise HTTPException(404, "Job not found")
    job = res.data[0]
    # infer round number from the client's job order (1 = first report)
    round_num = 1
    cid = job.get("client_id")
    if cid:
        ordered = _client_jobs_ordered(cid)
        for idx, jj in enumerate(ordered, 1):
            if jj.get("job_id") == job_id:
                round_num = idx
                break
    return compose_cir(job, round_num=round_num)


@app.get("/progress/{client_id}")
async def get_progress(client_id: str, user=Depends(get_current_user)):
    jobs = _client_jobs_ordered(client_id)
    if len(jobs) < 2:
        raise HTTPException(400, "Need at least two reports (rounds) to build a progress report.")
    prev, now = jobs[-2], jobs[-1]
    out = compute_progress(prev, now)
    out["round_prev"] = len(jobs) - 1
    out["round_now"] = len(jobs)
    return out


# ═══════════════════════════════════════════════════════════════
#  PORTAL (client-facing)
# ═══════════════════════════════════════════════════════════════

@app.get("/portal/overview")
async def portal_overview(user=Depends(get_current_user)):
    res = sb.table("api_clients").select("*").eq("user_id", user["id"]).execute()
    if not res.data or len(res.data) == 0:
        raise HTTPException(404, "No client record linked")
    return enrich_client(res.data[0])

@app.get("/portal/letters")
async def portal_letters(user=Depends(get_current_user)):
    res = sb.table("api_clients").select("job_ids").eq("user_id", user["id"]).execute()
    if not res.data or len(res.data) == 0:
        return []
    job_ids = res.data[0].get("job_ids") or []
    if not job_ids:
        return []
    jr = sb.table("api_jobs").select("job_id, letter_files").in_("job_id", job_ids).execute()
    letters = []
    for j in (jr.data or []):
        for lf in (j.get("letter_files") or []):
            letters.append({**lf, "job_id": j["job_id"]})
    return letters

# ═══════════════════════════════════════════════════════════════
#  HEALTH
# ═══════════════════════════════════════════════════════════════

@app.get("/health")
async def health():
    return {"status": "ok", "storage": "supabase", "timestamp": datetime.now(timezone.utc).isoformat()}
