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
                "letter_input_engine": {b: {} for b in ["transunion", "experian", "equifax"]},
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
        # Map from category key → singular negative_type value the engine expects
        _CAT_TO_NEG_TYPE = {
            "collections":      "collection",
            "charge_offs":      "charge_off",
            "late_payments":    "late_payment",
            "other_derogatory": "derogatory",
        }

        result = {}
        for bureau, accounts in negatives_by_bureau.items():
            result[bureau] = {
                "collections":      [],
                "charge_offs":      [],
                "late_payments":    [],
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

    dispute_letters = build_dispute_letter_engine(
        letter_input_to_use,
        consumer_name=consumer_name,
        report_date=report_date,
        variation_seed=body.variation_seed or 0,
        target_round=body.round or "round_1",
        bureau_response_parsed=bureau_response_parsed,
    )

    # Flatten letters for response
    letters_out = []
    letter_text = ""
    for b, groups in dispute_letters.items():
        for grp, rounds in groups.items():
            for rnd, text in rounds.items():
                letters_out.append({
                    "bureau": b,
                    "category": grp,
                    "round": rnd,
                    "text": text,
                })
                letter_text = text  # last one for simple preview

    sb.table("api_jobs").update({
        "letters_generated": True,
    }).eq("job_id", body.job_id).execute()

    return {"letter_text": letter_text, "letters": letters_out, "job_id": body.job_id}

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
