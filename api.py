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
def _base_tradelines_from_negatives(negatives_by_bureau: dict) -> list:
    """
    Reconstruye base_tradelines a partir de negatives_by_bureau.

    POR QUE HACE FALTA. Los ataques cross-bureau se anclan en el block_id
    (el bloque del PDF), NO en el numero de cuenta: el mismo tradeline sale
    enmascarado distinto en cada buro ("3719****" en uno, "0003719****" en
    otro) y compararlos por numero daria falsos positivos. El ancla la
    provee base_tradelines, que build_report si arma pero que NO se guarda
    en api_jobs. Sin el, todo el pase cross-bureau devuelve vacio.

    Los negativos que si guardamos traen block_id y todos los campos que
    el detector necesita, asi que la estructura se puede rearmar sin tocar
    el esquema de la base ni volver a leer el PDF.

    Si las cuentas no traen block_id (jobs viejos, intake por connector),
    devuelve lista vacia y el motor se comporta como hasta hoy.
    """
    por_bloque: dict = {}
    for bureau, accounts in (negatives_by_bureau or {}).items():
        for acc in accounts or []:
            if not isinstance(acc, dict):
                continue
            bid = acc.get("block_id") or ""
            if not bid:
                continue
            tl = por_bloque.setdefault(bid, {
                "base_tradeline_id": bid,
                "furnisher_name": acc.get("name", ""),
                "bureau_entries": {},
                "raw_lines": acc.get("raw_lines", []),
            })
            if not tl["furnisher_name"]:
                tl["furnisher_name"] = acc.get("name", "")
            tl["bureau_entries"][bureau] = {
                "account_number":        acc.get("account_number", ""),
                "masked_account_number": acc.get("account_number", ""),
                "status":                acc.get("status", ""),
                "payment_status":        acc.get("payment_status", ""),
                "balance":               acc.get("balance", ""),
                "past_due":              acc.get("past_due", ""),
                "comments":              acc.get("comments", ""),
            }
    return list(por_bloque.values())


def _compute_letter_input(
    negatives_by_bureau: dict,
    report_date: str = "",
    client_state: str = "",
    base_tradelines: list | None = None,
) -> dict:
    """
    client_state y base_tradelines son los dos parametros que antes iban
    fijos en "" y None. Con ellos encendidos:
      - base_tradelines habilita los 8 ataques cross-bureau
      - client_state habilita la tabla de leyes estatales de deuda medica
    Los dos siguen siendo opcionales, asi que cualquier llamada vieja
    sigue funcionando igual que antes.
    """
    from original_parser import (
        build_dofd_engine, build_legal_detection_engine,
        build_attack_scoring_engine, build_strategy_engine,
        build_letter_input_engine,
    )
    enriched = build_dofd_engine(negatives_by_bureau or {}, report_date or "")
    if base_tradelines is None:
        base_tradelines = _base_tradelines_from_negatives(enriched)
    lde      = build_legal_detection_engine(
        enriched, base_tradelines or None,
        report_date=report_date or "",
        client_state=client_state or "",
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


_raw_report_cache: dict = {}


def _raw_report_text_for_job(job: dict) -> str:
    """
    Texto crudo del PDF del job, para el chequeo de fidelidad de e-OSCAR.

    Se lee una sola vez por job y queda en memoria del proceso. Si el PDF
    ya no esta (job viejo, contenedor reciclado, intake por connector sin
    PDF), devuelve cadena vacia y el chequeo se saltea igual que antes: no
    se bloquea la generacion por no poder leer un archivo.
    """
    jid = str(job.get("job_id") or "")
    if jid and jid in _raw_report_cache:
        return _raw_report_cache[jid]
    texto = ""
    path = job.get("pdf_path") or ""
    if path and os.path.exists(path):
        try:
            from original_parser import extract_text_from_pdf
            texto = extract_text_from_pdf(path) or ""
        except Exception as e:
            print(f"[eoscar] no se pudo leer el PDF del job {jid}: {e}")
            texto = ""
    if jid:
        _raw_report_cache[jid] = texto
    return texto


# Columnas reales de api_jobs. Escribir una clave que no este aca hace que
# PostgREST rechace el INSERT ENTERO con PGRST204 ("Could not find the
# 'X' column of 'api_jobs' in the schema cache") y la subida del reporte
# falla con 500. Filtrar contra esta lista convierte ese 500 en una linea
# de log.
#
# Si alguna vez agregas una columna a la tabla, agregala tambien aca.
_API_JOBS_COLUMNS = {
    "job_id", "client_id", "operator_id", "consumer_name", "source",
    "report_date", "pdf_path", "scores", "attack_count", "letters_generated",
    "letter_files", "letter_count", "negatives_by_bureau", "inventory_by_bureau",
    "personal_info", "personal_info_issues", "letter_input_engine", "attacks",
    "inquiries", "inquiry_attacks", "response_history", "status", "error",
    "created_at",
}


def _solo_columnas_de_api_jobs(payload: dict) -> dict:
    """Descarta claves que no son columnas de api_jobs, avisando en el log."""
    limpio = {k: v for k, v in (payload or {}).items() if k in _API_JOBS_COLUMNS}
    sobrantes = sorted(set(payload or {}) - _API_JOBS_COLUMNS)
    if sobrantes:
        print(f"[api_jobs] descartadas claves que no son columnas: {sobrantes}")
    return limpio


def _resolve_letter_input(result: dict, negatives: dict, client_state: str = "") -> dict:
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
        _compute_letter_input(
            negatives,
            result.get("report_date", ""),
            client_state=client_state,
            base_tradelines=(
                result.get("base_tradeline_engine")
                or _base_tradelines_from_negatives(negatives)
                or None
            ),
        )
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

# ─── Control de propiedad ─────────────────────────────────────
# Antes, endpoints como /jobs/{id}, /cir/{id} o PATCH /clients/{id} solo
# comprobaban que el token fuera válido, nunca que el recurso fuera tuyo.
# Un token de cualquier cuenta llegaba a los datos de otra: reportes de
# crédito completos, direcciones, cartas, recibos.
#
# Regla:
#   - la cuenta admin ve todo (soporte)
#   - un operador ve los clientes donde operator_id == su id
#   - un consumidor ve el cliente donde user_id == su id (portal)
#   - cualquier otro caso: 404, NUNCA 403 — un 403 confirma que el
#     recurso existe, y eso ya es información que no le debemos a nadie.

# Cuentas con vista completa (soporte / dueño del negocio). Configurable por
# entorno: ADMIN_EMAILS="uno@x.com,dos@y.com". Por defecto, las dos cuentas
# del dueño, para que la vista global no dependa del rol.
#
# Por qué no basta con role == "operator": el rol se auto-otorga con
# OPERATOR_CODE, que tiene default publico en el codigo. Cualquiera que lo
# adivine seria "operator". Ser admin, en cambio, exige estar en esta lista,
# que solo se cambia desde las variables de entorno del servidor.
ADMIN_EMAILS = {
    e.strip().lower()
    for e in os.environ.get("ADMIN_EMAILS", ADMIN_EMAIL).split(",")
    if e.strip()
}

def _is_admin(user: dict) -> bool:
    return str(user.get("email", "")).lower() in ADMIN_EMAILS

def _owns_client_row(user: dict, client_row: dict) -> bool:
    if _is_admin(user):
        return True
    uid = user["id"]
    return client_row.get("operator_id") == uid or client_row.get("user_id") == uid

def _get_client_or_404(user: dict, client_id: str) -> dict:
    """Devuelve la fila del cliente si el usuario puede verla; si no, 404."""
    res = sb.table("api_clients").select("*").eq("id", client_id).execute()
    if not res.data or len(res.data) == 0:
        raise HTTPException(404, "Client not found")
    row = res.data[0]
    if not _owns_client_row(user, row):
        print(f"[acceso] denegado: user={user['id']} intento cliente={client_id}")
        raise HTTPException(404, "Client not found")
    return row

def _get_job_or_404(user: dict, job_id: str) -> dict:
    """Devuelve la fila del job si el usuario puede verla; si no, 404."""
    res = sb.table("api_jobs").select("*").eq("job_id", job_id).execute()
    if not res.data or len(res.data) == 0:
        raise HTTPException(404, "Job not found")
    job = res.data[0]
    if _is_admin(user) or job.get("operator_id") == user["id"]:
        return job
    # Puede que el job no tenga operator_id (filas viejas): caemos al cliente.
    cid = job.get("client_id")
    if cid:
        cr = sb.table("api_clients").select("*").eq("id", cid).execute()
        if cr.data and _owns_client_row(user, cr.data[0]):
            return job
    print(f"[acceso] denegado: user={user['id']} intento job={job_id}")
    raise HTTPException(404, "Job not found")

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

def _verify_supabase_token(access_token: str) -> dict:
    """Valida el access_token contra Supabase y devuelve la identidad REAL.

    Este es el corazón del arreglo: antes el endpoint confiaba en el `email`
    que mandaba el navegador y ni siquiera leía el token, así que cualquiera
    que conociera la ruta podía pedir una sesión a nombre de cualquier
    cuenta, sin contraseña. Ahora el email sale de la respuesta de Supabase,
    nunca del body.

    Lanza ValueError con el motivo (para el log). El motivo NO se le devuelve
    al cliente.
    """
    import httpx

    if not access_token or len(access_token) < 20:
        raise ValueError("token ausente o demasiado corto")

    # /auth/v1/user acepta la anon key o la service key como apikey.
    apikey = os.environ.get("SUPABASE_ANON_KEY") or SUPABASE_KEY
    try:
        r = httpx.get(
            f"{SUPABASE_URL.rstrip('/')}/auth/v1/user",
            headers={"Authorization": f"Bearer {access_token}", "apikey": apikey},
            timeout=10.0,
        )
    except Exception as e:
        # Fallar cerrado: si no podemos comprobar, no emitimos sesión.
        raise ValueError(f"no se pudo contactar a Supabase: {e}")

    if r.status_code != 200:
        raise ValueError(f"Supabase rechazó el token (HTTP {r.status_code})")

    data = r.json() or {}
    email = (data.get("email") or "").strip().lower()
    if not email:
        raise ValueError("Supabase no devolvió email para ese token")
    if not data.get("id"):
        raise ValueError("Supabase no devolvió id de usuario")

    meta = data.get("user_metadata") or {}
    return {
        "supabase_id": data["id"],
        "email": email,
        "full_name": meta.get("full_name") or meta.get("name") or "",
    }


@app.post("/auth/supabase")
async def auth_supabase(body: SupabaseAuthBody, request: Request):
    # Límite de intentos: este endpoint emite sesiones y antes no tenía ninguno.
    check_rate_limit(request, "auth/supabase", email=body.email,
                     max_requests=10, window_seconds=900)

    try:
        verified = _verify_supabase_token(body.access_token)
    except ValueError as e:
        # El motivo va al log, no a la respuesta: no le damos al atacante un
        # oráculo que le diga en qué se equivocó.
        print(f"[auth/supabase] rechazado: {e}")
        raise HTTPException(401, "Invalid Supabase session")

    # La identidad sale de Supabase. body.email queda solo como pista para el
    # rate limit; ya no decide quién sos.
    email     = verified["email"]
    full_name = body.full_name or verified["full_name"] or email.split("@")[0]

    existing = sb.table("api_users").select("*").eq("email", email).execute()
    if existing.data and len(existing.data) > 0:
        u = existing.data[0]
    else:
        # Reintento sin distinguir mayúsculas, para no duplicar cuentas
        # creadas antes con otra capitalización.
        alt = sb.table("api_users").select("*").ilike("email", email).execute()
        if alt.data and len(alt.data) > 0:
            u = alt.data[0]
        else:
            res = sb.table("api_users").insert({
                "email": email,
                "full_name": full_name,
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
    # Coherente con _get_client_or_404: si aparece en la lista, se puede abrir.
    # Antes un operador veia los clientes de TODAS las agencias.
    if _is_admin(user):
        filas = sb.table("api_clients").select("*").order("created_at", desc=True).execute().data or []
    else:
        uid = user["id"]
        a = sb.table("api_clients").select("*").eq("operator_id", uid).execute().data or []
        b = sb.table("api_clients").select("*").eq("user_id", uid).execute().data or []
        vistos, filas = set(), []
        for c in a + b:
            if c["id"] not in vistos:
                vistos.add(c["id"]); filas.append(c)
        filas.sort(key=lambda c: c.get("created_at", ""), reverse=True)
    return [enrich_client(c) for c in filas]

@app.post("/clients", status_code=201)
async def create_client(body: ClientCreate, user=Depends(get_current_user)):
    _enforce_can_add_client(user)   # plan requerido; Básica = máx 1 cliente
    data = body.model_dump()
    data["operator_id"] = user["id"]
    res = sb.table("api_clients").insert(data).execute()
    c = res.data[0]
    return {**c, "job_ids": c.get("job_ids", []), "user_id": c.get("user_id")}

@app.get("/clients/{client_id}")
async def get_client(client_id: str, user=Depends(get_current_user)):
    c = _get_client_or_404(user, client_id)
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
    _get_client_or_404(user, client_id)          # solo el dueno puede modificar
    sb.table("api_clients").update(updates).eq("id", client_id).execute()
    return await get_client(client_id, user)

@app.delete("/clients/{client_id}")
async def delete_client(client_id: str, user=Depends(get_current_user)):
    sb.table("api_clients").delete().eq("id", client_id).eq("operator_id", user["id"]).execute()
    return {"ok": True}

@app.get("/clients/{client_id}/history")
async def client_history(client_id: str, user=Depends(get_current_user)):
    row = _get_client_or_404(user, client_id)
    job_ids = row.get("job_ids") or []
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
    row = _get_client_or_404(user, client_id)
    job_ids = row.get("job_ids") or []
    if not job_ids:
        return []
    jr = sb.table("api_jobs").select("job_id, letter_files, letters_generated").in_("job_id", job_ids).execute()
    letters = []
    for j in (jr.data or []):
        for lf in (j.get("letter_files") or []):
            letters.append({**lf, "job_id": j["job_id"]})
    return letters

# ═══════════════════════════════════════════════════════════════
#  TASK 3 — CERTIFIED MAIL RECEIPTS (por carta, por cliente)
#  Requiere la tabla letter_receipts (ver letter_receipts.sql) y el
#  bucket 'receipts' en Supabase Storage. El archivo del recibo se sube
#  desde el frontend; aquí guardamos la metadata + la URL.
# ═══════════════════════════════════════════════════════════════

class ReceiptCreate(BaseModel):
    client_id: str
    job_id: Optional[str] = None
    round: Optional[str] = None
    recipient_type: Optional[str] = None      # 'bureau' | 'furnisher'
    recipient_name: Optional[str] = None
    tracking_number: Optional[str] = None
    receipt_file_url: Optional[str] = None
    postalocity_job_id: Optional[str] = None
    date_sent: Optional[str] = None           # 'YYYY-MM-DD'
    return_receipt_date: Optional[str] = None
    notes: Optional[str] = None

class ReceiptUpdate(BaseModel):
    tracking_number: Optional[str] = None
    receipt_file_url: Optional[str] = None
    date_sent: Optional[str] = None
    return_receipt_date: Optional[str] = None
    notes: Optional[str] = None

@app.post("/letter-receipts", status_code=201)
async def create_receipt(body: ReceiptCreate, user=Depends(get_current_user)):
    data = {k: v for k, v in body.model_dump().items() if v is not None}
    _get_client_or_404(user, body.client_id)     # el cliente tiene que ser tuyo
    data["operator_id"] = user["id"]
    res = sb.table("letter_receipts").insert(data).execute()
    return res.data[0] if res.data else {"ok": True}

@app.get("/clients/{client_id}/receipts")
async def list_receipts(client_id: str, user=Depends(get_current_user)):
    _get_client_or_404(user, client_id)
    res = (sb.table("letter_receipts").select("*")
           .eq("client_id", client_id).order("created_at", desc=True).execute())
    return res.data or []

@app.patch("/letter-receipts/{receipt_id}")
async def update_receipt(receipt_id: str, body: ReceiptUpdate, user=Depends(get_current_user)):
    updates = {k: v for k, v in body.model_dump().items() if v is not None}
    if not updates:
        raise HTTPException(400, "No fields to update")
    _own = sb.table("letter_receipts").select("id").eq("id", receipt_id).eq(
        "operator_id", user["id"]).execute()
    if not _own.data and not _is_admin(user):
        raise HTTPException(404, "Receipt not found")
    sb.table("letter_receipts").update(updates).eq("id", receipt_id).execute()
    r = sb.table("letter_receipts").select("*").eq("id", receipt_id).execute()
    return r.data[0] if r.data else {"ok": True}

@app.delete("/letter-receipts/{receipt_id}")
async def delete_receipt(receipt_id: str, user=Depends(get_current_user)):
    sb.table("letter_receipts").delete().eq("id", receipt_id).eq("operator_id", user["id"]).execute()
    return {"ok": True}

# ═══════════════════════════════════════════════════════════════
#  JOB / REPORT ROUTES
# ═══════════════════════════════════════════════════════════════

@app.get("/jobs/{job_id}")
async def get_job(job_id: str, user=Depends(get_current_user)):
    return _get_job_or_404(user, job_id)

@app.post("/upload-report")
async def upload_report(
    file: UploadFile = File(...),
    consumer_name: str = Form(...),
    client_id: str = Form(...),
    source: str = Form("identityiq"),
    user=Depends(get_current_user),
):
    client_row = _get_client_or_404(user, client_id)   # no se sube a un cliente ajeno
    job_id = str(uuid.uuid4())
    pdf_path = os.path.join(UPLOAD_DIR, f"{job_id}.pdf")
    with open(pdf_path, "wb") as f:
        shutil.copyfileobj(file.file, f)

    # Estado del cliente. Sin esto la tabla de leyes estatales de deuda
    # medica no se consulta nunca, aunque el dato ya este en la ficha.
    client_state = str((client_row or {}).get("state") or "").strip()

    # ── Run parser ──
    from original_parser import build_report
    result = build_report(pdf_path, client_state=client_state)

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
    # NOTA: base_tradelines NO se guarda. La tabla api_jobs no tiene esa
    # columna y agregarla obligaria a una migracion. No hace falta: se
    # reconstruye desde negatives_by_bureau agrupando por block_id, y esa
    # reconstruccion da exactamente los mismos ataques que el motor
    # completo (verificado tipo por tipo). Lo mismo con client_state: se
    # lee de api_clients cuando se generan las cartas.
    sb.table("api_jobs").insert(_solo_columnas_de_api_jobs(job_data)).execute()

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

    # Validate client exists Y que sea tuyo
    client_data = _get_client_or_404(user, body.client_id)
    # BUG: la columna es full_name; con "name" consumer_name quedaba siempre "".
    consumer_name = client_data.get("full_name", "")

    job_id = str(uuid.uuid4())

    # Store pending job
    sb.table("api_jobs").insert(_solo_columnas_de_api_jobs({
        "job_id":      job_id,
        "client_id":   body.client_id,
        "operator_id": user["id"],
        "consumer_name": consumer_name,
        "source":      "identityiq_json",
        "status":      "pending",
        "error":       None,
    })).execute()

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

    # Validate client Y que sea tuyo
    client_data = _get_client_or_404(user, body.client_id)
    # BUG: la columna es full_name; con "name" consumer_name quedaba siempre "".
    consumer_name = client_data.get("full_name", "")

    job_id = str(uuid.uuid4())

    # Store pending job immediately
    sb.table("api_jobs").insert(_solo_columnas_de_api_jobs({
        "job_id":       job_id,
        "client_id":    body.client_id,
        "operator_id":  user["id"],
        "consumer_name": consumer_name,
        "source":       "identityiq_json",
        "status":       "pending",
        "error":        None,
    })).execute()

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
                "letter_input_engine": _resolve_letter_input(
                    result, negatives,
                    client_state=str(client_data.get("state") or "").strip(),
                ),
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
    _enforce_round(user, body.round)   # plan requerido; Básica = solo Round 1
    job = _get_job_or_404(user, body.job_id)

    from original_parser import build_dispute_letter_engine

    letter_input = job.get("letter_input_engine", {})
    consumer_name = body.consumer_name
    report_date = job.get("report_date", "")

    # ── Los dos parametros que antes iban fijos ───────────────────────────────
    # client_state: sale de la ficha del cliente. api_jobs no tiene columna
    # propia y no hace falta: el estado del cliente es el dato vigente.
    _client_state = ""
    if job.get("client_id"):
        try:
            _cr = sb.table("api_clients").select("state").eq(
                "id", job["client_id"]).execute()
            if _cr.data:
                _client_state = str(_cr.data[0].get("state") or "").strip()
        except Exception as _e:
            print(f"[generate-letters] no se pudo leer el estado del cliente: {_e}")

    # base_tradelines: ancla de los 8 ataques cross-bureau. Se reconstruye
    # desde los negativos agrupando por block_id. Verificado: da exactamente
    # los mismos ataques que el base_tradeline_engine que arma build_report.
    _base_tradelines = _base_tradelines_from_negatives(
        job.get("negatives_by_bureau", {})
    ) or None
    print(f"[generate-letters] client_state={_client_state!r} "
          f"base_tradelines={len(_base_tradelines or [])}")

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
            _real = _compute_letter_input(
                negatives_by_bureau, report_date,
                client_state=_client_state,
                base_tradelines=_base_tradelines,
            )
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

    # raw_report_text: habilita el chequeo de fidelidad. Sin el, ese check se
    # auto-reportaba como aprobado aunque el codigo de abajo lo trate como
    # critico, o sea que era un candado sin cerradura. El texto se extrae una
    # sola vez del PDF del job y se cachea en memoria.
    _raw_report_text = _raw_report_text_for_job(job)

    def _eoscar_check(text: str, others: list | None = None) -> dict:
        v = validate_eoscar_compliance(
            text,
            raw_report_text=_raw_report_text,
            other_letters=others or None,
            letter_type="bureau_dispute",
        )
        c = v["checks"]
        # CRITICO (bloquea el envio): ascii, frases prohibidas y fidelidad.
        # INFO (no bloquea): largo, estructura y solapamiento. Una carta con
        # varias cuentas excede el rango de palabras por diseno, y el
        # solapamiento entre buros es inevitable en la seccion de informacion
        # personal, que habla de los mismos hechos.
        critical_ok = (
            c["ascii"]["pass"]
            and not c["forbidden_phrases"]["found"]
            and c.get("fidelity_to_report", {}).get("pass", True)
        )
        _ov = c.get("overlap", {})
        return {
            "critical_ok": critical_ok,
            "passed":      v["passed"],
            "score":       v["score"],
            "ascii":       c["ascii"]["pass"],
            "forbidden":   c["forbidden_phrases"]["found"],
            "fidelity":    c.get("fidelity_to_report", {}),
            "overlap":     {"pass": _ov.get("pass", True),
                            "skipped": _ov.get("skipped", True),
                            "detail": _ov.get("detail", "")},
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
    #
    # other_letters: cada carta se compara contra las DEMAS de esta misma
    # tanda para el chequeo de solapamiento. Sin esto el check quedaba
    # apagado y el reporte decia "Skipped". Es informativo, no bloquea: la
    # seccion de informacion personal habla de los mismos hechos en las tres
    # cartas y va a solapar siempre.
    _todas = [
        text
        for _b, _g in dispute_letters.items()
        for _grp, _r in _g.items()
        for _rnd, text in _r.items()
    ]

    letters_out = []
    blocked     = []
    letter_text = ""
    for b, groups in dispute_letters.items():
        for grp, rounds in groups.items():
            for rnd, text in rounds.items():
                chk = _eoscar_check(text, others=[t for t in _todas if t != text])
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

    # letter_files: hasta ahora se escribia [] al crear el job y no se
    # poblaba nunca, asi que GET /clients/{id}/letters devolvia lista vacia
    # siempre y las cartas generadas se perdian apenas se cerraba la
    # respuesta. Se guardan aca, acumulando por ronda: una segunda
    # generacion de la misma ronda reemplaza la anterior en vez de duplicar.
    try:
        _previas = (job.get("letter_files") or [])
        _rondas_nuevas = {(l["bureau"], l["category"], l["round"]) for l in letters_out}
        _acumuladas = [
            lf for lf in _previas
            if (lf.get("bureau"), lf.get("category"), lf.get("round")) not in _rondas_nuevas
        ]
        _ahora = datetime.now(timezone.utc).isoformat()
        for l in letters_out:
            _acumuladas.append({
                "bureau":      l["bureau"],
                "category":    l["category"],
                "round":       l["round"],
                "filename":    f"{l['bureau']}_{l['category']}_{l['round']}.txt",
                "text":        l["text"],
                "generated_at": _ahora,
                "eoscar_score": (l.get("eoscar") or {}).get("score"),
            })
        sb.table("api_jobs").update({
            "letters_generated": True,
            "letter_files": _acumuladas,
            "letter_count": len(_acumuladas),
        }).eq("job_id", body.job_id).execute()
    except Exception as e:
        # Guardar las cartas no puede impedir devolverlas al operador.
        print(f"[generate-letters] no se pudo guardar letter_files: {e}")
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
#  TASK 1 — FURNISHER / CREDITOR LETTERS
#  Genera las cartas a furnishers/acreedores (debt validation + 1681s-2),
#  con el mismo gate e-OSCAR que las cartas a buró. No toca nada existente.
# ═══════════════════════════════════════════════════════════════

class GenerateFurnisherLettersBody(BaseModel):
    job_id: str
    consumer_name: str
    consumer_address: Optional[str] = None       # opcional: reemplaza [Address]
    consumer_city_state_zip: Optional[str] = None # opcional: reemplaza [City, State ZIP]
    round: Optional[str] = "round_1"             # round_1 | round_2 (no hay furnisher en R3)
    selected_furnishers: Optional[list] = None    # opcional: lista de nombres a incluir
    variation_seed: Optional[int] = 0

@app.post("/generate-furnisher-letters")
async def generate_furnisher_letters(body: GenerateFurnisherLettersBody, user=Depends(get_current_user)):
    _enforce_round(user, body.round)   # plan requerido; Básica = solo Round 1
    job = _get_job_or_404(user, body.job_id)

    from original_parser import build_furnisher_letter_engine, validate_eoscar_compliance

    # Reusar el letter_input_engine guardado; si viniera vacío, reconstruir desde negativos
    letter_input = job.get("letter_input_engine", {}) or {}
    _has = any(any(len(i) > 0 for i in g.values()) for g in letter_input.values())
    if not _has:
        letter_input = _compute_letter_input(
            job.get("negatives_by_bureau", {}),
            job.get("report_date", ""),
            base_tradelines=_base_tradelines_from_negatives(
                job.get("negatives_by_bureau", {})
            ) or None,
        )

    report_date = job.get("report_date", "")
    target_round = body.round or "round_1"

    def _eoscar_ok(text: str) -> dict:
        v = validate_eoscar_compliance(text, letter_type="furnisher_dispute")
        c = v["checks"]
        critical_ok = c["ascii"]["pass"] and not c["forbidden_phrases"]["found"]
        return {"critical_ok": critical_ok, "passed": v["passed"], "score": v["score"],
                "ascii": c["ascii"]["pass"], "forbidden": c["forbidden_phrases"]["found"]}

    # e-OSCAR gate con reintentos (reshuffle por seed)
    seed = body.variation_seed or 0
    furnisher_letters = {}
    for attempt in range(5):
        try:
            furnisher_letters = build_furnisher_letter_engine(
                letter_input, consumer_name=body.consumer_name, report_date=report_date)
        except TypeError:
            # por si tu versión no acepta variation_seed/target_round: llamada básica
            furnisher_letters = build_furnisher_letter_engine(
                letter_input, consumer_name=body.consumer_name, report_date=report_date)
        any_fail = False
        for _f, rounds in furnisher_letters.items():
            txt = (rounds or {}).get(target_round, "")
            if txt and not _eoscar_ok(txt)["critical_ok"]:
                any_fail = True
        if not any_fail:
            break
        seed += 1

    sel = None
    if body.selected_furnishers:
        sel = {s.strip().lower() for s in body.selected_furnishers}

    letters_out, blocked = [], []
    for furnisher, rounds in (furnisher_letters or {}).items():
        if sel is not None and furnisher.strip().lower() not in sel:
            continue
        text = (rounds or {}).get(target_round, "")
        if not text:
            continue
        # sustitución opcional de la dirección del cliente
        if body.consumer_address:
            text = text.replace("[Address]", body.consumer_address)
        if body.consumer_city_state_zip:
            text = text.replace("[City, State ZIP]", body.consumer_city_state_zip)
        chk = _eoscar_ok(text)
        if not chk["critical_ok"]:
            blocked.append({"furnisher": furnisher, "round": target_round,
                            "reason": "eoscar_critical_fail",
                            "forbidden": chk["forbidden"], "ascii": chk["ascii"]})
            continue
        letters_out.append({
            "furnisher": furnisher,
            "round": target_round,
            "text": text,
            "eoscar": chk,
            # nota: [Collector Address] lo completa el operador (no está en el reporte)
            "needs_collector_address": "[Collector Address]" in text,
        })

    return {"letters": letters_out, "blocked": blocked, "job_id": body.job_id,
            "count": len(letters_out)}

# ═══════════════════════════════════════════════════════════════
#  PATH C — RESPUESTA DEL BURÓ
#
#  Cierra el ciclo de rondas. El operador pega (o sube) la carta de
#  investigación que mandó el buró y sale la carta de escalación que
#  corresponde, con el estatuto correcto para cada caso.
#
#  Hasta ahora el motor tenía los 6 sub-builders escritos y NINGUNO se
#  invocaba desde la web: faltaba la tabla de traducción entre los 5
#  `outcome` que devuelve parse_bureau_response y los 7 `response_type`
#  del dispatcher. Esa tabla ya vive dentro de original_parser
#  (classify_response_text_extended / plan_bureau_response_letters);
#  acá sólo se le abre la puerta.
#
#  Se emite UNA carta por response_type, no una por cuenta: cada tipo
#  invoca un estatuto distinto y mezclarlos hace que e-OSCAR procese
#  todo bajo el reason code más débil.
# ═══════════════════════════════════════════════════════════════

# Nombres de buró tal como pueden venir en letter_receipts.recipient_name
_BUREAU_ALIASES = {
    "transunion": ("transunion", "trans union", "tu"),
    "experian":   ("experian", "exp"),
    "equifax":    ("equifax", "eqf", "eq"),
}


def _normalizar_bureau(valor: str) -> str:
    v = (valor or "").strip().lower()
    for canon, alias in _BUREAU_ALIASES.items():
        if v == canon or v in alias:
            return canon
    for canon, alias in _BUREAU_ALIASES.items():
        if any(a in v for a in alias):
            return canon
    return ""


def _fecha_de_entrega(job_id: str, bureau: str) -> str:
    """
    Fecha desde la que corre el reloj del 1681i para ese buró.

    El plazo de 30 días arranca cuando el buró RECIBE, no cuando el cliente
    despacha. Por eso se prefiere return_receipt_date y sólo se cae a
    date_sent si el acuse todavía no se cargó.
    """
    try:
        r = (sb.table("letter_receipts")
             .select("recipient_name, recipient_type, return_receipt_date, date_sent, created_at")
             .eq("job_id", job_id).execute())
    except Exception as e:
        print(f"[bureau-response] no se pudieron leer los recibos: {e}")
        return ""
    candidatos = []
    for row in (r.data or []):
        if _normalizar_bureau(row.get("recipient_name", "")) != bureau:
            continue
        fecha = row.get("return_receipt_date") or row.get("date_sent") or ""
        if fecha:
            candidatos.append(str(fecha)[:10])
    # El más reciente: si hubo varias rondas, interesa la última enviada.
    return max(candidatos) if candidatos else ""


def _borradas_en_rondas_previas(job: dict, bureau: str) -> set:
    """
    Nombres de furnisher que un buró ya había borrado antes.

    Es lo que permite detectar REINSERCIÓN, que es el ataque de mayor
    valor del sistema: un dato borrado que reaparece viola
    1681i(a)(5)(B)(ii) aunque el buró ahora diga que lo verificó. No se
    puede deducir leyendo la carta actual: hace falta el historial.

    Sale de response_history, que ya es una columna de api_jobs.
    """
    borradas = set()
    for ev in (job.get("response_history") or []):
        if not isinstance(ev, dict) or ev.get("type") != "bureau_response":
            continue
        if _normalizar_bureau(ev.get("bureau", "")) != bureau:
            continue
        for nombre in (ev.get("deleted_accounts") or []):
            if nombre:
                borradas.add(str(nombre).upper())
    return borradas


def _cuentas_del_job(job: dict, bureau: str) -> list:
    """Todas las cuentas conocidas del job, las del buró primero."""
    negativos = job.get("negatives_by_bureau", {}) or {}
    lie = job.get("letter_input_engine", {}) or {}

    ataque_por_nombre = {}
    for b, grupos in lie.items():
        for items in (grupos or {}).values():
            for it in items or []:
                if isinstance(it, dict) and it.get("furnisher_name") and it.get("attack_type"):
                    ataque_por_nombre.setdefault(
                        (b, str(it["furnisher_name"]).upper()), it["attack_type"])

    orden = [bureau] + [b for b in negativos if b != bureau]
    salida = []
    for b in orden:
        for acc in (negativos.get(b) or []):
            if not isinstance(acc, dict) or not acc.get("name"):
                continue
            salida.append({
                "furnisher_name": acc.get("name", ""),
                "name":           acc.get("name", ""),
                "account_number": acc.get("account_number", ""),
                "balance":        acc.get("balance", ""),
                "date_opened":    acc.get("date_opened", ""),
                "payment_status": acc.get("payment_status", ""),
                "status":         acc.get("status", ""),
                "attack_type":    ataque_por_nombre.get((b, str(acc["name"]).upper()), ""),
                "bureau":         b,
                "_mismo_buro":    b == bureau,
            })
    return salida


def _lookup_de_cuentas(job: dict, bureau: str, nombres_en_respuesta) -> dict:
    """
    Nombre tal como lo escribe el BURÓ -> cuenta real del reporte.

    El buró casi nunca copia el nombre textual del reporte. En el reporte
    de Genesis, por ejemplo, TransUnion lista
    "LVNV FUNDING (Original Creditor: 12 CREDIT ONE BANK N A)" y la carta
    de respuesta dice "LVNV FUNDING LLC". Con igualdad exacta no matchea y
    la carta sale con "Account #:" en blanco, que es justo el dato que el
    buró necesita para identificar la cuenta.

    Por eso se resuelve en tres pasos, de más estricto a menos:
      1. igualdad exacta en mayúsculas
      2. misma identidad de furnisher, con la misma lógica que usa el
         detector cross-bureau: ignora sufijos societarios, expande
         abreviaturas y descarta la anotación "(Original Creditor: X)"
      3. si no hay match, se deja sin resolver y la carta sale sólo con el
         nombre. Nunca se adivina un número de cuenta.

    Se prefiere siempre la cuenta del mismo buró que respondió.
    """
    try:
        from original_parser import _same_furnisher_identity
    except ImportError:
        _same_furnisher_identity = None

    candidatas = _cuentas_del_job(job, bureau)
    por_nombre_exacto = {}
    for c in candidatas:
        por_nombre_exacto.setdefault(str(c["name"]).upper(), c)

    lookup, sin_resolver = {}, []
    for nombre in (nombres_en_respuesta or []):
        clave = str(nombre).upper()
        elegida = por_nombre_exacto.get(clave)
        if elegida is None and _same_furnisher_identity is not None:
            for c in candidatas:            # ya vienen con el buró propio primero
                try:
                    if _same_furnisher_identity(nombre, c["name"]):
                        elegida = c
                        break
                except Exception:
                    continue
        if elegida is None:
            sin_resolver.append(nombre)
            continue
        lookup[clave] = {k: v for k, v in elegida.items() if not k.startswith("_")}
    if sin_resolver:
        print(f"[bureau-response] sin cuenta en el reporte: {sin_resolver}")
    return lookup


class BureauResponseBody(BaseModel):
    job_id: str
    bureau: str                                   # transunion | experian | equifax
    response_text: str                            # texto de la carta del buró
    response_date: Optional[str] = ""             # fecha que figura en la carta
    dispute_date: Optional[str] = ""              # acuse de recibo; si falta se busca solo
    consumer_name: Optional[str] = None           # si falta, sale del job
    round: Optional[str] = "round_2"


def _procesar_respuesta_de_buro(body: "BureauResponseBody", user: dict) -> dict:
    from original_parser import (
        classify_response_text_extended,
        plan_bureau_response_letters,
        build_bureau_response_letter,
        validate_eoscar_compliance,
    )

    job = _get_job_or_404(user, body.job_id)

    bureau = _normalizar_bureau(body.bureau)
    if not bureau:
        raise HTTPException(400, "bureau debe ser transunion, experian o equifax")
    if not (body.response_text or "").strip():
        raise HTTPException(400, "response_text viene vacío")

    consumer_name = body.consumer_name or job.get("consumer_name") or ""
    report_date   = job.get("report_date", "")

    # 1) Clasificar. Esto agrega frivolous y unable_to_process, que
    #    parse_bureau_response solo no detecta y manda a "other".
    parsed = classify_response_text_extended(body.response_text)

    # 2) Las dos reglas que no salen del texto de la carta.
    dispute_date = (body.dispute_date or "").strip() or _fecha_de_entrega(body.job_id, bureau)
    prev_borradas = _borradas_en_rondas_previas(job, bureau)
    hoy = datetime.now(timezone.utc).date().isoformat()

    # 3) Planificar: una carta por response_type, ordenadas por urgencia.
    planes = plan_bureau_response_letters(
        parsed,
        bureau=bureau,
        previously_deleted_names=prev_borradas,
        dispute_date=dispute_date,
        today=hoy,
        account_lookup=_lookup_de_cuentas(
            job, bureau, list((parsed.get("accounts") or {}).keys())
        ),
    )
    print(f"[bureau-response] job={body.job_id} bureau={bureau} "
          f"cuentas={len(parsed.get('accounts') or {})} "
          f"nivel_carta={parsed.get('letter_level_outcome') or '-'} "
          f"dispute_date={dispute_date or '-'} "
          f"prev_borradas={len(prev_borradas)} planes={len(planes)}")

    # 4) Generar cada carta y pasarla por el mismo gate crítico que las demás.
    raw_report_text = _raw_report_text_for_job(job)
    generadas, bloqueadas = [], []
    textos = []
    for p in planes:
        try:
            res = build_bureau_response_letter(
                consumer_name=consumer_name,
                response_date=(body.response_date or "").strip(),
                report_date=report_date,
                **p["call"],
            )
        except Exception as e:
            print(f"[bureau-response] fallo generando {p['response_type']}: {e}")
            bloqueadas.append({"response_type": p["response_type"],
                               "reason": "generation_error", "detail": str(e)[:300]})
            continue
        texto = (res or {}).get("letter", "") if isinstance(res, dict) else str(res or "")
        if not texto.strip():
            bloqueadas.append({"response_type": p["response_type"], "reason": "empty_letter"})
            continue
        textos.append(texto)
        generadas.append((p, res, texto))

    letters_out = []
    for p, res, texto in generadas:
        v = validate_eoscar_compliance(
            texto,
            raw_report_text=raw_report_text,
            other_letters=[t for t in textos if t != texto] or None,
            letter_type="bureau_dispute",
        )
        c = v["checks"]
        critical_ok = (c["ascii"]["pass"]
                       and not c["forbidden_phrases"]["found"]
                       and c.get("fidelity_to_report", {}).get("pass", True))
        if not critical_ok:
            bloqueadas.append({
                "response_type": p["response_type"],
                "reason": "eoscar_critical_fail",
                "forbidden": c["forbidden_phrases"]["found"],
                "fidelity": c.get("fidelity_to_report", {}),
            })
            continue
        letters_out.append({
            "bureau":        bureau,
            "response_type": p["response_type"],
            "round":         body.round or "round_2",
            "text":          texto,
            "next_steps":    (res or {}).get("next_steps", "") if isinstance(res, dict) else "",
            "account_names": p["account_names"],
            "account_count": len(p["accounts"]),
            # por qué esta carta y no otra: qué estatuto se invoca y por qué
            "why":                 p["why"],
            "needs_manual_review": p["needs_manual_review"],
            "eoscar": {"passed": v["passed"], "score": v["score"],
                       "ascii": c["ascii"]["pass"],
                       "forbidden": c["forbidden_phrases"]["found"],
                       "overlap": c.get("overlap", {}).get("detail", "")},
        })

    # 5) Historial. Se guarda qué borró el buró para que la PRÓXIMA ronda
    #    pueda detectar reinserción. Sin esto, reinsertion nunca dispara.
    borradas_ahora = sorted({
        str(nombre).upper()
        for nombre, data in (parsed.get("accounts") or {}).items()
        if (data.get("outcome_extended") or data.get("outcome")) == "deleted"
    })
    evento = {
        "type": "bureau_response",
        "bureau": bureau,
        "round": body.round or "round_2",
        "response_date": (body.response_date or "").strip(),
        "dispute_date": dispute_date,
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "letter_level_outcome": parsed.get("letter_level_outcome", ""),
        "outcomes": {
            str(n): (d.get("outcome_extended") or d.get("outcome", ""))
            for n, d in (parsed.get("accounts") or {}).items()
        },
        "deleted_accounts": borradas_ahora,
        "response_types": [l["response_type"] for l in letters_out],
    }
    try:
        hist = list(job.get("response_history") or [])
        hist.append(evento)
        actualizacion = {"response_history": hist}

        # Las cartas también van a letter_files, igual que las de disputa,
        # para que no se pierdan al cerrar la respuesta.
        previas = list(job.get("letter_files") or [])
        claves_nuevas = {(l["bureau"], l["response_type"], l["round"]) for l in letters_out}
        acumuladas = [
            lf for lf in previas
            if (lf.get("bureau"), lf.get("response_type"), lf.get("round")) not in claves_nuevas
        ]
        for l in letters_out:
            acumuladas.append({
                "bureau":        l["bureau"],
                "category":      "bureau_response",
                "response_type": l["response_type"],
                "round":         l["round"],
                "filename":      f"{l['bureau']}_response_{l['response_type']}_{l['round']}.txt",
                "text":          l["text"],
                "generated_at":  evento["recorded_at"],
                "eoscar_score":  l["eoscar"]["score"],
            })
        if letters_out:
            actualizacion["letter_files"] = acumuladas
            actualizacion["letter_count"] = len(acumuladas)
        sb.table("api_jobs").update(
            _solo_columnas_de_api_jobs(actualizacion)
        ).eq("job_id", body.job_id).execute()
    except Exception as e:
        # Guardar el historial no puede impedir devolverle las cartas al operador.
        print(f"[bureau-response] no se pudo guardar el historial: {e}")

    return {
        "job_id":  body.job_id,
        "bureau":  bureau,
        "letters": letters_out,
        "blocked": bloqueadas,
        "count":   len(letters_out),
        "classification": {
            "letter_level_outcome": parsed.get("letter_level_outcome", ""),
            "letter_level_evidence": parsed.get("letter_level_evidence", ""),
            "outcomes": evento["outcomes"],
            "deleted_accounts": borradas_ahora,
        },
        "dispute_date_usada": dispute_date,
        "reinsertion_detectada": any(
            l["response_type"] == "reinsertion" for l in letters_out
        ),
        "nota": (
            "dispute_date es la fecha del acuse de recibo: el plazo del 1681i "
            "corre desde que el buro RECIBE, no desde el despacho."
            if dispute_date else
            "Sin fecha de entrega no se puede evaluar el vencimiento de los 30 dias. "
            "Cargá return_receipt_date en el recibo o pasá dispute_date."
        ),
    }


@app.post("/bureau-response")
async def bureau_response(body: BureauResponseBody, user=Depends(get_current_user)):
    _enforce_round(user, body.round)   # las respuestas son R2+; Básica no las tiene
    return _procesar_respuesta_de_buro(body, user)


class BureauNoResponseBody(BaseModel):
    job_id: str
    bureau: str
    dispute_date: Optional[str] = ""     # acuse de recibo; si falta se busca solo
    consumer_name: Optional[str] = None
    round: Optional[str] = "round_2"
    extended_deadline: Optional[bool] = False   # 45 dias si el consumidor aporto info


@app.post("/bureau-no-response")
async def bureau_no_response(body: BureauNoResponseBody, user=Depends(get_current_user)):
    """
    El buró nunca contestó. No hay carta que pegar, así que este caso no
    puede salir de /bureau-response: sale del calendario.

    15 U.S.C. section 1681i(a)(1)(A): 30 días desde que el buró RECIBE la
    disputa, 45 si el consumidor aportó información adicional durante el
    período. Vencido el plazo sin respuesta, la eliminación procede como
    cuestión de derecho bajo 1681i(a)(5)(A).

    Si todavía estás dentro del plazo NO genera nada y te dice cuántos días
    faltan: mandar la carta antes de tiempo debilita el reclamo.
    """
    _enforce_round(user, body.round)
    from original_parser import (
        resolve_response_type, build_bureau_response_letter,
        validate_eoscar_compliance,
    )

    job = _get_job_or_404(user, body.job_id)
    bureau = _normalizar_bureau(body.bureau)
    if not bureau:
        raise HTTPException(400, "bureau debe ser transunion, experian o equifax")

    dispute_date = (body.dispute_date or "").strip() or _fecha_de_entrega(body.job_id, bureau)
    if not dispute_date:
        raise HTTPException(
            400,
            "No hay fecha de entrega para ese buro. El plazo del 1681i corre "
            "desde que el buro RECIBE, asi que sin esa fecha no se puede "
            "afirmar que vencio. Carga return_receipt_date en el recibo o "
            "pasa dispute_date."
        )

    hoy = datetime.now(timezone.utc).date().isoformat()
    veredicto = resolve_response_type(
        "", response_received=False, dispute_date=dispute_date,
        today=hoy, extended_deadline=bool(body.extended_deadline),
    )
    if veredicto["response_type"] != "no_response_30_days":
        limite = 45 if body.extended_deadline else 30
        pasados = veredicto.get("days_elapsed")
        return {
            "job_id": body.job_id, "bureau": bureau, "letters": [], "count": 0,
            "days_elapsed": pasados,
            "days_remaining": (limite - pasados) if isinstance(pasados, int) else None,
            "nota": veredicto["why"],
        }

    # Las cuentas que se disputaron en esa ronda a ese buró.
    cuentas = []
    for grupo, items in ((job.get("letter_input_engine", {}) or {}).get(bureau, {}) or {}).items():
        for it in items or []:
            if not isinstance(it, dict) or not it.get("furnisher_name"):
                continue
            cuentas.append({
                "furnisher_name": it.get("furnisher_name", ""),
                "name":           it.get("furnisher_name", ""),
                "account_number": it.get("account_number", ""),
                "attack_type":    it.get("attack_type", ""),
                "balance":        it.get("balance", ""),
                "date_opened":    it.get("date_opened", ""),
                "bureau":         bureau,
            })
    if not cuentas:
        raise HTTPException(
            400,
            "No hay cuentas disputadas registradas para ese buro en este job, "
            "asi que no se sabe que items reclamar. Genera primero las cartas "
            "de disputa."
        )

    consumer_name = body.consumer_name or job.get("consumer_name") or ""
    res = build_bureau_response_letter(
        response_type="no_response_30_days",
        bureau=bureau,
        accounts=cuentas,
        consumer_name=consumer_name,
        report_date=job.get("report_date", ""),
        dispute_date=dispute_date,
    )
    texto = (res or {}).get("letter", "") if isinstance(res, dict) else str(res or "")
    v = validate_eoscar_compliance(
        texto, raw_report_text=_raw_report_text_for_job(job),
        letter_type="bureau_dispute",
    )
    c = v["checks"]
    if not (c["ascii"]["pass"] and not c["forbidden_phrases"]["found"]
            and c.get("fidelity_to_report", {}).get("pass", True)):
        return {"job_id": body.job_id, "bureau": bureau, "letters": [], "count": 0,
                "blocked": [{"response_type": "no_response_30_days",
                             "reason": "eoscar_critical_fail",
                             "forbidden": c["forbidden_phrases"]["found"],
                             "fidelity": c.get("fidelity_to_report", {})}]}

    carta = {
        "bureau": bureau, "response_type": "no_response_30_days",
        "round": body.round or "round_2", "text": texto,
        "next_steps": (res or {}).get("next_steps", "") if isinstance(res, dict) else "",
        "account_names": [a["furnisher_name"] for a in cuentas],
        "account_count": len(cuentas),
        "why": veredicto["why"],
        "needs_manual_review": False,
        "eoscar": {"passed": v["passed"], "score": v["score"]},
    }

    try:
        ahora = datetime.now(timezone.utc).isoformat()
        hist = list(job.get("response_history") or [])
        hist.append({
            "type": "bureau_response", "bureau": bureau,
            "round": body.round or "round_2", "recorded_at": ahora,
            "dispute_date": dispute_date,
            "letter_level_outcome": "no_response_30_days",
            "days_elapsed": veredicto.get("days_elapsed"),
            "outcomes": {}, "deleted_accounts": [],
            "response_types": ["no_response_30_days"],
        })
        previas = [
            lf for lf in (job.get("letter_files") or [])
            if not (lf.get("bureau") == bureau
                    and lf.get("response_type") == "no_response_30_days"
                    and lf.get("round") == carta["round"])
        ]
        previas.append({
            "bureau": bureau, "category": "bureau_response",
            "response_type": "no_response_30_days", "round": carta["round"],
            "filename": f"{bureau}_response_no_response_{carta['round']}.txt",
            "text": texto, "generated_at": ahora, "eoscar_score": v["score"],
        })
        sb.table("api_jobs").update(_solo_columnas_de_api_jobs({
            "response_history": hist,
            "letter_files": previas,
            "letter_count": len(previas),
        })).eq("job_id", body.job_id).execute()
    except Exception as e:
        print(f"[bureau-no-response] no se pudo guardar el historial: {e}")

    return {
        "job_id": body.job_id, "bureau": bureau,
        "letters": [carta], "blocked": [], "count": 1,
        "days_elapsed": veredicto.get("days_elapsed"),
        "dispute_date_usada": dispute_date,
        "nota": veredicto["why"],
    }


@app.post("/bureau-response/pdf")
async def bureau_response_pdf(
    file: UploadFile = File(...),
    job_id: str = Form(...),
    bureau: str = Form(...),
    response_date: str = Form(""),
    dispute_date: str = Form(""),
    consumer_name: str = Form(""),
    round: str = Form("round_2"),
    user=Depends(get_current_user),
):
    """Igual que /bureau-response pero subiendo el PDF que mandó el buró."""
    _enforce_round(user, round)
    ruta = os.path.join(UPLOAD_DIR, f"bureau_response_{uuid.uuid4()}.pdf")
    with open(ruta, "wb") as f:
        shutil.copyfileobj(file.file, f)
    try:
        import pdfplumber
        with pdfplumber.open(ruta) as pdf:
            texto = "\n".join((p.extract_text() or "") for p in pdf.pages)
    except Exception as e:
        raise HTTPException(400, f"No se pudo leer el PDF de la respuesta: {e}")
    finally:
        try:
            os.remove(ruta)
        except OSError:
            pass
    if not texto.strip():
        raise HTTPException(400, "El PDF no tiene texto extraible. "
                                 "Si es un escaneo, pegá el texto a mano.")
    return _procesar_respuesta_de_buro(BureauResponseBody(
        job_id=job_id, bureau=bureau, response_text=texto,
        response_date=response_date, dispute_date=dispute_date,
        consumer_name=consumer_name or None, round=round,
    ), user)

# ═══════════════════════════════════════════════════════════════
#  TASK 2 — POSTALOCITY INTEGRATION (USPS Certified Mail)
#  Envía una carta ya generada por Certified Mail vía Postalocity.
#  Se detiene en la COTIZACIÓN (no aprueba / no cobra / no manda).
#  Requiere: postalocity_dispatch.py en la misma carpeta, reportlab,
#  y env vars POSTALOCITY_ENV(dev|prod)/POSTALOCITY_USER/POSTALOCITY_PASS.
#  Probar SIEMPRE primero con POSTALOCITY_ENV=dev (no cobra).
# ═══════════════════════════════════════════════════════════════

class DispatchLetterBody(BaseModel):
    job_id: str
    letter_text: str                       # el texto de la carta (de /generate-letters)
    client_id: str                         # remitente = el cliente (dinámico por job)
    recipient_name: str                    # destinatario (buró o collector)
    recipient_line1: str
    recipient_line2: Optional[str] = ""
    recipient_city: str
    recipient_state: str
    recipient_zip: str
    recipient_type: Optional[str] = None   # 'bureau' | 'furnisher' (para el recibo)
    round: Optional[str] = None            # round_1 | round_2 | round_3 (para el recibo)
    letter_name: Optional[str] = None      # nombre para el job en Postalocity (igual al del PDF)
    save_receipt: Optional[bool] = True    # crea fila en letter_receipts con el postalocity_job_id

def _letter_text_to_pdf(text: str, out_path: str) -> str:
    """Renderiza el texto de la carta a un PDF mailable (Times-Roman 11pt).
    Usa el generador de PDF puro del módulo de Postalocity (sin reportlab),
    para no depender de librerías que puedan faltar en el deploy."""
    from postalocity_dispatch import write_text_pdf
    return write_text_pdf(out_path, text or "", size=11, margin=72, leading=15, wrap=95)

@app.post("/dispatch-letter")
async def dispatch_letter(body: DispatchLetterBody, user=Depends(get_current_user)):
    _enforce_round(user, body.round)   # plan requerido; Básica = solo Round 1
    # 1) remitente = el cliente (dirección dinámica por job)
    cl = _get_client_or_404(user, body.client_id)

    # 2) validar que la carta no tenga placeholders de dirección sin resolver
    if "[Address]" in body.letter_text or "[Collector Address]" in body.letter_text:
        raise HTTPException(400, "Letter still contains address placeholders. "
                                 "Fill the client address and recipient address before mailing.")

    # 3) La carta se envía TAL CUAL (formato completo: cliente arriba, buró debajo,
    #    cuerpo) — NO se modifica. Fijamos el addressZone justo sobre el bloque del
    #    buró (coordenadas exactas medidas del render real) para que Postalocity lea
    #    el buró como destino, no el cliente. Render en Helvetica (nítido para OCR).
    from postalocity_dispatch import Address, send_certified_letter, write_text_pdf

    # Credenciales de Postalocity de LA AGENCIA (multi-cuenta). OBLIGATORIO:
    # si la agencia NO conectó su propia cuenta, NO se permite enviar (sin
    # fallback a la cuenta global). Se valida ANTES de crear/despachar nada.
    pu, pp, penv = _agency_postalocity_creds(user["id"])
    if not pu:
        raise HTTPException(400, "Conecta tu cuenta de Postalocity antes de enviar "
                                 "(Ajustes → Postalocity). Cada envío sale desde tu "
                                 "propia cuenta.")

    # Nombre del job en Postalocity: usar el que manda el frontend (mismo que el
    # nombre del PDF al descargarlo). Si no viene, se arma uno con cliente+buró+ronda.
    def _slug(s):
        return re.sub(r"[^A-Za-z0-9]+", "_", str(s or "")).strip("_")
    job_name = (body.letter_name or "").strip()
    if not job_name:
        parts = [cl.get("full_name", ""), body.recipient_name, body.round or ""]
        job_name = "_".join(_slug(p) for p in parts if p) or f"Letter_{body.job_id}"

    pdf_path = os.path.join(UPLOAD_DIR, f"dispatch_{body.job_id}_{uuid.uuid4().hex[:8]}.pdf")
    write_text_pdf(pdf_path, body.letter_text, font="Helvetica", size=11)

    # 4) despachar por Postalocity (se detiene en la cotización, no aprueba/paga)
    try:
        sender = Address(
            cl.get("full_name", ""),
            cl.get("address", ""),
            cl.get("city", ""),
            cl.get("state", ""),
            cl.get("zip_code", ""),
        )
        # recipient=None: la dirección la lee Postalocity de la propia carta (addressZone).
        result = send_certified_letter(pdf_path, sender=sender, recipient=None,
                                       user=pu, password=pp, env=penv,
                                       job_name=job_name)
    except Exception as e:
        raise HTTPException(502, f"Postalocity dispatch failed: {e}\n{traceback.format_exc()[:800]}")

    pjob = result.get("job_id") if isinstance(result, dict) else None

    # 5) guardar historial en el job (opcional, no rompe si falla)
    try:
        job = sb.table("api_jobs").select("response_history").eq("job_id", body.job_id).execute()
        hist = (job.data[0].get("response_history") if job.data else []) or []
        hist.append({"type": "postalocity_dispatch", "recipient": body.recipient_name,
                     "result": result if isinstance(result, dict) else str(result)})
        sb.table("api_jobs").update({"response_history": hist}).eq("job_id", body.job_id).execute()
    except Exception:
        pass

    # 6) crear fila en letter_receipts con el postalocity_job_id (Tarea 3).
    #    El tracking / date_sent se cargan luego, al aprobar y enviar.
    receipt = None
    if body.save_receipt:
        try:
            data = {
                "client_id": body.client_id,
                "job_id": body.job_id,
                "round": body.round,
                "recipient_type": body.recipient_type,
                "recipient_name": body.recipient_name,
                "postalocity_job_id": str(pjob) if pjob is not None else None,
                "operator_id": user["id"],
            }
            data = {k: v for k, v in data.items() if v is not None}
            r = sb.table("letter_receipts").insert(data).execute()
            receipt = r.data[0] if r.data else None
        except Exception:
            receipt = None

    # NOTA: llega hasta la cotización. La APROBACIÓN/PAGO es un paso manual
    # aparte (dashboard de Postalocity), NO se hace en este endpoint.
    return {"status": "quoted", "quote": result, "postalocity_job_id": pjob,
            "receipt": receipt, "note":
            "Stopped at quote. Approve & pay manually in the Postalocity dashboard."}

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
    job = _get_job_or_404(user, job_id)
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
    _get_client_or_404(user, client_id)
    jobs = _client_jobs_ordered(client_id)
    if len(jobs) < 2:
        raise HTTPException(400, "Need at least two reports (rounds) to build a progress report.")
    prev, now = jobs[-2], jobs[-1]
    out = compute_progress(prev, now)
    out["round_prev"] = len(jobs) - 1
    out["round_now"] = len(jobs)
    return out


# ═══════════════════════════════════════════════════════════════
#  CIR SUMMARY PDF  (requiere reportlab en requirements.txt)
#  Pegar después del endpoint /progress (antes de la sección PORTAL).
# ═══════════════════════════════════════════════════════════════
import io as _cir_io

def build_cir_summary_pdf(cir: dict) -> bytes:
    from reportlab.lib.pagesizes import LETTER
    from reportlab.lib.units import inch
    from reportlab.lib.colors import HexColor
    from reportlab.pdfgen import canvas as _canvas
    NAVY = HexColor('#00364F'); GOLD = HexColor('#C9A24B'); CREAM = HexColor('#F4EEE2')
    INK = HexColor('#1b2b34'); MUT = HexColor('#5c6975'); REDC = HexColor('#8C2A2A')
    BUR = ('transunion', 'experian', 'equifax')
    BLAB = {'transunion': 'TRANSUNION', 'experian': 'EXPERIAN', 'equifax': 'EQUIFAX'}
    buf = _cir_io.BytesIO()
    c = _canvas.Canvas(buf, pagesize=LETTER)
    W, H = LETTER
    M = 0.8 * inch

    def line(x, y, txt, font="Helvetica", size=10, color=INK):
        c.setFillColor(color); c.setFont(font, size); c.drawString(x, y, txt)

    def wrap(x, y, txt, width, font="Helvetica", size=9.5, color=INK, leading=13):
        c.setFillColor(color); c.setFont(font, size)
        linew = ""
        for w in (txt or "").split():
            t = (linew + " " + w).strip()
            if c.stringWidth(t, font, size) <= width:
                linew = t
            else:
                c.drawString(x, y, linew); y -= leading; linew = w
        if linew:
            c.drawString(x, y, linew); y -= leading
        return y

    # header band
    c.setFillColor(NAVY); c.rect(0, H - 1.35 * inch, W, 1.35 * inch, stroke=0, fill=1)
    c.setFillColor(GOLD); c.rect(0, H - 1.39 * inch, W, 0.04 * inch, stroke=0, fill=1)
    line(M, H - 0.62 * inch, "REPORT DEFENCE", "Helvetica-Bold", 15, CREAM)
    line(M, H - 0.82 * inch, "PROTECTING YOUR CREDIT. DEFENDING YOUR FUTURE.", "Helvetica", 7.5, GOLD)
    c.setFillColor(GOLD); c.setFont("Helvetica-Bold", 10)
    c.drawRightString(W - M, H - 0.6 * inch, "CREDIT INVESTIGATION REPORT")
    c.setFillColor(CREAM); c.setFont("Helvetica", 9)
    c.drawRightString(W - M, H - 0.78 * inch, "Summary")
    c.drawRightString(W - M, H - 0.96 * inch, f"Round {cir.get('round', 1)}")

    y = H - 1.72 * inch
    line(M, y, f"Client: {cir.get('client', '')}", "Helvetica-Bold", 12, INK)
    c.setFillColor(MUT); c.setFont("Helvetica", 9)
    c.drawRightString(W - M, y, f"Source: {str(cir.get('source', '')).upper()}   -   Report date: {cir.get('report_date', '')}")
    y -= 0.34 * inch

    # scores
    sc = cir.get("scores", {})
    cw = (W - 2 * M - 2 * (0.2 * inch)) / 3.0
    x = M
    for b in BUR:
        s = sc.get(b, {})
        c.setLineWidth(1); c.setStrokeColor(HexColor("#d8dce0")); c.setFillColor(HexColor("#faf7f0"))
        c.roundRect(x, y - 0.9 * inch, cw, 0.9 * inch, 8, stroke=1, fill=1)
        c.setFillColor(GOLD); c.rect(x, y - 0.06 * inch, cw, 0.06 * inch, stroke=0, fill=1)
        c.setFillColor(MUT); c.setFont("Helvetica-Bold", 8); c.drawCentredString(x + cw / 2, y - 0.28 * inch, BLAB[b])
        c.setFillColor(NAVY); c.setFont("Helvetica-Bold", 26); c.drawCentredString(x + cw / 2, y - 0.66 * inch, str(s.get("score", "")))
        c.setFillColor(GOLD); c.setFont("Helvetica-Bold", 8.5); c.drawCentredString(x + cw / 2, y - 0.82 * inch, str(s.get("rating", "")))
        x += cw + 0.2 * inch
    y -= 1.22 * inch

    def head(label):
        nonlocal y
        line(M, y, label, "Helvetica-Bold", 10, GOLD); y -= 0.06 * inch
        c.setStrokeColor(GOLD); c.setLineWidth(1.2); c.line(M, y, M + 0.9 * inch, y); y -= 0.2 * inch

    head("EXECUTIVE SUMMARY")
    y = wrap(M, y, cir.get("executive_summary", ""), W - 2 * M, size=9.5, leading=13) - 0.16 * inch

    counts = cir.get("counts", {}); total = cir.get("total_findings", 0)
    head("FINDINGS")
    line(M, y, f"{total} negative findings   -   TransUnion {counts.get('transunion', 0)}   -   "
               f"Experian {counts.get('experian', 0)}   -   Equifax {counts.get('equifax', 0)}",
         "Helvetica-Bold", 9.5, INK)
    y -= 0.2 * inch
    findings = cir.get("findings", [])
    for f in findings[:6]:
        c.setFillColor(MUT); c.setFont("Helvetica", 8.5)
        bal = f"  {f.get('balance', '')}" if f.get('balance') else ""
        c.drawString(M + 0.1 * inch, y, f"- [{str(f.get('bureau', ''))[:3].upper()}] {f.get('type', '')}: {f.get('furnisher', '')}{bal}")
        y -= 0.155 * inch
    if len(findings) > 6:
        c.setFillColor(MUT); c.setFont("Helvetica-Oblique", 8)
        c.drawString(M + 0.1 * inch, y, f"+ {len(findings) - 6} more (see full report)"); y -= 0.155 * inch
    y -= 0.1 * inch

    risks = cir.get("risks", [])[:5]
    if risks and y > 2.1 * inch:
        head("KEY RISKS & ANGLES")
        for r in risks:
            dot = REDC if (r.get("severity") or "").lower() == "high" else GOLD
            c.setFillColor(dot); c.circle(M + 0.06 * inch, y + 0.03 * inch, 0.035 * inch, stroke=0, fill=1)
            line(M + 0.2 * inch, y, r.get("title", ""), "Helvetica-Bold", 9, INK)
            y -= 0.185 * inch

    c.setFillColor(HexColor("#eef0f2")); c.rect(0, 0, W, 0.7 * inch, stroke=0, fill=1)
    wrap(M, 0.5 * inch, cir.get("disclaimer", ""), W - 2 * M, size=7, color=MUT, leading=9)

    c.showPage(); c.save()
    return buf.getvalue()


@app.get("/cir/{job_id}/pdf")
async def get_cir_pdf(job_id: str, user=Depends(get_current_user)):
    job = _get_job_or_404(user, job_id)
    round_num = 1
    cid = job.get("client_id")
    if cid:
        ordered = _client_jobs_ordered(cid)
        for idx, jj in enumerate(ordered, 1):
            if jj.get("job_id") == job_id:
                round_num = idx
                break
    cir = compose_cir(job, round_num=round_num)
    pdf = build_cir_summary_pdf(cir)
    safe = (cir.get("client") or "client").replace(" ", "_")
    fname = f"CIR_Summary_{safe}_R{round_num}.pdf"
    return StreamingResponse(
        _cir_io.BytesIO(pdf),
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


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


# ═══════════════════════════════════════════════════════════════
#  POSTALOCITY MULTI-CUENTA (por agencia)
#  Cada agencia conecta su propia cuenta de Postalocity. La clave se
#  guarda ENCRIPTADA (Fernet). Requiere:
#   - tabla api_postalocity_accounts (ver api_postalocity_accounts.sql)
#   - env POSTALOCITY_ENC_KEY (clave Fernet)
#   - 'cryptography' instalado (agregar a requirements.txt si falta)
# ═══════════════════════════════════════════════════════════════

def _fernet():
    from cryptography.fernet import Fernet
    key = os.environ.get("POSTALOCITY_ENC_KEY", "")
    if not key:
        raise HTTPException(500, "POSTALOCITY_ENC_KEY no está configurada en el servidor.")
    return Fernet(key.encode())

def _enc_secret(text: str) -> str:
    return _fernet().encrypt((text or "").encode()).decode()

def _dec_secret(token: str) -> str:
    return _fernet().decrypt((token or "").encode()).decode()

def _agency_postalocity_creds(user_id):
    """Devuelve (user, password, env) de la agencia, o (None, None, None) si no conectó."""
    try:
        r = sb.table("api_postalocity_accounts").select("*").eq("user_id", user_id).execute()
        if not r.data:
            return (None, None, None)
        row = r.data[0]
        return (row.get("postalocity_user"),
                _dec_secret(row.get("postalocity_pass_enc")),
                row.get("env") or "prod")
    except Exception:
        return (None, None, None)

class PostalocityConnectBody(BaseModel):
    postalocity_user: str
    postalocity_pass: str
    env: Optional[str] = "prod"

@app.post("/postalocity/connect")
async def postalocity_connect(body: PostalocityConnectBody, user=Depends(get_current_user)):
    """Verifica las credenciales (login real) y las guarda encriptadas para la agencia."""
    from postalocity_dispatch import verify_credentials
    env = (body.env or "prod").lower()
    if env not in ("dev", "prod"):
        env = "prod"
    check = verify_credentials(body.postalocity_user, body.postalocity_pass, env)
    if not check.get("ok"):
        raise HTTPException(400, f"No pude validar la cuenta de Postalocity: {check.get('message')}")
    now = datetime.now(timezone.utc).isoformat()
    data = {
        "user_id": user["id"],
        "postalocity_user": body.postalocity_user,
        "postalocity_pass_enc": _enc_secret(body.postalocity_pass),
        "env": env,
        "connected_at": now,
        "last_verified_at": now,
    }
    sb.table("api_postalocity_accounts").upsert(data, on_conflict="user_id").execute()
    return {"connected": True, "postalocity_user": body.postalocity_user, "env": env}

@app.get("/postalocity/status")
async def postalocity_status(user=Depends(get_current_user)):
    """Dice si la agencia tiene cuenta conectada (usuario enmascarado, sin la clave)."""
    r = (sb.table("api_postalocity_accounts")
         .select("postalocity_user, env, connected_at, last_verified_at")
         .eq("user_id", user["id"]).execute())
    if not r.data:
        return {"connected": False}
    row = r.data[0]
    u = row.get("postalocity_user") or ""
    masked = (u[:2] + "***" + u[-2:]) if len(u) > 4 else "***"
    return {"connected": True, "postalocity_user_masked": masked,
            "env": row.get("env"), "connected_at": row.get("connected_at"),
            "last_verified_at": row.get("last_verified_at")}

@app.delete("/postalocity/disconnect")
async def postalocity_disconnect(user=Depends(get_current_user)):
    sb.table("api_postalocity_accounts").delete().eq("user_id", user["id"]).execute()
    return {"connected": False}

# ═══════════════════════════════════════════════════════════════
#  SINCRONIZACIÓN DE ESTADO DE ENVÍO (¿la carta ya se envió?)
#  Consulta el job en Postalocity con las credenciales de la agencia
#  y actualiza letter_receipts (mail_status / tracking / date_sent).
#  - POST /postalocity/sync-status  → manual (botón "Actualizar estado")
#  - POST /postalocity/sync-all?key=…  → todas las agencias (para el cron)
# ═══════════════════════════════════════════════════════════════

def _derive_mail_status(job) -> dict:
    """Deduce del job de Postalocity si la carta ya se envió.
    Señal fuerte = ya tiene tracking USPS o fecha de envío/producción.
    Antes de aprobar/pagar, el job NO tiene tracking (queda 'ready for approval')."""
    if not isinstance(job, dict):
        return {"status": "unknown", "sent": False, "state": None,
                "tracking": None, "mailed_date": None}
    tracking = (job.get("trackingNumber") or job.get("tracking")
                or job.get("certifiedTrackingNumber") or job.get("uspsTracking") or "")
    mailed_date = (job.get("mailedDate") or job.get("mailed_date")
                   or job.get("dateMailed") or job.get("productionDate") or "")
    state = job.get("state")
    status_str = str(job.get("status") or job.get("statusText") or "").lower()
    sent = bool(tracking) or bool(mailed_date) or any(
        k in status_str for k in ("mail", "production", "produced", "shipped",
                                   "complete", "sent", "in-transit", "delivered"))
    if sent:
        label = "sent"
    elif "approv" in status_str or state == 3:   # 3 = READY FOR APPROVAL (observado; calibrar)
        label = "ready_for_approval"
    else:
        label = "pending"
    return {"status": label, "sent": sent, "state": state,
            "tracking": tracking or None, "mailed_date": mailed_date or None}

def _sync_receipts_for_agency(user_id, pu, pp, penv) -> dict:
    """Consulta en Postalocity todas las cartas (letter_receipts) de esta agencia
    que tienen postalocity_job_id y aún no están marcadas como enviadas."""
    from postalocity_dispatch import PostalocityClient
    summary = {"checked": 0, "updated": 0, "sent": 0, "errors": 0, "not_found": 0}
    try:
        rows = (sb.table("letter_receipts").select("*")
                .eq("operator_id", user_id).execute()).data or []
    except Exception as e:
        summary["error"] = str(e)
        return summary
    # No re-consultar las ya enviadas ni las marcadas como no rastreables.
    DONE = ("sent", "no_encontrada")
    rows = [r for r in rows if r.get("postalocity_job_id")
            and r.get("mail_status") not in DONE]
    if not rows:
        return summary
    client = PostalocityClient(user=pu, password=pp, env=penv)
    now = datetime.now(timezone.utc).isoformat()
    # Mensajes que indican que el job NO existe o no pertenece a esta cuenta
    # (error permanente -> se marca de una vez, no se reintenta).
    PERMA = ("not found", "no such", "does not exist", "unauthorized",
             "forbidden", "invalid job", "no existe", "not owned",
             "access denied", "permission", "404")
    for row in rows:
        pid = str(row.get("postalocity_job_id") or "").strip()
        if not pid:
            continue
        summary["checked"] += 1
        try:
            if not pid.isdigit():
                raise ValueError("job id no numérico")
            job = client.get_job(int(pid))
            d = _derive_mail_status(job)
            upd = {"mail_status": d["status"], "last_synced_at": now}
            if d["state"] is not None:
                upd["mail_state"] = str(d["state"])
            if d["tracking"]:
                upd["tracking_number"] = d["tracking"]
            if d["sent"]:
                summary["sent"] += 1
                md = d["mailed_date"]
                upd["date_sent"] = (str(md)[:10] if md else now[:10])
            sb.table("letter_receipts").update(upd).eq("id", row["id"]).execute()
            summary["updated"] += 1
        except Exception as e:
            # Cuenta de intentos fallidos guardada en mail_state como "err:N".
            # Permanente (job inexistente/ajeno) o 3 fallos seguidos -> se marca
            # "no_encontrada" y deja de reintentarse. Un fallo transitorio (red)
            # solo suma un intento y se reintenta en la próxima corrida.
            msg = str(e).lower()
            prev = str(row.get("mail_state") or "")
            n = (int(prev[4:]) if prev.startswith("err:") and prev[4:].isdigit() else 0) + 1
            permanent = (not pid.isdigit()) or any(k in msg for k in PERMA)
            if permanent or n >= 3:
                summary["not_found"] += 1
                upd = {"mail_status": "no_encontrada",
                       "mail_state": f"err:{n}", "last_synced_at": now}
            else:
                summary["errors"] += 1
                upd = {"mail_state": f"err:{n}", "last_synced_at": now}
            try:
                sb.table("letter_receipts").update(upd).eq("id", row["id"]).execute()
            except Exception:
                pass
    return summary

@app.post("/postalocity/sync-status")
async def postalocity_sync_status(user=Depends(get_current_user)):
    """Botón 'Actualizar estado': consulta AHORA las cartas de esta agencia."""
    pu, pp, penv = _agency_postalocity_creds(user["id"])
    if not pu:
        raise HTTPException(400, "Conecta tu cuenta de Postalocity para actualizar estados.")
    summary = _sync_receipts_for_agency(user["id"], pu, pp, penv)
    return {"ok": True, **summary}

@app.post("/postalocity/sync-all")
async def postalocity_sync_all(key: str = ""):
    """Revisión automática de TODAS las agencias. Protegido por POSTALOCITY_SYNC_KEY.
    Pensado para un cron externo (Railway cron / cron-job.org) cada X horas."""
    expected = os.environ.get("POSTALOCITY_SYNC_KEY", "")
    if not expected or key != expected:
        raise HTTPException(403, "bad key")
    try:
        agencies = (sb.table("api_postalocity_accounts").select("*").execute()).data or []
    except Exception as e:
        raise HTTPException(500, f"No pude leer las cuentas de Postalocity: {e}")
    total = {"agencies": 0, "checked": 0, "updated": 0, "sent": 0,
             "errors": 0, "not_found": 0}
    for a in agencies:
        try:
            pu = a.get("postalocity_user")
            pp = _dec_secret(a.get("postalocity_pass_enc"))
            penv = a.get("env") or "prod"
        except Exception:
            total["errors"] += 1
            continue
        s = _sync_receipts_for_agency(a.get("user_id"), pu, pp, penv)
        total["agencies"] += 1
        for k in ("checked", "updated", "sent", "errors", "not_found"):
            total[k] += s.get(k, 0)
    return {"ok": True, **total}

# ═══════════════════════════════════════════════════════════════
#  REMINDERS AUTOMÁTICOS (seguimiento de 30 días)
#  No hay tabla aparte: se derivan de letter_receipts. En cuanto una
#  carta tiene tracking (ya se envió), aparece aquí con el conteo de
#  30 días. El reloj arranca en la fecha del return receipt si existe
#  (más precisa), o en date_sent (cuando se asignó el tracking).
#  Todo trabaja sobre los mismos datos: Dispute Letters, Certified
#  Mail y Reminders quedan siempre sincronizados.
# ═══════════════════════════════════════════════════════════════

WAIT_DAYS = 30

def _build_reminders(rows, names):
    from datetime import timedelta
    today = datetime.now(timezone.utc).date()
    out = []
    for r in rows:
        tracking = (r.get("tracking_number") or "").strip()
        if not tracking:
            continue  # solo cartas ya enviadas (con tracking) generan recordatorio
        start_s = r.get("return_receipt_date") or r.get("date_sent")
        if not start_s:
            continue
        try:
            start = datetime.fromisoformat(str(start_s)[:10]).date()
        except Exception:
            continue
        due = start + timedelta(days=WAIT_DAYS)
        day_x = (today - start).days + 1
        days_left = (due - today).days
        out.append({
            "receipt_id": r.get("id"),
            "client_id": r.get("client_id"),
            "client_name": names.get(r.get("client_id"), ""),
            "recipient_name": r.get("recipient_name"),
            "recipient_type": r.get("recipient_type"),
            "round": r.get("round"),
            "tracking_number": tracking,
            "start_date": start.isoformat(),
            "due_date": due.isoformat(),
            "day_of_30": max(1, min(day_x, WAIT_DAYS)),
            "days_left": days_left,
            "done": days_left < 0,                 # ya pasaron los 30 días
            "based_on": "return_receipt" if r.get("return_receipt_date") else "tracking",
        })
    out.sort(key=lambda x: x["due_date"])
    return out

def _reminders_query(operator_id, client_id=None):
    q = sb.table("letter_receipts").select("*").eq("operator_id", operator_id)
    if client_id:
        q = q.eq("client_id", client_id)
    rows = (q.execute().data) or []
    ids = list({r.get("client_id") for r in rows if r.get("client_id")})
    names = {}
    if ids:
        try:
            cr = sb.table("api_clients").select("id, full_name").in_("id", ids).execute()
            names = {c["id"]: c.get("full_name") for c in (cr.data or [])}
        except Exception:
            names = {}
    return _build_reminders(rows, names)

@app.get("/reminders")
async def list_reminders(user=Depends(get_current_user)):
    """Recordatorios automáticos de TODOS los clientes de la agencia."""
    try:
        return _reminders_query(user["id"])
    except Exception:
        return []

@app.get("/clients/{client_id}/reminders")
async def list_client_reminders(client_id: str, user=Depends(get_current_user)):
    """Recordatorios automáticos de un cliente."""
    try:
        return _reminders_query(user["id"], client_id)
    except Exception:
        return []

# ═══════════════════════════════════════════════════════════════
#  SUSCRIPCIONES / COBRO (Stripe, vía httpx — sin dependencias nuevas)
#  Planes:
#    - basic  $50/mes   -> 1 cliente en total, solo Round 1
#    - pro    $150/mes  o  anual (-15%) -> ilimitado, todas las rondas
#  El cobro se ENFORCEA solo cuando BILLING_ENFORCED=1 (así puedes
#  desplegar, configurar Stripe y probar antes de bloquear a nadie).
#  Env vars: STRIPE_SECRET_KEY, STRIPE_WEBHOOK_SECRET, FRONTEND_URL,
#    STRIPE_PRICE_BASIC_MONTHLY, STRIPE_PRICE_PRO_MONTHLY,
#    STRIPE_PRICE_PRO_ANNUAL, BILLING_ADMIN_EMAILS (coma-separadas)
# ═══════════════════════════════════════════════════════════════

STRIPE_API = "https://api.stripe.com/v1"
PLAN_LIMITS = {
    "basic": {"max_clients": 1, "rounds": {"round_1"}},
    "pro":   {"max_clients": None, "rounds": None},   # None = ilimitado
}
PRICE_ENV = {
    ("basic", "monthly"): "STRIPE_PRICE_BASIC_MONTHLY",
    ("pro",   "monthly"): "STRIPE_PRICE_PRO_MONTHLY",
    ("pro",   "annual"):  "STRIPE_PRICE_PRO_ANNUAL",
}

def _billing_enforced() -> bool:
    return os.environ.get("BILLING_ENFORCED", "").lower() in ("1", "true", "yes", "on")

def _is_billing_admin(user) -> bool:
    emails = [e.strip().lower() for e in
              os.environ.get("BILLING_ADMIN_EMAILS", "").split(",") if e.strip()]
    return (user.get("role") == "admin") or (str(user.get("email", "")).lower() in emails)

def _user_plan(user_id):
    """Fila de suscripción si está activa, si no None."""
    try:
        r = sb.table("api_subscriptions").select("*").eq("user_id", user_id).execute()
        if not r.data:
            return None
        row = r.data[0]
        if row.get("status") not in ("active", "trialing"):
            return None
        return row
    except Exception:
        return None

def _active_plan_or_402(user) -> str:
    """Devuelve 'basic'|'pro'. Si el cobro no está activado o es admin, todo = 'pro'.
    Si el cobro está activo y no hay plan, corta con 402."""
    if not _billing_enforced() or _is_billing_admin(user):
        return "pro"
    row = _user_plan(user["id"])
    if not row:
        raise HTTPException(402, "Elige un plan para continuar (Suscripción).")
    return (row.get("plan") or "basic").lower()

def _enforce_can_add_client(user):
    plan = _active_plan_or_402(user)
    lim = PLAN_LIMITS.get(plan, PLAN_LIMITS["basic"])["max_clients"]
    if lim is None:
        return
    try:
        cnt = (sb.table("api_clients").select("id", count="exact")
               .eq("operator_id", user["id"]).execute())
        n = cnt.count if getattr(cnt, "count", None) is not None else len(cnt.data or [])
    except Exception:
        n = 0
    if n >= lim:
        raise HTTPException(402, f"El plan Básico permite {lim} cliente. "
                                 "Sube a Pro para agregar más.")

def _enforce_round(user, rnd):
    plan = _active_plan_or_402(user)
    allowed = PLAN_LIMITS.get(plan, PLAN_LIMITS["basic"])["rounds"]
    if allowed is not None and (rnd or "round_1") not in allowed:
        raise HTTPException(402, "El plan Básico solo permite Round 1. "
                                 "Sube a Pro para Round 2 y 3.")

# ── Stripe REST (httpx, form-encoded) ──────────────────────────
def _stripe_key():
    k = os.environ.get("STRIPE_SECRET_KEY", "")
    if not k:
        raise HTTPException(500, "STRIPE_SECRET_KEY no está configurada en el servidor.")
    return k

def _stripe_post(path, data):
    import httpx
    r = httpx.post(f"{STRIPE_API}{path}", data=data, auth=(_stripe_key(), ""), timeout=30)
    if not r.is_success:
        raise HTTPException(502, f"Stripe error {r.status_code}: {r.text[:300]}")
    return r.json()

def _stripe_get(path):
    import httpx
    r = httpx.get(f"{STRIPE_API}{path}", auth=(_stripe_key(), ""), timeout=30)
    if not r.is_success:
        raise RuntimeError(f"Stripe GET {path}: {r.status_code} {r.text[:200]}")
    return r.json()

def _price_id(plan, cycle):
    env = PRICE_ENV.get((plan, cycle))
    pid = os.environ.get(env, "") if env else ""
    if not pid:
        raise HTTPException(400, f"Plan/ciclo no disponible: {plan}/{cycle}")
    return pid

def _plan_from_price(price_id):
    for (plan, cycle), env in PRICE_ENV.items():
        if price_id and os.environ.get(env, "") == price_id:
            return plan, cycle
    return None, None

def _sub_fields(s: dict) -> dict:
    out = {"status": s.get("status")}
    out["cancel_at_period_end"] = bool(s.get("cancel_at_period_end"))
    md = s.get("metadata") or {}
    if md.get("plan"):
        out["plan"] = md["plan"]
    if md.get("cycle"):
        out["billing_cycle"] = md["cycle"]
    # current_period_end: en API viejas está en el objeto; en las nuevas está en
    # el ítem de la suscripción (items.data[0].current_period_end). Probamos ambos.
    cpe = s.get("current_period_end")
    try:
        items = (s.get("items") or {}).get("data") or []
        first = items[0] if items else {}
        if not cpe:
            cpe = first.get("current_period_end")
        pid = (first.get("price") or {}).get("id")
        p, c = _plan_from_price(pid)
        if p:
            out.setdefault("plan", p)
            out.setdefault("billing_cycle", c)
    except Exception:
        pass
    if cpe:
        out["current_period_end"] = datetime.fromtimestamp(cpe, timezone.utc).isoformat()
    return out

def _sub_upsert(user_id, base: dict):
    base["user_id"] = user_id
    base["updated_at"] = datetime.now(timezone.utc).isoformat()
    base = {k: v for k, v in base.items() if v is not None}
    sb.table("api_subscriptions").upsert(base, on_conflict="user_id").execute()

def _sub_from_checkout(session: dict):
    md = session.get("metadata") or {}
    user_id = session.get("client_reference_id") or md.get("user_id")
    if not user_id:
        return
    base = {"stripe_customer_id": session.get("customer"),
            "stripe_subscription_id": session.get("subscription"),
            "status": "active", "plan": md.get("plan"),
            "billing_cycle": md.get("cycle")}
    if session.get("subscription"):
        try:
            base.update(_sub_fields(_stripe_get(f"/subscriptions/{session['subscription']}")))
        except Exception:
            pass
    _sub_upsert(user_id, base)

def _sub_from_subscription(s: dict):
    md = s.get("metadata") or {}
    user_id = md.get("user_id")
    if not user_id:
        try:
            r = (sb.table("api_subscriptions").select("user_id")
                 .eq("stripe_customer_id", s.get("customer")).execute())
            user_id = r.data[0]["user_id"] if r.data else None
        except Exception:
            user_id = None
    if not user_id:
        return
    base = {"stripe_customer_id": s.get("customer"),
            "stripe_subscription_id": s.get("id")}
    base.update(_sub_fields(s))
    _sub_upsert(user_id, base)

def _sub_mark_canceled(s: dict):
    try:
        (sb.table("api_subscriptions")
         .update({"status": "canceled",
                  "updated_at": datetime.now(timezone.utc).isoformat()})
         .eq("stripe_subscription_id", s.get("id")).execute())
    except Exception:
        pass

# Tolerancia de antigüedad de la firma. Stripe recomienda 300s. Configurable
# por si el reloj del contenedor queda desfasado respecto de Stripe.
STRIPE_SIG_TOLERANCE_SECONDS = int(os.environ.get("STRIPE_SIG_TOLERANCE_SECONDS", "300"))

def _verify_stripe_sig(raw: bytes, sig_header: str, secret: str) -> tuple:
    """Verifica la firma del webhook de Stripe.

    Devuelve (ok, motivo). El motivo va SOLO al log: al cliente se le
    responde siempre lo mismo, para no darle un oráculo que le diga en qué
    se equivocó.

    Valida además la antigüedad del timestamp (anti-replay) y acepta varias
    firmas v1 en el mismo header, que es lo que Stripe manda mientras se
    rota el secreto.
    """
    if not secret:
        return (False, "no_secret_configured")
    if not sig_header:
        return (False, "missing_signature_header")

    # Header: "t=123,v1=abc,v1=def" — puede traer más de un v1.
    timestamp = ""
    signatures = []
    for part in sig_header.split(","):
        if "=" not in part:
            continue
        k, v = part.split("=", 1)
        k = k.strip()
        if k == "t":
            timestamp = v.strip()
        elif k == "v1":
            signatures.append(v.strip())

    if not timestamp or not signatures:
        return (False, "malformed_signature_header")

    try:
        ts = int(timestamp)
    except ValueError:
        return (False, "non_numeric_timestamp")

    age = abs(time.time() - ts)
    if age > STRIPE_SIG_TOLERANCE_SECONDS:
        return (False, f"timestamp_outside_tolerance({int(age)}s)")

    # HMAC sobre "<t>.<cuerpo crudo>". Tienen que ser los bytes EXACTOS que
    # llegaron: si se re-serializa el JSON, la firma no da.
    signed = timestamp.encode() + b"." + raw
    expected = hmac.new(secret.encode(), signed, hashlib.sha256).hexdigest()

    ok = False
    for sig in signatures:
        if hmac.compare_digest(expected, sig):
            ok = True
    return (ok, "" if ok else "signature_mismatch")

def _subscription_url():
    """URL de la pantalla de suscripción (a donde vuelve Stripe). Configurable con
    APP_SUBSCRIPTION_URL (URL completa); si no, se arma con FRONTEND_URL + /subscription."""
    u = os.environ.get("APP_SUBSCRIPTION_URL", "").strip()
    if u:
        return u.rstrip("/")
    front = os.environ.get("FRONTEND_URL", "").rstrip("/")
    return f"{front}/subscription"

# ── Endpoints de cobro ─────────────────────────────────────────
@app.get("/billing/plans")
async def billing_plans():
    """Catálogo de planes para pintar la pantalla de precios."""
    return {"currency": "usd", "plans": [
        {"id": "basic", "name": "Básica", "monthly": 50, "annual": None,
         "tagline": "Para empezar con un cliente",
         "features": [
             "1 cliente",
             "Disputas Round 1 (burós y acreedores)",
             "Análisis completo del reporte (motor FCRA)",
             "Generación de cartas de disputa",
             "Correo certificado USPS con tu cuenta Postalocity",
             "Recordatorios automáticos de 30 días",
         ]},
        {"id": "pro", "name": "Pro", "monthly": 150, "annual": 1530,
         "annual_discount_pct": 15,
         "tagline": "Sin límites, para tu agencia",
         "features": [
             "Clientes ilimitados",
             "Las 3 rondas de disputa (1, 2 y 3)",
             "Cartas a burós y acreedores",
             "Seguimiento multi-ronda + recordatorios de 30 días",
             "Correo certificado con tracking automático",
             "Reportes de progreso (CIR) y panel de cumplimiento",
             "Soporte prioritario",
         ]},
    ]}

@app.get("/billing/status")
async def billing_status(user=Depends(get_current_user)):
    row = _user_plan(user["id"])
    base = {"enforced": _billing_enforced(), "is_admin": _is_billing_admin(user)}
    if not row:
        return {"plan": None, "status": "none", **base}
    return {"plan": row.get("plan"), "cycle": row.get("billing_cycle"),
            "status": row.get("status"),
            "current_period_end": row.get("current_period_end"),
            "cancel_at_period_end": row.get("cancel_at_period_end"), **base}

class CheckoutBody(BaseModel):
    plan: str                              # 'basic' | 'pro'
    cycle: Optional[str] = "monthly"       # 'monthly' | 'annual'

@app.post("/billing/checkout")
async def billing_checkout(body: CheckoutBody, user=Depends(get_current_user)):
    plan = (body.plan or "").lower()
    cycle = (body.cycle or "monthly").lower()
    if plan not in ("basic", "pro"):
        raise HTTPException(400, "Plan inválido.")
    if plan == "basic":
        cycle = "monthly"                  # Básica solo mensual
    price = _price_id(plan, cycle)
    base = _subscription_url()
    try:
        r = sb.table("api_subscriptions").select("*").eq("user_id", user["id"]).execute()
        existing = r.data[0] if r.data else None
    except Exception:
        existing = None
    sep = "&" if "?" in base else "?"
    data = {
        "mode": "subscription",
        "line_items[0][price]": price,
        "line_items[0][quantity]": "1",
        "success_url": f"{base}{sep}status=success",
        "cancel_url": f"{base}{sep}status=cancel",
        "client_reference_id": user["id"],
        "metadata[user_id]": user["id"],
        "metadata[plan]": plan,
        "metadata[cycle]": cycle,
        "subscription_data[metadata][user_id]": user["id"],
        "subscription_data[metadata][plan]": plan,
        "subscription_data[metadata][cycle]": cycle,
    }
    if existing and existing.get("stripe_customer_id"):
        data["customer"] = existing["stripe_customer_id"]
    elif user.get("email"):
        data["customer_email"] = user["email"]
    session = _stripe_post("/checkout/sessions", data)
    return {"url": session.get("url"), "id": session.get("id")}

@app.post("/billing/portal")
async def billing_portal(user=Depends(get_current_user)):
    """Portal de Stripe para gestionar/cancelar la suscripción."""
    r = sb.table("api_subscriptions").select("*").eq("user_id", user["id"]).execute()
    sub = r.data[0] if r.data else None
    if not sub or not sub.get("stripe_customer_id"):
        raise HTTPException(400, "No hay una suscripción para gestionar.")
    session = _stripe_post("/billing_portal/sessions",
                           {"customer": sub["stripe_customer_id"],
                            "return_url": _subscription_url()})
    return {"url": session.get("url")}

@app.post("/billing/webhook")
async def billing_webhook(request: Request):
    """Stripe llama aquí al pagar / renovar / cancelar. Actualiza el plan."""
    raw = await request.body()
    secret = os.environ.get("STRIPE_WEBHOOK_SECRET", "")

    # FALLA CERRADO. Antes era `if secret and not _verify_stripe_sig(...)`:
    # si la variable faltaba o quedaba vacía, el chequeo entero se salteaba
    # y el webhook aceptaba cualquier POST sin firma — un
    # checkout.session.completed forjado activaba el plan Pro de cualquier
    # cuenta. Sin secreto no se procesa nada, y el fallo queda en el log.
    if not secret:
        print("[billing webhook] RECHAZADO: STRIPE_WEBHOOK_SECRET no está "
              "configurada. El webhook no procesa nada sin ella.")
        raise HTTPException(503, "Webhook not configured")

    ok, motivo = _verify_stripe_sig(raw, request.headers.get("stripe-signature", ""), secret)
    if not ok:
        print(f"[billing webhook] firma rechazada: {motivo}")
        raise HTTPException(400, "bad signature")

    try:
        event = json.loads(raw.decode("utf-8"))
    except Exception:
        raise HTTPException(400, "bad payload")
    typ = event.get("type", "")
    obj = (event.get("data") or {}).get("object") or {}
    try:
        if typ == "checkout.session.completed":
            _sub_from_checkout(obj)
        elif typ in ("customer.subscription.created", "customer.subscription.updated"):
            _sub_from_subscription(obj)
        elif typ == "customer.subscription.deleted":
            _sub_mark_canceled(obj)
    except Exception as e:
        # 500 para que Stripe reintente. Antes devolvía 200 y el evento se
        # perdía para siempre si el upsert fallaba.
        print(f"[billing webhook] error procesando {typ}: {e}")
        raise HTTPException(500, "processing error")
    return {"received": True}
