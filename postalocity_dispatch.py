"""
postalocity_dispatch.py
Elitte Solutions  ->  Postalocity certified-mail dispatch adapter.

FLUJO REAL (confirmado de apidoc.html). Mandar UNA carta certificada es un
pipeline de varios pasos, no un solo POST:

  1. login                -> JWT (2 min; RegenToken lo refresca)
  2. CreateJob            -> crea un job vacio, devuelve jobId
  3. configurar el perfil -> remitente (return address) + certificado
  4. GetUploadParams      -> params firmados para subir el PDF a S3
  5. subir PDF a S3       -> multipart POST directo a S3
  6. AddSource           -> asocia el archivo subido con la direccion destino (el buro)
  7. SplitSource         -> parte el PDF en paginas para procesar
  8. JobStart (/job/run) -> dispara el procesamiento (NO manda todavia)
  9. GetJob (/job)       -> estado + precio; aqui revisas antes de aprobar

>>> APROBACION / PAGO (lo que gasta balance y manda de verdad) es un paso
    aparte que NO se hace aqui. Este modulo llega hasta GetJob y te devuelve
    el precio para que TU apruebes desde tu cuenta. Ademas por defecto corre
    contra DEV (production:false), donde no se cobra ni se manda.

Confirmado:
  * dev  base: https://dev.postalocity.com   |  prod base: https://prod.postalocity.com
  * POST /user/login            {"userName","password"} -> {"token","type","message"}
  * POST /job/srcuploadparams   {"jobId"} -> {"uploadParams": {...S3...}}
  * POST /job/addsource         {"jobId","uploadUrl","deliveryAddress"}
  * POST /job/splitsource       {"filename","jobId"}
  * GET  /job/run?id=<jobId>    dispara procesamiento
  * GET  /job?id=<jobId>        devuelve el job object (state, progress, precio)
  * mailingClass "FIRST_CLASS" + objeto certifiedMail (receipt/signature) = certificado

# CONFIRMAR (los 3-4 puntos que el dry-run en dev va a resolver):
  * path exacto de CreateJob y UpdateJob (aqui asumo /job/create y /job/update)
  * como se fija el return address + certifiedMail en el perfil (via UpdateJob?)
  * como se forma el uploadUrl que AddSource descarga (objeto S3 vs presigned GET)
  * enum de certifiedMail.signature (NONE / ELECTRONIC / ...)
"""

from __future__ import annotations

import os
import json
import time
import hashlib
from dataclasses import dataclass, field
from typing import Optional, Any

import httpx


def _check_http(r):
    """Como raise_for_status pero incluye el mensaje del servidor (clave para depurar)."""
    if not r.is_success:
        raise RuntimeError(f"HTTP {r.status_code} en {r.url}\n--- respuesta del servidor ---\n{r.text[:1000]}")
    return r


# ----------------------------------------------------------------------------
# Generador de PDF puro (sin reportlab). Multipágina + wrap simple.
# ----------------------------------------------------------------------------

def _pdf_escape(s):
    return (s or "").replace("\\", r"\\").replace("(", r"\(").replace(")", r"\)")

def _wrap_lines(text, wrap=95):
    out = []
    for raw in (text or "").split("\n"):
        if len(raw) <= wrap:
            out.append(raw); continue
        cur = ""
        for w in raw.split(" "):
            if len((cur + " " + w).strip()) <= wrap:
                cur = (cur + " " + w).strip()
            else:
                out.append(cur); cur = w
        out.append(cur)
    return out

def text_to_pdf_bytes(text, size=11, margin=72, leading=15, wrap=95,
                      page_w=612, page_h=792):
    """Renderiza texto a un PDF mailable (Times-Roman) sin dependencias externas."""
    lines = _wrap_lines(text, wrap)
    per_page = max(1, int((page_h - 2 * margin) // leading))
    pages = [lines[i:i + per_page] for i in range(0, len(lines), per_page)] or [[""]]

    page_obj_ids, content_obj_ids, content_streams = [], [], []
    next_id = 4
    for pl in pages:
        pid = next_id; cid = next_id + 1; next_id += 2
        page_obj_ids.append(pid); content_obj_ids.append(cid)
        y = page_h - margin
        s = "BT\n/F1 %d Tf\n%d TL\n%d %d Td\n" % (size, leading, margin, y)
        for ln in pl:
            s += "(%s) Tj\nT*\n" % _pdf_escape(ln)
        s += "ET"
        content_streams.append(s)

    kids = " ".join("%d 0 R" % p for p in page_obj_ids)
    parts = []
    parts.append((1, b"<< /Type /Catalog /Pages 2 0 R >>"))
    parts.append((2, ("<< /Type /Pages /Count %d /Kids [%s] >>" % (len(page_obj_ids), kids)).encode()))
    parts.append((3, b"<< /Type /Font /Subtype /Type1 /BaseFont /Times-Roman >>"))
    for idx, pid in enumerate(page_obj_ids):
        cid = content_obj_ids[idx]
        body = ("<< /Type /Page /Parent 2 0 R /MediaBox [0 0 %d %d] "
                "/Resources << /Font << /F1 3 0 R >> >> /Contents %d 0 R >>"
                % (page_w, page_h, cid)).encode()
        parts.append((pid, body))
        cs = content_streams[idx].encode("latin-1", "replace")
        stream = b"<< /Length %d >>\nstream\n" % len(cs) + cs + b"\nendstream"
        parts.append((cid, stream))

    parts.sort(key=lambda x: x[0])
    out = bytearray(b"%PDF-1.4\n")
    offsets = {}
    for num, body in parts:
        offsets[num] = len(out)
        out += ("%d 0 obj\n" % num).encode() + body + b"\nendobj\n"
    xref_pos = len(out)
    n = len(parts) + 1
    out += ("xref\n0 %d\n" % n).encode()
    out += b"0000000000 65535 f \n"
    for num in range(1, n):
        out += ("%010d 00000 n \n" % offsets[num]).encode()
    out += ("trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF" % (n, xref_pos)).encode()
    return bytes(out)

def write_text_pdf(path, text, **kw):
    with open(path, "wb") as f:
        f.write(text_to_pdf_bytes(text, **kw))
    return path



# ----------------------------------------------------------------------------
# Config (DEV por defecto = seguro)
# ----------------------------------------------------------------------------

ENV = os.environ.get("POSTALOCITY_ENV", "dev")
BASE = {"dev": "https://dev.postalocity.com",
        "prod": "https://prod.postalocity.com"}[ENV]

POSTALOCITY_USER = os.environ.get("POSTALOCITY_USER", "")
POSTALOCITY_PASS = os.environ.get("POSTALOCITY_PASS", "")

TOKEN_TTL = 120
REFRESH_BEFORE = 30


# ----------------------------------------------------------------------------
# Direcciones
# ----------------------------------------------------------------------------

@dataclass
class Address:
    name: str
    line1: str
    city: str
    state: str
    zip: str
    line2: str = ""

    def to_postal(self) -> dict:
        # Formato de direccion que espera Postalocity.
        return {
            "name": self.name,
            "company1": "", "company2": "",
            "address1": self.line1, "address2": self.line2,
            "city": self.city, "state": self.state, "zip": self.zip,
            "country": "United States",
        }


# ----------------------------------------------------------------------------
# Token JWT (2 min) con refresh via RegenToken + fallback a login
# ----------------------------------------------------------------------------

class TokenManager:
    def __init__(self, base: str = BASE):
        self.base = base
        self._token: Optional[str] = None
        self._obtained_at = 0.0

    def _hdr(self, token: Optional[str] = None) -> dict:
        h = {"Accept": "application/json", "Content-Type": "application/json"}
        if token:
            h["Authorization"] = f"Bearer {token}"
        return h

    def _store(self, data: dict) -> str:
        if data.get("type") and data["type"] != "SUCCESS":
            raise RuntimeError(f"Auth fallo: {data.get('message') or data}")
        token = data.get("token")
        if not token:
            raise RuntimeError(f"Respuesta sin token: {data}")
        self._token, self._obtained_at = token, time.time()
        return token

    def _login(self) -> str:
        r = httpx.post(f"{self.base}/user/login", headers=self._hdr(),
                          json={"userName": POSTALOCITY_USER,
                                "password": POSTALOCITY_PASS}, timeout=30)
        _check_http(r)
        return self._store(r.json())

    def _regen(self) -> str:
        r = httpx.post(f"{self.base}/user/regenToken",   # CONFIRMAR path
                          headers=self._hdr(self._token), json="", timeout=30)
        _check_http(r)
        return self._store(r.json())

    def get(self) -> str:
        if self._token is None:
            return self._login()
        if (time.time() - self._obtained_at) > (TOKEN_TTL - REFRESH_BEFORE):
            try:
                return self._regen()
            except Exception:
                return self._login()
        return self._token


# ----------------------------------------------------------------------------
# Cliente Postalocity (un metodo por paso del pipeline)
# ----------------------------------------------------------------------------

class PostalocityClient:
    def __init__(self, base: str = BASE):
        self.base = base
        self.tokens = TokenManager(base)

    def _h(self) -> dict:
        return {"Accept": "application/json", "Content-Type": "application/json",
                "Authorization": f"Bearer {self.tokens.get()}"}

    def _check(self, data: dict, ctx: str) -> dict:
        if data.get("type") and data["type"] != "SUCCESS":
            raise RuntimeError(f"{ctx} fallo: {data.get('message') or data}")
        return data

    # 2) CreateJob  (POST /job = UpdateJob; en JAX-RS el verbo distingue: PUT /job = crear)
    CREATE_ATTEMPTS = [("PUT", "/job"), ("PATCH", "/job"),
                       ("PUT", "/jobs"), ("POST", "/job/createjob")]

    def create_job(self, paper="Letter", mailing_type="Letter") -> int:
        body = {"paperSize": paper, "mailingType": mailing_type}
        errors = []
        for method, path in self.CREATE_ATTEMPTS:
            try:
                r = httpx.request(method, f"{self.base}{path}",
                                     headers=self._h(), json=body, timeout=30)
            except Exception as e:
                errors.append(f"{method} {path}: {e}")
                continue
            if r.status_code in (404, 405):
                errors.append(f"{method} {path}: {r.status_code}")
                continue
            if r.is_success:
                print(f"  [ok] ruta de crear encontrada: {method} {path}")
                data = self._check(r.json(), "create_job")
                return data.get("data") or data["job"]["id"]
            txt = r.text[:300]
            errors.append(f"{method} {path}: {r.status_code} -> {txt}")
            if "CreateJobRequest" in txt:
                raise RuntimeError(
                    f"Ruta CORRECTA {method} {path} (llega a CreateJobRequest) pero el "
                    f"cuerpo necesita ajuste:\n{r.text[:800]}")
        raise RuntimeError("Ninguna combinacion de crear respondio bien. Intentos:\n  " +
                           "\n  ".join(errors))

    # 3) UpdateJob (un campo a la vez: field/value)
    def update_job(self, job_id: int, field_name: str, value: Any) -> dict:
        # Confirmado: POST /job = UpdateJob ({jobId, field, value}).
        # El server espera "value" como STRING; si es objeto/lista, lo serializamos.
        if isinstance(value, (dict, list)):
            value = json.dumps(value)
        elif not isinstance(value, str):
            value = str(value)
        r = httpx.post(f"{self.base}/job", headers=self._h(),
                          json={"jobId": job_id, "field": field_name, "value": value},
                          timeout=30)
        _check_http(r)
        return self._check(r.json(), f"update_job[{field_name}]")

    def configure_certified(self, job_id: int, return_addr: Address,
                            receipt=True, signature="NONE") -> None:
        # Confirmado por Postalocity: los campos del perfil llevan el prefijo
        # "jobProfile." y el value va como STRING JSON (update_job lo serializa).
        # El remitente = el cliente (dinamico por job).
        self.update_job(job_id, "jobProfile.envelopeReturnAddress",
                        return_addr.to_postal())
        self.update_job(job_id, "jobProfile.certifiedMail", {
            "receipt": receipt, "restricted": False, "adult": False,
            "signature": signature,
            "returnAddress": return_addr.to_postal(),
        })

    # 4) GetUploadParams
    def get_upload_params(self, job_id: int) -> dict:
        r = httpx.post(f"{self.base}/job/srcuploadparams", headers=self._h(),
                          json={"jobId": job_id}, timeout=30)
        _check_http(r)
        return self._check(r.json(), "get_upload_params")["uploadParams"]

    # 5) subir PDF a S3 (multipart directo a S3 con los params firmados)
    def upload_pdf_to_s3(self, up: dict, pdf_path: str) -> tuple[str, str]:
        fn = os.path.basename(pdf_path)
        key = up["key"].replace("${filename}", fn)
        fields = {
            "key": key,
            "Filename": key,
            "name": key,
            "acl": up["acl"],
            "Content-Type": "application/pdf",   # la policy S3 lo exige (starts-with)
            "success_action_status": str(up["success_action_status"]),
            "policy": up["policy"],
            "x-amz-algorithm": up["x_amz_algorithm"],
            "x-amz-credential": up["x_amz_credential"],
            "x-amz-date": up["x_amz_date"],
            "x-amz-signature": up["x_amz_signature"],
        }
        with open(pdf_path, "rb") as f:
            # El "file" debe ir de ULTIMO en un POST de S3, y con su content-type.
            r = httpx.post(up["url"], data=fields,
                              files={"file": (fn, f, "application/pdf")}, timeout=120)
        _check_http(r)
        object_url = f'{up["url"]}/{key}'   # CONFIRMAR: AddSource puede requerir presigned GET
        return fn, object_url

    # 6) AddSource  (confirmado en apidoc: AddSourceRequest =
    #    { jobId, uploadUrl, uploadedFileName, deliveryAddress }).
    #    deliveryAddress fija el DESTINATARIO explicito: no depende de la
    #    posicion sobre-ventana del PDF ni de un CSV.
    def add_source(self, job_id: int, upload_url: str,
                   delivery: "Address | None" = None) -> dict:
        # Confirmado por el ejemplo oficial: payload = {jobId, uploadUrl, deliveryAddress}.
        # (uploadedFileName existe en el schema pero el ejemplo NO lo envia.)
        # "This will download the provided url and apply the supplied address."
        payload = {"jobId": job_id, "uploadUrl": upload_url}
        if delivery is not None:
            payload["deliveryAddress"] = delivery.to_postal()
        r = httpx.post(f"{self.base}/job/addsource", headers=self._h(),
                          json=payload, timeout=60)
        _check_http(r)
        return self._check(r.json(), "add_source")

    # 7) SplitSource
    def split_source(self, job_id: int, filename: str) -> dict:
        r = httpx.post(f"{self.base}/job/splitsource", headers=self._h(),
                          json={"filename": filename, "jobId": job_id}, timeout=60)
        _check_http(r)
        return self._check(r.json(), "split_source")

    # 8) JobStart (GET /job/run?id=)
    def job_start(self, job_id: int) -> dict:
        r = httpx.get(f"{self.base}/job/run", headers=self._h(),
                         params={"id": job_id}, timeout=60)
        _check_http(r)
        return self._check(r.json(), "job_start")

    # 9) GetJob (GET /job?id=)  -> estado + precio, para tracking
    def get_job(self, job_id: int) -> dict:
        r = httpx.get(f"{self.base}/job", headers=self._h(),
                         params={"id": job_id}, timeout=30)
        _check_http(r)
        resp = self._check(r.json(), "get_job")
        # El objeto real viene en "job"; "data" a veces trae solo el id.
        data = resp.get("job")
        if not isinstance(data, dict):
            data = resp.get("data")
        if not isinstance(data, dict):
            return {"id": data}
        return data


# ----------------------------------------------------------------------------
# Orquestador: manda UNA carta certificada hasta el punto de revision de precio
# (NO aprueba / NO paga / NO manda — eso lo haces tu)
# ----------------------------------------------------------------------------

def send_certified_letter(pdf_path: str, sender: Address, recipient: Address | None = None,
                          receipt: bool = True,
                          poll: bool = True) -> dict:
    c = PostalocityClient()

    job_id = c.create_job()
    print(f"  [ok] job creado: {job_id}")
    try:
        c.configure_certified(job_id, return_addr=sender, receipt=receipt)
        print("  [ok] perfil certificado configurado")
    except Exception as e:
        print(f"  [aviso] no se pudo configurar el certificado aun: {e}")
        print("         (seguimos para ver el resto del flujo)")

    up = c.get_upload_params(job_id)
    print("  [ok] params de subida S3 obtenidos")
    filename, object_url = c.upload_pdf_to_s3(up, pdf_path)
    print(f"  [ok] PDF subido a S3: {filename}")

    # FLUJO DIRECTO documentado: tras subir el PDF, se llama SplitSource.
    # (AddSource NO se usa aqui: es una via ALTERNATIVA que descarga el PDF
    #  desde una URL publica/firmada + deliveryAddress; con la subida directa
    #  al bucket privado de Postalocity, AddSource devuelve 500.)
    # No abortamos si algo falla: seguimos hasta GetJob para leer errorReason.
    try:
        c.split_source(job_id, filename=filename)
        print("  [ok] documento partido en paginas")
    except Exception as e:
        print(f"  [aviso] SplitSource fallo: {e}")
    try:
        c.job_start(job_id)
        print("  [ok] procesamiento disparado")
    except Exception as e:
        print(f"  [aviso] JobStart fallo: {e}")

    job = {}
    if poll:
        # El job procesa unos segundos; consultamos varias veces hasta que
        # aparezca el estado / la direccion detectada del destinatario.
        for intento in range(6):
            time.sleep(4)
            job = c.get_job(job_id)
            if not isinstance(job, dict):
                job = {"id": job}
            detected = job.get("mailTo") or job.get("state")
            if detected:
                break
        # Mostrar que direccion LEYO Postalocity como destinatario
        mail_to = job.get("mailTo")
        print("  --- direccion que Postalocity detecto como DESTINATARIO ---")
        if mail_to:
            print(f"      {mail_to}")
        else:
            print("      (aun no disponible; el job puede seguir procesando)")
    if not isinstance(job, dict):
        job = {"id": job}
    return {
        "job_id": job_id,
        "state": job.get("state"),
        "progress": job.get("progress"),
        "total_price": job.get("totalPrice"),
        "total_postage": job.get("totalPostage"),
        "source_count": job.get("sourceCount"),
        "mail_piece_count": job.get("mailPieceCount"),
        "error_reason": job.get("errorReason"),
        "test": job.get("test"),
        "detected_recipient": job.get("mailTo"),
        "raw": job,
    }


# ----------------------------------------------------------------------------
# Direcciones de los buros
# ----------------------------------------------------------------------------

BUREAU_ADDRESSES = {
    "equifax": Address("Equifax Information Services LLC", "P.O. Box 740256",
                       "Atlanta", "GA", "30374"),
    "experian": Address("Experian", "P.O. Box 4500", "Allen", "TX", "75013"),
    "transunion": Address("TransUnion Consumer Solutions", "P.O. Box 2000",
                          "Chester", "PA", "19016"),
}


# ----------------------------------------------------------------------------
# Puente con Elitte Solutions / Supabase
# ----------------------------------------------------------------------------

def _idempotency_ref(job_id: str, bureau: str, round_no: int) -> str:
    return "rd_" + hashlib.sha1(f"{job_id}:{bureau}:R{round_no}".encode()).hexdigest()[:16]


def dispatch_bundle(supabase, job_id: str, bureau: str, pdf_path: str,
                    round_no: int = 1) -> dict:
    """Cliente=remitente -> carta certificada al buro -> escribe job/estado en api_jobs.
    Llega hasta revision de precio; la aprobacion final la haces tu."""
    row = (supabase.table("api_clients")
           .select("full_name,address_line1,address_line2,city,state,zip")
           .eq("job_id", job_id)                  # CONFIRMAR relacion
           .single().execute().data)
    sender = Address(row["full_name"], row["address_line1"], row["city"],
                     row["state"], row["zip"], row.get("address_line2", "") or "")

    result = send_certified_letter(pdf_path, sender, BUREAU_ADDRESSES[bureau])

    supabase.table("api_jobs").update({
        f"{bureau}_postalocity_job": result["job_id"],   # CONFIRMAR columnas
        f"{bureau}_mail_state": result["state"],
        f"{bureau}_idem_ref": _idempotency_ref(job_id, bureau, round_no),
    }).eq("id", job_id).execute()

    return result


# ----------------------------------------------------------------------------
# Harness de prueba (DEV, hasta revision de precio; sin aprobar/pagar)
# ----------------------------------------------------------------------------

def probe_return_address():
    """Descubre como fijar un remitente dinamico por-job (sin adivinar a ciegas)."""
    c = PostalocityClient()
    job_id = c.create_job()
    print(f"Job de prueba creado: {job_id}\n")

    # PASO 1: mandar un valor INVALIDO -> el server suele listar los validos
    print("== PASO 1: valor invalido a returnAddressSource (buscando el enum) ==")
    r = httpx.post(f"{c.base}/job", headers=c._h(),
                      json={"jobId": job_id, "field": "returnAddressSource",
                            "value": "__PROBE_INVALID__"}, timeout=30)
    print(f"  HTTP {r.status_code}")
    print(f"  {r.text[:900]}\n")

    # PASO 2: probar candidatos; para cada uno, fijar el remitente y ver si pega
    test_addr = {"name": "TEST CLIENT RETURN", "company1": "", "company2": "",
                 "address1": "999 Test Ave", "address2": "",
                 "city": "Testville", "state": "FL", "zip": "33333",
                 "country": "United States"}
    candidates = ["DYNAMIC", "DOCUMENT", "CUSTOM", "PERJOB", "PER_JOB",
                  "JOB", "MANUAL", "OVERRIDE", "MERGE", "CSV", "VARIABLE"]
    print("== PASO 2: probar cada modo + fijar remitente + verificar ==")
    for cand in candidates:
        # set returnAddressSource
        r1 = httpx.post(f"{c.base}/job", headers=c._h(),
                           json={"jobId": job_id, "field": "returnAddressSource",
                                 "value": cand}, timeout=30)
        ok_src = r1.is_success
        # set envelopeReturnAddress (como string JSON)
        r2 = httpx.post(f"{c.base}/job", headers=c._h(),
                           json={"jobId": job_id, "field": "envelopeReturnAddress",
                                 "value": json.dumps(test_addr)}, timeout=30)
        # leer que quedo
        job = c.get_job(job_id)
        jp = job.get("jobProfile", {}) if isinstance(job, dict) else {}
        era = jp.get("envelopeReturnAddress", {})
        src_now = jp.get("returnAddressSource")
        stuck = era.get("name") == "TEST CLIENT RETURN"
        flag = "  <<< PEGO EL REMITENTE!" if stuck else ""
        print(f"  {cand:<10} src_set={str(ok_src):<5} -> returnAddressSource ahora={src_now!s:<10} "
              f"remitente={era.get('name')!s:<20}{flag}")
    print("\nSi alguna linea dice 'PEGO EL REMITENTE', ese es el modo correcto.")


def probe_create_with_return():
    """Ultima prueba empirica: fijar el remitente en el CUERPO del PUT /job (al crear)."""
    c = PostalocityClient()
    test_addr = {"name": "TEST CLIENT RETURN", "company1": "", "company2": "",
                 "address1": "999 Test Ave", "address2": "",
                 "city": "Testville", "state": "FL", "zip": "33333",
                 "country": "United States"}

    bodies = {
        "A: campos planos": {
            "paperSize": "Letter", "mailingType": "Letter",
            "returnAddressSource": "DYNAMIC",
            "envelopeReturnAddress": test_addr,
        },
        "B: dentro de jobProfile": {
            "paperSize": "Letter", "mailingType": "Letter",
            "jobProfile": {
                "returnAddressSource": "DYNAMIC",
                "envelopeReturnAddress": test_addr,
            },
        },
        "C: solo la direccion plana": {
            "paperSize": "Letter", "mailingType": "Letter",
            "envelopeReturnAddress": test_addr,
        },
    }

    for label, body in bodies.items():
        print(f"== {label} ==")
        r = httpx.put(f"{c.base}/job", headers=c._h(), json=body, timeout=30)
        print(f"  PUT /job -> HTTP {r.status_code}")
        if not r.is_success:
            print(f"  {r.text[:400]}\n")   # 400 'Unrecognized field' nos revela el schema
            continue
        data = r.json()
        job_id = data.get("data") or (data.get("job") or {}).get("id")
        job = c.get_job(job_id)
        jp = job.get("jobProfile", {}) if isinstance(job, dict) else {}
        era = jp.get("envelopeReturnAddress", {})
        stuck = era.get("name") == "TEST CLIENT RETURN"
        print(f"  job={job_id}  remitente quedo='{era.get('name')}'  "
              f"returnAddressSource={jp.get('returnAddressSource')}"
              f"{'   <<< PEGO!' if stuck else ''}\n")
    print("Si alguna dice 'PEGO', el remitente se fija al crear el job.")


def inspect(job_id: int):
    """Consulta un job existente: muestra estado, precio, remitente y destinatario."""
    c = PostalocityClient()
    r = httpx.get(f"{c.base}/job", headers=c._h(),
                     params={"id": job_id}, timeout=30)
    print(f"== GET /job?id={job_id}  ->  HTTP {r.status_code} ==")
    try:
        resp = r.json()
    except Exception:
        print(r.text[:2000]); return
    job = resp.get("job") or resp.get("data") or {}
    if not isinstance(job, dict):
        print(f"respuesta = {job}"); return

    jp = job.get("jobProfile", {})
    era = jp.get("envelopeReturnAddress", {})

    print(f"  state          : {job.get('state')}")
    print(f"  mailPieceCount : {job.get('mailPieceCount')}")
    print(f"  totalPrice     : {job.get('totalPrice')}")
    print(f"  errorReason    : {job.get('errorReason')}")
    print(f"  REMITENTE (envelopeReturnAddress):")
    print(f"      {era.get('name')} | {era.get('address1')} | "
          f"{era.get('city')}, {era.get('state')} {era.get('zip')}")

    # Buscar la direccion de DESTINO en cualquier parte del objeto
    print(f"  DESTINATARIO / direcciones detectadas en el job:")
    found = False
    def scan(obj, path=""):
        global_found = False
        if isinstance(obj, dict):
            if ("address1" in obj or "city" in obj) and obj is not era:
                nm, a1 = obj.get("name"), obj.get("address1")
                ci, st, zp = obj.get("city"), obj.get("state"), obj.get("zip")
                if a1 or ci:
                    print(f"      [{path}] {nm} | {a1} | {ci}, {st} {zp}")
                    global_found = True
            for k, v in obj.items():
                if scan(v, f"{path}.{k}" if path else k):
                    global_found = True
        elif isinstance(obj, list):
            for i, v in enumerate(obj):
                if scan(v, f"{path}[{i}]"):
                    global_found = True
        return global_found
    if not scan(job):
        print("      (ninguna direccion de destino encontrada en el objeto)")

    print(f"\n== respuesta cruda (4000 chars) ==")
    print(r.text[:4000])


if __name__ == "__main__":
    import sys
    if len(sys.argv) >= 3 and sys.argv[1] == "inspect":
        inspect(int(sys.argv[2]))
        sys.exit(0)
    if len(sys.argv) >= 2 and sys.argv[1] == "probe":
        probe_return_address()
        sys.exit(0)
    if len(sys.argv) >= 2 and sys.argv[1] == "probecreate":
        probe_create_with_return()
        sys.exit(0)

    print(f"Ambiente: {ENV}  ({BASE})")
    c = PostalocityClient()
    print("login/JWT OK:", bool(c.tokens.get()))

    # Genera un PDF de prueba si no existe (para que el dry-run corra sin fricción).
    test_pdf = "ejemplo_carta_parte2.pdf"
    if not os.path.exists(test_pdf):
        write_text_pdf(test_pdf,
            "Cliente Prueba\n123 Main St\nOrlando, FL 32801\n\n"
            "August 11, 2026\n\n"
            "Equifax Information Services LLC\nP.O. Box 740256\nAtlanta, GA 30374\n\n"
            "To whom it may concern:\n\n"
            "This is a TEST letter used only to validate the mailing pipeline.\n"
            "No action is requested. This job will not be approved or mailed.")
        print(f"[ok] PDF de prueba generado (sin reportlab): {test_pdf}")

    print(f"\nCorriendo pipeline completo en {ENV} (sin aprobar/pagar)...")
    res = send_certified_letter(
        pdf_path=test_pdf,
        sender=Address("Cliente Prueba", "123 Main St", "Orlando", "FL", "32801"),
        recipient=BUREAU_ADDRESSES["equifax"],
    )
    print("jobId:", res["job_id"], "| state:", res["state"],
          "| precio:", res["total_price"], "| test:", res["test"])
