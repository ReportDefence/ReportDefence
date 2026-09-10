"""
Epic-Pro Report adapter — MyFreeScore "epic-pro-report" tri-bureau format (ES/EN)
=================================================================================

MyFreeScore ofrece un SEGUNDO formato de reporte ("epic-pro-report"): tri-buró
(Equifax | Experian | TransUnion, cabecera EQ/EX/TU) con secciones tipo
"Cuentas cerradas negativas" / "Negative Closed Accounts" y cada cuenta como una
tabla etiqueta+valor por buró. Las etiquetas pueden venir en ESPAÑOL o en INGLÉS
(a veces mezcladas en el mismo reporte).

Este adaptador extrae ese formato y lo convierte a la MISMA estructura canónica
(inventory_by_bureau) que consume el motor de original_parser, para que el
downstream (detectores, DOFD, cartas, e-OSCAR) corra sin cambios.

Entrypoint: build_report_epicpro(pdf_path) -> dict (compatible build_report).

Diseño espejo de threebureau_adapter.py:
 - anclaje por coordenadas X de las columnas EQ/EX/TU
 - cada cuenta se delimita por "Reporter Name:" / "Nombre del reportero:"
 - normaliza términos ES->EN que el motor entiende (RECOPILACIÓN->collection,
   GANANCIAS Y PÉRDIDAS->charge off, etc.)
"""
from __future__ import annotations
import re
from collections import defaultdict

BUR = ("equifax", "experian", "transunion")
COLKEY = {"EQ": "equifax", "EX": "experian", "TU": "transunion"}


# ----------------------------- carga base -----------------------------
def _load(path):
    import pdfplumber
    pdf = pdfplumber.open(path)
    out = []
    for pg in pdf.pages:
        out.append({
            "text": pg.extract_text() or "",
            "words": pg.extract_words(use_text_flow=False, keep_blank_chars=False),
        })
    return out


def _rows(words, ytol=3):
    d = defaultdict(list)
    for w in words:
        d[round(w["top"] / ytol)].append(w)
    return [sorted(d[k], key=lambda w: w["x0"]) for k in sorted(d)]


def _header_anchors(row):
    """Si la fila es la cabecera EQ/EX/TU, devuelve sus anclas X; si no, None.
    Las columnas cambian de posición entre bloques, así que se recalculan cada
    vez que aparece una cabecera."""
    txts = [w["text"] for w in row]
    if "EQ" in txts and "EX" in txts and "TU" in txts:
        a = {}
        for w in row:
            if w["text"] in ("EQ", "EX", "TU") and w["text"] not in a:
                a[w["text"]] = w["x0"]
        if len(a) == 3:
            return {"equifax": a["EQ"], "experian": a["EX"], "transunion": a["TU"]}
    return None


def _find_anchors(pages):
    """Primera cabecera EQ/EX/TU del reporte (semilla por defecto)."""
    for p in pages:
        for row in _rows(p["words"]):
            a = _header_anchors(row)
            if a:
                return a
    return None


def _assign(row, anchors):
    """Separa una fila en (etiqueta, {eq,ex,tu}) por posición X."""
    eq, ex, tu = anchors["equifax"], anchors["experian"], anchors["transunion"]
    lbl_bound = eq - 14
    b1 = (eq + ex) / 2
    b2 = (ex + tu) / 2
    label, cols = [], {b: [] for b in BUR}
    for w in sorted(row, key=lambda w: w["x0"]):
        x = w["x0"]
        if x < lbl_bound:
            label.append(w["text"])
        elif x < b1:
            cols["equifax"].append(w["text"])
        elif x < b2:
            cols["experian"].append(w["text"])
        else:
            cols["transunion"].append(w["text"])
    return " ".join(label).strip(), {b: " ".join(cols[b]).strip() for b in BUR}


# --------------------- mapa de etiquetas (ES + EN) ---------------------
# label(lowercased) -> campo canónico
_LABELS = {
    # número de cuenta
    "consumer account number": "account_number",
    "número de cuenta del consumidor": "account_number",
    "numero de cuenta del consumidor": "account_number",
    # saldos / montos
    "account current balance": "balance",
    "saldo actual de la cuenta": "balance",
    "account credit limit": "credit_limit",
    "límite de crédito de la cuenta": "credit_limit",
    "limite de credito de la cuenta": "credit_limit",
    "original loan amount / high credit": "high_credit",
    "monto original del préstamo / alto historial crediticio": "high_credit",
    "monto original del prestamo / alto historial crediticio": "high_credit",
    "amount past due": "past_due",
    "monto vencido": "past_due",
    "scheduled monthly payment amount": "monthly_payment",
    "importe del pago mensual programado": "monthly_payment",
    "charge off amount": "charge_off_amount",
    "monto de la cancelación": "charge_off_amount",
    "monto de la cancelacion": "charge_off_amount",
    # estado / clasificación
    "account condition": "condition",
    "condición de la cuenta": "condition",
    "condicion de la cuenta": "condition",
    "account status code": "status_code",
    "código de estado de la cuenta": "status_code",
    "codigo de estado de la cuenta": "status_code",
    "account rating": "rating",
    "calificación de la cuenta": "rating",
    "calificacion de la cuenta": "rating",
    "payment status": "payment_status",
    "estado del pago": "payment_status",
    "compliance condition code": "compliance",
    "código de condición de cumplimiento": "compliance",
    "codigo de condicion de cumplimiento": "compliance",
    "ecoa code": "ecoa",
    "código ecoa": "ecoa",
    "codigo ecoa": "ecoa",
    # tipos
    "portfolio type code": "portfolio_type",
    "código de tipo de cartera": "portfolio_type",
    "codigo de tipo de cartera": "portfolio_type",
    "account type code": "account_type",
    "código de tipo de cuenta": "account_type",
    "codigo de tipo de cuenta": "account_type",
    "creditor type": "creditor_type",
    "tipo de acreedor": "creditor_type",
    "account ownership type": "ownership",
    "tipo de titularidad de la cuenta": "ownership",
    # fechas
    "account date opened": "date_opened",
    "fecha de apertura de la cuenta": "date_opened",
    "account date closed": "date_closed",
    "fecha de cierre de la cuenta": "date_closed",
    "date of account information": "date_info",
    "fecha de la información de la cuenta": "date_info",
    "fecha de la informacion de la cuenta": "date_info",
    "date of last update/activity": "date_last_active",
    "fecha de la última actualización/actividad": "date_last_active",
    "fecha de la ultima actualizacion/actividad": "date_last_active",
    "date of last payment": "date_last_payment",
    "fecha del último pago": "date_last_payment",
    "fecha del ultimo pago": "date_last_payment",
    # otros
    "months reviewed": "months_reviewed",
    "meses revisados": "months_reviewed",
    "remarks": "remarks",
    "observaciones": "remarks",
}

# tabla de morosidades por cuenta (Days Late 7 Year)
_LATE_LABELS = {
    "30 days late payment:": "d30", "pago con 30 días de retraso:": "d30",
    "pago con 30 dias de retraso:": "d30",
    "60 days late payment:": "d60", "pago con 60 días de retraso:": "d60",
    "pago con 60 dias de retraso:": "d60",
    "90 days late payment:": "d90", "pago con 90 días de retraso:": "d90",
    "pago con 90 dias de retraso:": "d90",
}

# secciones (para polaridad + idioma). El motor igual re-clasifica por campos.
_SEC_EN = {
    "negative closed accounts": "negative", "negative open accounts": "negative",
    "negative mixed accounts": "negative", "positive closed accounts": "positive",
    "positive open accounts": "positive", "positive mixed accounts": "positive",
}
_SEC_ES = {
    "cuentas cerradas negativas": "negative", "cuentas abiertas negativas": "negative",
    "cuentas negativas mixtas": "negative", "cuentas cerradas positivas": "positive",
    "cuentas abiertas positivas": "positive", "cuentas positivas mixtas": "positive",
}

_STATUS_WORDS = ("closed", "open", "mixed", "cerrado", "cerrada", "abierto",
                 "abierta", "mixto", "mixta")
_CATEGORY_MARKERS = ("other / unknown", "auto / vehicle", "otras / cuentas",
                     "cuentas relacionadas", "accounts", "vehículos", "vehiculos",
                     "desconocidas")


_PAGE_JUNK = ("get your credit", "obtén tu puntaje", "obten tu puntaje",
              "myfreescorenow", "http")


def _is_page_junk(line: str) -> bool:
    low = (line or "").lower()
    if any(k in low for k in _PAGE_JUNK):
        return True
    if re.search(r"\d{1,2}:\d{2}", low):   # timestamp del encabezado
        return True
    return False


def _reporter_creditor(header_line: str, reporter_val: str) -> str:
    rv = (reporter_val or "").strip()
    if rv and rv != "-":
        return rv
    if _is_page_junk(header_line):
        return ""   # la línea previa era encabezado/pie de página, no un header real
    # parsear del header: quitar status inicial + categoría final
    h = (header_line or "").strip()
    low = h.lower()
    for sw in _STATUS_WORDS:
        if low.startswith(sw + " "):
            h = h[len(sw):].strip()
            low = h.lower()
            break
    # cortar en el primer marcador de categoría
    cut = len(h)
    for m in _CATEGORY_MARKERS:
        i = low.find(m)
        if i != -1:
            cut = min(cut, i)
    # también cortar en "OTHER"/"OTRAS"/"AUTO" mayúsculas sueltas al final
    name = h[:cut].strip(" -/·")
    return name or rv


# --------------------- normalización de negatividad ---------------------
def _match_label(ll: str):
    """Mapea una etiqueta a su campo canónico. Tolera etiquetas CORTADAS por
    salto de línea (ej. 'número de cuenta del' -> 'número de cuenta del
    consumidor') mediante coincidencia por prefijo, solo si es inequívoca."""
    if not ll:
        return None
    if ll in _LABELS:
        return _LABELS[ll]
    hits = set()
    for lab, key in _LABELS.items():
        if len(ll) >= 8 and (lab.startswith(ll) or ll.startswith(lab)):
            hits.add(key)
    return next(iter(hits)) if len(hits) == 1 else None


def _low(*vals):
    return " ".join(v for v in vals if v).lower()


def _is_collection(blob: str) -> bool:
    return any(k in blob for k in (
        "collection", "recopilación", "recopilacion", "en cobro", "collectionattorney"))


def _is_charge_off(blob: str) -> bool:
    return any(k in blob for k in (
        "charge off", "charge-off", "chargeoff", "charged off",
        "profit and loss", "cancelación de deudas", "cancelacion de deudas",
        "ganancias y pérdidas", "ganancias y perdidas", "pérdida", "perdida"))


def _is_repo(blob: str) -> bool:
    return any(k in blob for k in (
        "repossess", "repossession", "recuperación", "recuperacion",
        "voluntary surrender", "surrender"))


def _is_bankruptcy(blob: str) -> bool:
    return any(k in blob for k in (
        "bankruptcy", "bancarrota", "quiebra", "included in bankruptcy"))


def _is_paid(blob: str) -> bool:
    return any(k in blob for k in ("paid", "pagad", "settled", "liquidad"))


def _auto_type(blob: str) -> bool:
    return any(k in blob for k in ("automobile", "automóvil", "automovil", "auto",
                                   "vehicle", "vehículo", "vehiculo"))


def _val(fields, key, b):
    v = fields.get(key, {})
    return (v.get(b, "") if isinstance(v, dict) else "") or ""


def _build_inventory(accounts):
    inv = {b: [] for b in BUR}
    for idx, acc in enumerate(accounts):
        fields = acc["fields"]
        name = acc["creditor"] or "UNKNOWN"
        remarks_all = " ".join(
            _val(fields, "remarks", b) for b in BUR).strip()
        for b in BUR:
            acct_no = _val(fields, "account_number", b)
            status_code = _val(fields, "status_code", b)
            rating = _val(fields, "rating", b)
            pay_raw = _val(fields, "payment_status", b)
            remarks = _val(fields, "remarks", b) or remarks_all
            portfolio = _val(fields, "portfolio_type", b) + " " + _val(fields, "account_type", b)
            condition = _val(fields, "condition", b)
            compliance = _val(fields, "compliance", b)
            late = acc.get("late", {}).get(b, {})

            blob = _low(status_code, rating, pay_raw, remarks, portfolio, acc.get("category", ""))
            is_coll = _is_collection(blob)
            is_co = _is_charge_off(blob)
            is_repo = _is_repo(blob)
            is_bk = _is_bankruptcy(blob)
            is_paid = _is_paid(_low(status_code, rating, remarks))
            # morosidad: por la tabla de conteo O por el código de estado
            # (LATE_30/60/90/120_DAYS, "moroso", "X días de retraso")
            status_late = any(k in _low(status_code, rating, pay_raw) for k in (
                "late_", "late ", "moroso", "de retraso", "días de mora", "dias de mora",
                "past due", "pasado", "vencid"))
            has_late = status_late or any(_int(late.get(k)) for k in ("d30", "d60", "d90"))

            # ¿este buró reporta esta cuenta? (tiene número, o señal negativa, o saldo)
            has_data = bool(acct_no) or is_coll or is_co or is_repo or is_bk or has_late \
                or bool(_val(fields, "balance", b)) or bool(status_code) or bool(rating)
            if not has_data:
                continue

            # payment_status normalizado (inglés que el motor entiende)
            pay = []
            if is_coll:
                pay.append("Collection")
            if is_co:
                pay.append("Charge Off")
            if has_late and not (is_coll or is_co):
                pay.append("Late")
            if not pay:
                pay.append(_map_current(pay_raw) or _map_current(status_code) or "")
            payment_status = " ".join(p for p in pay if p).strip()

            # status (rating): derogatory / paid / closed
            status = ""
            if "derog" in _low(rating):
                status = "derogatory"
            elif is_paid and (is_coll or is_co):
                status = "paid"
            elif "closed" in _low(condition, rating) or "cerrad" in _low(condition, rating):
                status = "closed"

            # raw_lines con keywords en inglés para is_negative/normalize
            raw = [name]
            if is_coll:
                raw.append("collection account")
            if is_co:
                raw.append("charged off")
                if "profit" in blob or "pérdida" in blob or "perdida" in blob or "ganancias" in blob:
                    raw.append("profit and loss")
            if is_repo:
                raw.append("repossession")
            if is_bk:
                raw.append("included in bankruptcy")
            if remarks:
                raw.append(remarks)

            atype_detail = ""
            if _auto_type(portfolio) or _auto_type(acc.get("category", "")):
                atype_detail = "auto loan"
            elif "collectionattorney" in _low(portfolio):
                atype_detail = "collection"

            inv[b].append({
                "name": name,
                "bureau": b,
                "account_number": acct_no,
                "status": status,
                "payment_status": payment_status,
                "balance": _val(fields, "balance", b) or "$0.00",
                "past_due": _val(fields, "past_due", b) or "$0.00",
                "credit_limit": _val(fields, "credit_limit", b) or "$0.00",
                "high_credit": _val(fields, "high_credit", b) or "$0.00",
                "monthly_payment": _val(fields, "monthly_payment", b) or "$0.00",
                "comments": remarks,
                "account_type": "",
                "account_type_detail": atype_detail,
                "date_opened": _val(fields, "date_opened", b),
                "date_last_active": _val(fields, "date_last_active", b) or _val(fields, "date_info", b),
                "date_of_last_payment": _val(fields, "date_last_payment", b),
                "last_reported": _val(fields, "date_info", b) or _val(fields, "date_closed", b),
                "no_of_months": _val(fields, "months_reviewed", b),
                "raw_lines": raw,
                "has_30_in_history": bool(_int(late.get("d30"))),
                "has_60_in_history": bool(_int(late.get("d60"))),
                "has_90_in_history": bool(_int(late.get("d90"))),
                "block_id": f"epic_{idx}",
            })
    return inv


def _int(v):
    try:
        return int(re.sub(r"\D", "", v or "0") or 0)
    except Exception:
        return 0


def _map_current(v):
    low = (v or "").lower()
    if "current" in low or "actual" in low or "al día" in low or "al dia" in low:
        return "Current"
    return ""


# --------------------------- parseo principal ---------------------------
def _last4_set(a):
    """Conjunto de últimos-4 dígitos del número de cuenta en todos los burós.
    Sirve para deduplicar la misma cuenta aunque el nombre venga traducido
    (ES vs EN) — comparten los mismos últimos 4 dígitos."""
    fv = a["fields"].get("account_number", {})
    s = set()
    if isinstance(fv, dict):
        for b in BUR:
            m = re.findall(r"(\d{4})", fv.get(b, "") or "")
            if m:
                s.add(m[-1])
    return frozenset(s)


def _content_sig(a):
    """Firma de contenido independiente del idioma (montos, que no se traducen):
    límite + monto de cancelación + saldo por buró. Sirve para deduplicar la misma
    cuenta cuando el nombre viene traducido y el número está enmascarado/partido."""
    parts = []
    for k in ("high_credit", "charge_off_amount", "balance", "credit_limit"):
        fv = a["fields"].get(k, {})
        vals = tuple(sorted(re.sub(r"[^\d.]", "", fv.get(b, "") or "") for b in BUR))
        parts.append((k, vals))
    return tuple(parts)


def _dedup_accounts(accounts):
    """El reporte con TODAS las opciones trae cada cuenta 2 veces (sección ES y
    sección EN, con el nombre traducido). Se deduplica por últimos-4 dígitos del
    número, o por firma de contenido (montos), quedándose con la más completa."""
    best = {}
    order = []
    for a in accounts:
        l4 = _last4_set(a)
        if l4:
            key = "n:" + ",".join(sorted(l4))
        else:
            sig = _content_sig(a)
            # solo usar la firma si tiene algún monto real (evita fusionar vacíos)
            has_amt = any(any(x and x != "0.00" and x != "0" for x in vals) for _, vals in sig)
            key = ("s:" + repr(sig)) if has_amt else ("c:" + a["creditor"].upper().strip() or id(a))
        sc = len(a["fields"]) + len(a.get("late", {}))
        if key not in best:
            best[key] = (sc, a)
            order.append(key)
        elif sc > best[key][0]:
            best[key] = (sc, a)
    return [best[k][1] for k in order]


def _parse_accounts(pages, init_anchors):
    accounts = []
    cur = None
    polarity = None
    lang = None          # idioma de la sección actual ('es' | 'en')
    prev_text = ""       # línea anterior (posible header de cuenta)
    anchors = dict(init_anchors)

    def flush():
        nonlocal cur
        if cur and (cur["fields"] or cur.get("late")):
            accounts.append(cur)
        cur = None

    for p in pages:
        for row in _rows(p["words"]):
            ha = _header_anchors(row)
            if ha:                       # cabecera EQ/EX/TU -> recalibrar columnas
                anchors = ha
                continue
            label, vals = _assign(row, anchors)
            joined = " ".join(w["text"] for w in sorted(row, key=lambda w: w["x0"])).strip()
            low_join = joined.lower()

            # sección (polaridad + idioma)
            for s, pol in _SEC_EN.items():
                if s in low_join:
                    polarity = pol
                    lang = "en"
            for s, pol in _SEC_ES.items():
                if s in low_join:
                    polarity = pol
                    lang = "es"

            # inicio de cuenta
            if low_join.startswith("reporter name:") or low_join.startswith("nombre del reportero:"):
                flush()
                reporter_val = joined.split(":", 1)[1].strip() if ":" in joined else ""
                creditor = _reporter_creditor(prev_text, reporter_val)
                category = ""
                lp = prev_text.lower()
                for m in _CATEGORY_MARKERS:
                    if m in lp:
                        category = prev_text[lp.find(m):].strip()
                        break
                cur = {"creditor": creditor, "polarity": polarity, "lang": lang,
                       "category": category, "fields": {}, "late": {}}
                prev_text = joined
                continue

            # campos tri-buró dentro de una cuenta
            ll = label.lower().strip().rstrip(":")
            if cur is not None:
                key = _match_label(ll)
                if key and key not in cur["fields"]:
                    # solo guardar si hay algún valor
                    if any(vals[b] and vals[b] != "--" for b in BUR):
                        clean = {b: ("" if vals[b] in ("--", "-") else vals[b]) for b in BUR}
                        cur["fields"][key] = clean
                # tabla de morosidades
                lab_late = _LATE_LABELS.get(low_join.split("  ")[0].strip()) or _LATE_LABELS.get(
                    (label.lower().strip()))
                if lab_late:
                    for b in BUR:
                        cur["late"].setdefault(b, {})[lab_late] = vals[b]

            prev_text = joined
    flush()

    # Si el reporte trae AMBOS idiomas (reporte con "todas las opciones"), cada
    # cuenta aparece 2 veces. Nos quedamos con el idioma más completo para no
    # duplicar. En reportes de un solo idioma esto no cambia nada.
    langs = {a.get("lang") for a in accounts if a.get("lang")}
    if "es" in langs and "en" in langs:
        by_lang = {"es": [], "en": []}
        for a in accounts:
            by_lang.get(a.get("lang"), by_lang["en"]).append(a)
        def _completeness(lst):
            return sum(len(a["fields"]) + len(a.get("late", {})) for a in lst)
        keep = "en" if _completeness(by_lang["en"]) >= _completeness(by_lang["es"]) else "es"
        accounts = [a for a in accounts if a.get("lang") == keep]

    return _dedup_accounts(accounts)


# ------------------------- orquestación motor -------------------------
def build_report_epicpro(pdf_path: str) -> dict:
    try:
        import report_parser as RP
    except ImportError:
        import original_parser as RP

    pages = _load(pdf_path)
    anchors = _find_anchors(pages) or {"equifax": 179.0, "experian": 375.0, "transunion": 479.0}

    accounts = _parse_accounts(pages, anchors)
    inventory = _build_inventory(accounts)

    report_date = _report_date(pages)
    scores = _scores(pages)
    personal_info, ssn_by_bureau = _personal_info(pages, anchors)
    try:
        personal_info_issues = RP.detect_personal_info_issues(personal_info)
    except Exception:
        personal_info_issues = []
    _ssns = {v for v in ssn_by_bureau.values() if v}
    if len(_ssns) > 1:
        personal_info_issues.append({
            "type": "ssn_inconsistency", "severity": "high", "bureaus": ssn_by_bureau,
            "description": ("The Social Security Number is reported differently across "
                            "bureaus. This is a strong mixed-file indicator and must be "
                            "corrected under 15 U.S.C. section 1681e(b)."),
        })

    negatives = RP.build_negative_inventory_by_bureau(inventory)
    negatives = RP.build_dofd_engine(negatives, report_date)
    legal = RP.build_legal_detection_engine(negatives, {}, report_date=report_date, client_state="")
    legal_summary = RP.build_legal_detection_summary(negatives, legal)
    scoring = RP.build_attack_scoring_engine(legal)
    strategy = RP.build_strategy_engine(scoring)
    lei = RP.build_letter_input_engine(strategy, negatives)
    dispute_letters = RP.build_dispute_letter_engine(
        lei, consumer_name="[CLIENT NAME]", report_date=report_date,
        personal_info=personal_info, personal_info_issues=personal_info_issues)
    furnisher_letters = RP.build_furnisher_letter_engine(
        lei, consumer_name="[CLIENT NAME]", report_date=report_date)

    return {
        "source": "epic_pro",
        "report_date": report_date,
        "scores": scores,
        "personal_info": personal_info,
        "personal_info_issues": personal_info_issues,
        "inquiries": [],
        "inquiry_attacks": [],
        "inquiry_letters": [],
        "inventory_by_bureau": inventory,
        "negatives_by_bureau": negatives,
        "legal_detection_engine": legal,
        "legal_detection_summary": legal_summary,
        "attack_scoring_engine": scoring,
        "strategy_engine": strategy,
        "letter_input_engine": lei,
        "dispute_letters": dispute_letters,
        "furnisher_letters": furnisher_letters,
        "expanded_accounts_found": sum(len(v) for v in inventory.values()),
        "raw_accounts": len(accounts),
    }


# --------------------------- metadatos varios ---------------------------
def _report_date(pages):
    for p in pages[:3]:
        for pat in (r"Date of Credit Report:\s*([\d/\-]+)",
                    r"Fecha del informe crediticio:\s*([\d/\-]+)"):
            m = re.search(pat, p["text"])
            if m:
                return m.group(1)
    return ""


def _scores(pages):
    return {}


_PI_MAP_EN = {
    "consumer's primary name": "name", "consumer's birth year": "dob",
    "consumer's other name(s)": "aka",
    "social security number/tax fraud identification": "ssn",
}
_PI_MAP_ES = {
    "nombre principal del consumidor": "name",
    "año de nacimiento del consumidor": "dob", "ano de nacimiento del consumidor": "dob",
    "otros nombres del consumidor": "aka",
    "número de seguro social/identificación de delito fiscal": "ssn",
    "numero de seguro social/identificacion de delito fiscal": "ssn",
}


def _personal_info(pages, anchors):
    pi = {
        "name_by_bureau": {}, "dob_by_bureau": {}, "aka_by_bureau": {},
        "former_name_by_bureau": {},
        "current_addresses_by_bureau": {b: [] for b in BUR},
        "previous_addresses_by_bureau": {b: [] for b in BUR},
        "current_addresses": [], "previous_addresses": [], "raw_block": [],
    }
    ssn_by_bureau = {}
    in_pi = False
    cur_anchors = dict(anchors)
    for p in pages:
        t = p["text"].lower()
        if "información personal" in t or "informacion personal" in t or "personal information" in t:
            in_pi = True
        if "resumen del informe" in t or "report summary" in t or "resumen de la cuenta" in t \
                or "account summary" in t:
            if in_pi:
                in_pi = False
        if not in_pi:
            continue
        for row in _rows(p["words"]):
            ha = _header_anchors(row)
            if ha:
                cur_anchors = ha
                continue
            label, vals = _assign(row, cur_anchors)
            ll = label.lower().strip().rstrip(":")
            key = _PI_MAP_EN.get(ll) or _PI_MAP_ES.get(ll)
            if not key:
                continue
            for b in BUR:
                v = vals[b].strip()
                if not v or v in ("-", "--"):
                    continue
                if key == "name":
                    pi["name_by_bureau"][b] = v
                elif key == "dob":
                    pi["dob_by_bureau"][b] = v
                elif key == "aka":
                    pi["aka_by_bureau"][b] = v
                elif key == "ssn":
                    ssn_by_bureau[b] = v
    return pi, ssn_by_bureau


# ------------------------------ detección ------------------------------
def looks_like_epicpro(text: str) -> bool:
    """True si el texto (primeras páginas) es el formato epic-pro."""
    t = (text or "").lower()
    if "epic-pro-report" in t:
        return True
    has_pi = ("personal information" in t or "información personal" in t
              or "informacion personal" in t)
    has_primary = ("consumer's primary name" in t
                   or "nombre principal del consumidor" in t)
    return has_pi and has_primary
