"""
Three Bureau Credit Report adapter (MyFreeScore / SmartCredit / ScoreSense-style)
=================================================================================

Estos servicios emiten un "Three Bureau Credit Report" con layout tri-columna
(Equifax | Experian | TransUnion) por cuenta. NO es el formato de IdentityIQ,
por eso el parser principal devolvia cero. Este adaptador extrae ese formato y
lo convierte a la MISMA estructura canonica (inventory_by_bureau) que consume el
motor, para que el downstream (detectores, cartas, e-OSCAR) corra sin cambios.

Cubre MyFreeScore y SmartCredit: ambos comparten exactamente el mismo layout.

Entrypoint: build_report_threebureau(pdf_path) -> dict (compatible build_report).
"""
from __future__ import annotations
import re
from collections import defaultdict

BUR3 = ["Equifax", "Experian", "TransUnion"]
BURKEY = {"Equifax": "equifax", "Experian": "experian", "TransUnion": "transunion"}
BUR = ("equifax", "experian", "transunion")


# ----------------------------- extraccion base -----------------------------
def _load(path):
    import pdfplumber
    pdf = pdfplumber.open(path)
    pages = []
    for pg in pdf.pages:
        pages.append({
            "text": pg.extract_text() or "",
            "words": pg.extract_words(use_text_flow=False, keep_blank_chars=False),
        })
    return pages


def _col_anchors(words):
    a = {}
    for w in words:
        if w["text"] in BUR3 and w["text"] not in a:
            a[w["text"]] = w["x0"]
    return a if len(a) == 3 else None


def _lines(words, ytol=3):
    d = defaultdict(list)
    for w in words:
        d[round(w["top"] / ytol)].append(w)
    return [sorted(d[k], key=lambda w: w["x0"]) for k in sorted(d)]


def _assign(ws, anchors):
    order = sorted(anchors.items(), key=lambda kv: kv[1])
    xs = [x for _, x in order]
    names = [n for n, _ in order]
    b0 = xs[0] - 5
    bounds = [(xs[0] + xs[1]) / 2, (xs[1] + xs[2]) / 2]
    label, cols = [], {n: [] for n in names}
    for w in ws:
        x = w["x0"]
        if x < b0:
            label.append(w["text"])
        elif x < bounds[0]:
            cols[names[0]].append(w["text"])
        elif x < bounds[1]:
            cols[names[1]].append(w["text"])
        else:
            cols[names[2]].append(w["text"])
    return " ".join(label).strip(), {BURKEY[n]: " ".join(cols[n]).strip() for n in names}


_PI_LABELS = [
    "Information Reported", "Social Security Number", "Date Of Birth",
    "Formerly Known As", "Also Known As", "Date Reported", "Address",
    "Status", "Name",
]


def _assign_pi(ws, anchors):
    """Como _assign pero anclando la ETIQUETA en labels conocidos de la seccion
    de info personal (la columna Equifax se corre a la izquierda y contamina la
    etiqueta si se usa el corte por posicion)."""
    order = sorted(anchors.items(), key=lambda kv: kv[1])
    xs = [x for _, x in order]
    names = [n for n, _ in order]
    bounds = [(xs[0] + xs[1]) / 2, (xs[1] + xs[2]) / 2]
    wsort = sorted(ws, key=lambda w: w["x0"])
    words_txt = [w["text"] for w in wsort]
    label = ""
    n_label = 0
    for lab in _PI_LABELS:
        lw = lab.split()
        if len(words_txt) >= len(lw) and " ".join(words_txt[:len(lw)]).lower() == lab.lower():
            label = lab
            n_label = len(lw)
            break
    cols = {n: [] for n in names}
    for w in wsort[n_label:]:
        x = w["x0"]
        if x < bounds[0]:
            cols[names[0]].append(w["text"])
        elif x < bounds[1]:
            cols[names[1]].append(w["text"])
        else:
            cols[names[2]].append(w["text"])
    return label, {BURKEY[n]: " ".join(cols[n]).strip() for n in names}


def _scores(pages):
    for p in pages:
        if "Credit Score and Rating" in p["text"]:
            m = re.search(r'Equifax\d?\s+Experian\d?\s+TransUnion\d?\s*\n\s*(\d{3})\s+(\d{3})\s+(\d{3})', p["text"])
            if m:
                return {"equifax": int(m.group(1)), "experian": int(m.group(2)), "transunion": int(m.group(3))}
    return {}


def _report_date(pages):
    for p in pages[:6]:
        m = re.search(r'Report Date\s+([A-Z][a-z]{2} \d{2}, \d{4})', p["text"])
        if m:
            return m.group(1)
    return ""


def _inquiries(pages):
    out = []
    in_sec = False
    cur = None
    date_re = re.compile(r'^([A-Z][a-z]{2} \d{2}, \d{4})\s+(.+)$')
    for p in pages:
        for ln in p["text"].splitlines():
            s = ln.strip()
            if re.match(r'^9\.\s+Inquiries', s):
                in_sec = True
            if re.match(r'^10\.\s+Public Records', s):
                in_sec = False
            if not in_sec:
                continue
            if s in BUR3:
                cur = BURKEY[s]
                continue
            m = date_re.match(s)
            if cur and m:
                out.append({"bureau": cur, "date": m.group(1), "company": m.group(2).strip()})
    return out


def _collections(pages):
    cols = {b: [] for b in BUR}
    in_sec = False
    cur = None
    entry = None

    def flush():
        nonlocal entry
        if entry and cur:
            cols[cur].append(entry)
        entry = None

    for p in pages:
        for ln in p["text"].splitlines():
            s = ln.strip()
            if re.match(r'^11\.\s+Collections', s):
                in_sec = True
                continue
            if re.match(r'^12\.\s+Dispute', s):
                flush()
                in_sec = False
            if not in_sec:
                continue
            if s in BUR3:
                flush()
                cur = BURKEY[s]
                continue
            if s.startswith("Date Reported:"):
                flush()
                entry = {"date_reported": s.split(":", 1)[1].strip()}
                continue
            if entry is not None:
                if s.startswith("Agency Client:"):
                    entry["name"] = s.split(":", 1)[1].strip()
                elif s.startswith("Amount"):
                    m = re.search(r'\$[\d,]+', s)
                    entry["balance"] = m.group(0) if m else "$0"
                elif s.startswith("Original Amount Owed"):
                    m = re.search(r'\$[\d,]+', s)
                    entry["original_amount"] = m.group(0) if m else ""
                elif s.startswith("Account Number"):
                    m = re.search(r'([x\d]+ *\d{3,4})\s*$', s)
                    entry["account_number"] = m.group(1) if m else s.replace("Account Number", "").strip()
    flush()
    return cols


NEGSUM = {
    "30 Days Past Due": "d30", "60 Days Past Due": "d60", "90 Days Past Due": "d90",
    "120 Days Past Due": "d120", "Collection Account": "coll", "Charge Off": "co",
    "Included in Bankruptcy": "bk", "Repossession": "repo",
}
FIELDS_WANTED = {
    "account number", "account status", "status", "reported balance", "balance",
    "credit limit", "loan type", "date opened", "date of first delinquency",
    "activity designator", "high credit",
}


def _tradelines(pages):
    header_re = re.compile(r'^(\d+)\.(\d+)\s+(.+)$')
    accounts = []
    cur = None
    anchors = None
    for p in pages:
        a = _col_anchors(p["words"])
        if a:
            anchors = a
        for ln in p["text"].splitlines():
            s = ln.strip()
            h = header_re.match(s)
            if h and int(h.group(1)) in (2, 3, 4, 5):
                if cur:
                    accounts.append(cur)
                name = re.sub(r'\s*\(CLOSED\)\s*$', '', h.group(3)).strip()
                cur = {"name": name, "num": f"{h.group(1)}.{h.group(2)}", "raw": []}
            elif cur is not None:
                cur["raw"].append(s)
        if cur is not None and anchors:
            for ws in _lines(p["words"]):
                label, vals = _assign(ws, anchors)
                ll = label.lower()
                if ll in FIELDS_WANTED:
                    cur.setdefault("fields", {}).setdefault(label, vals)
                for k, short in NEGSUM.items():
                    if ll == k.lower():
                        cur.setdefault("negsum", {})[short] = vals
    if cur:
        accounts.append(cur)
    # dedupe por num, preferir instancia con datos
    best = {}
    for a in accounts:
        sc = len(a.get("negsum", {})) + len(a.get("fields", {}))
        n = a["num"]
        if n not in best or sc > best[n][0]:
            best[n] = (sc, a)
    return [v[1] for v in sorted(best.values(), key=lambda t: t[1]["num"])]


def _neg_tags(acc):
    ns = acc.get("negsum", {})

    def pos(v):
        try:
            return int(re.sub(r'\D', '', v or "0") or 0)
        except Exception:
            return 0
    res = {}
    for b in BUR:
        tags = []
        if pos(ns.get("co", {}).get(b)):
            tags.append("charge_off")
        if pos(ns.get("coll", {}).get(b)):
            tags.append("collection")
        if pos(ns.get("repo", {}).get(b)):
            tags.append("repossession")
        if pos(ns.get("bk", {}).get(b)):
            tags.append("bankruptcy")
        if any(pos(ns.get(k, {}).get(b)) for k in ("d30", "d60", "d90", "d120")):
            tags.append("late_payment")
        if tags:
            res[b] = tags
    return res


def _personal_info(pages):
    """Extrae info personal por buro en el formato canonico del motor
    (name/dob/former/aka + current/previous addresses) + ssn_by_bureau."""
    pi = {
        "name_by_bureau": {}, "dob_by_bureau": {}, "aka_by_bureau": {},
        "former_name_by_bureau": {},
        "current_addresses_by_bureau": {b: [] for b in BUR},
        "previous_addresses_by_bureau": {b: [] for b in BUR},
        "current_addresses": [], "previous_addresses": [], "raw_block": [],
    }
    ssn_by_bureau = {}
    in_sec = False
    in_contact = False
    anchors = None
    pend_street = None
    pend_addr = None
    for p in pages:
        t = p["text"]
        if "8. Personal Information" in t:
            in_sec = True
        if "9. Inquiries" in t or "10. Public Records" in t:
            in_sec = False
        if not in_sec:
            continue
        a = _col_anchors(p["words"])
        if a:
            anchors = a
        if not anchors:
            continue
        for ws in _lines(p["words"]):
            line_txt = " ".join(w["text"] for w in sorted(ws, key=lambda w: w["x0"])).lower()
            if "contact information" in line_txt:
                in_contact = True
                pend_street = None
                pend_addr = None
                continue
            if "employment history" in line_txt:
                in_contact = False
                pend_street = None
                pend_addr = None
                continue
            label, vals = _assign_pi(ws, anchors)
            ll = label.lower().strip()
            if not in_contact:
                if ll == "name":
                    for b in BUR:
                        if vals[b] and vals[b].lower() != "n/a":
                            pi["name_by_bureau"][b] = vals[b]
                elif ll == "formerly known as":
                    for b in BUR:
                        if vals[b] and vals[b].lower() != "n/a":
                            pi["former_name_by_bureau"][b] = vals[b]
                elif ll in ("also known as", "aka"):
                    for b in BUR:
                        if vals[b] and vals[b].lower() != "n/a":
                            pi["aka_by_bureau"][b] = vals[b]
                elif ll == "social security number":
                    for b in BUR:
                        if vals[b] and vals[b].lower() != "n/a":
                            ssn_by_bureau[b] = vals[b]
                elif ll == "date of birth":
                    for b in BUR:
                        if vals[b] and vals[b].lower() != "n/a":
                            pi["dob_by_bureau"][b] = vals[b]
            else:
                if ll == "address":
                    pend_street = {b: vals[b] for b in BUR}
                    pend_addr = None
                elif ll == "status":
                    if pend_street is not None:
                        for b in BUR:
                            street = pend_street.get(b, "")
                            cont = (pend_addr or {}).get(b, "") if pend_addr else ""
                            parts = [x for x in (street, cont) if x and x.lower() != "n/a"]
                            full = ", ".join(parts).strip(", ").strip()
                            if not full or full.lower() == "n/a":
                                continue
                            st = vals[b].lower()
                            if "current" in st:
                                pi["current_addresses_by_bureau"][b].append(full)
                            elif "former" in st or "previous" in st:
                                pi["previous_addresses_by_bureau"][b].append(full)
                    pend_street = None
                    pend_addr = None
                elif ll == "" and pend_street is not None and pend_addr is None:
                    pend_addr = {b: vals[b] for b in BUR}
    return pi, ssn_by_bureau


# ----------------------- canonico + orquestacion motor ----------------------
def _fval(a, keys, b):
    f = a.get("fields", {})
    for k in keys:
        for fk, vals in f.items():
            if fk.lower() == k:
                return vals.get(b, "")
    return ""


def _build_inventory(tradelines, collections):
    inv = {b: [] for b in BUR}
    for a in tradelines:
        neg = _neg_tags(a)
        for b in BUR:
            acct = _fval(a, ["account number"], b)
            tags = neg.get(b, [])
            if not (acct or tags):
                continue
            pay, raw = [], [a["name"]]
            if "charge_off" in tags:
                pay.append("Charge Off"); raw.append("charged off")
            if "collection" in tags:
                pay.append("Collection"); raw.append("collection")
            if "repossession" in tags:
                raw.append("repossession")
            if "bankruptcy" in tags:
                raw.append("included in bankruptcy")
            if "late_payment" in tags:
                pay.append("Late")
            inv[b].append({
                "name": a["name"], "bureau": b, "account_number": acct or "",
                "status": _fval(a, ["account status", "status"], b),
                "payment_status": " ".join(pay),
                "balance": _fval(a, ["reported balance", "balance"], b) or "$0.00",
                "past_due": "$0.00", "credit_limit": _fval(a, ["credit limit"], b) or "$0.00",
                "high_credit": _fval(a, ["high credit"], b) or "$0.00",
                "monthly_payment": "$0.00", "comments": "",
                "account_type_detail": _fval(a, ["loan type"], b) or "",
                "account_type": "", "raw_lines": raw,
                "has_30_in_history": "late_payment" in tags,
                "date_opened": _fval(a, ["date opened"], b),
                "block_id": a["num"],
            })
    for b, items in collections.items():
        for it in items:
            inv[b].append({
                "name": it.get("name", ""), "bureau": b,
                "account_number": it.get("account_number", ""),
                "status": "Collection", "payment_status": "Collection",
                "balance": it.get("balance", "$0.00"), "past_due": "$0.00",
                "credit_limit": "$0.00", "high_credit": "$0.00", "monthly_payment": "$0.00",
                "comments": "placed for collection", "account_type_detail": "collection",
                "account_type": "", "raw_lines": [it.get("name", ""), "collection"],
                "has_30_in_history": False,
                "block_id": "col_" + (it.get("account_number", "") or it.get("name", "")[:8]),
            })
    return inv


def build_report_threebureau(pdf_path: str) -> dict:
    try:
        import report_parser as RP
    except ImportError:
        import original_parser as RP
    pages = _load(pdf_path)
    import ultra4k as U4
    if U4.is_ultra4k(pages):
        # SmartCredit "Ultra 4k 3B Report" (label-arriba / valores-abajo)
        _al = U4._all_lines(pages)
        _xs = U4._anchors(_al)
        scores = U4.scores(pages)
        report_date = U4.report_date(pages)
        inventory = U4.build_inventory(U4.accounts(_al, _xs))
        inquiries = U4.inquiries(pages)
        personal_info = U4.personal_info(pages, _al, _xs)
        ssn_by_bureau = {}
        _rawn = sum(len(v) for v in inventory.values())
    else:
        # "Three Bureau Credit Report" (MyFreeScore / SmartCredit clasico)
        scores = _scores(pages)
        report_date = _report_date(pages)
        tradelines = _tradelines(pages)
        collections = _collections(pages)
        inquiries = _inquiries(pages)
        personal_info, ssn_by_bureau = _personal_info(pages)
        inventory = _build_inventory(tradelines, collections)
        _rawn = len(tradelines)
    personal_info_issues = RP.detect_personal_info_issues(personal_info)
    _ssns = {v for v in ssn_by_bureau.values() if v}
    if len(_ssns) > 1:
        personal_info_issues.append({
            "type": "ssn_inconsistency", "severity": "high",
            "bureaus": ssn_by_bureau,
            "description": ("The Social Security Number is reported differently across "
                "bureaus: " + ", ".join(f"{b}={v}" for b, v in ssn_by_bureau.items() if v)
                + ". This is a strong mixed-file indicator and must be corrected under "
                "15 U.S.C. section 1681e(b)."),
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
        "source": "three_bureau",
        "report_date": report_date,
        "scores": scores,
        "personal_info": personal_info,
        "personal_info_issues": personal_info_issues,
        "inquiries": inquiries,
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
        "raw_accounts": _rawn,
    }
