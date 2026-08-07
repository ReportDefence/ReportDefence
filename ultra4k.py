"""Extractor para SmartCredit 'Ultra 4k 3B Report' (formato distinto al
'Three Bureau Credit Report'). Etiqueta en una linea, 3 valores en la siguiente,
columnas TransUnion/Experian/Equifax. Devuelve estructuras canonicas."""
import re
from collections import defaultdict

BURKEY = {"TransUnion": "transunion", "Experian": "experian", "Equifax": "equifax"}
BUR = ("transunion", "experian", "equifax")


def _lines(words, ytol=3):
    d = defaultdict(list)
    for w in words:
        d[round(w["top"] / ytol)].append(w)
    return [sorted(d[k], key=lambda w: w["x0"]) for k in sorted(d)]


def _all_lines(pages):
    out = []
    for p in pages:
        for ws in _lines(p["words"]):
            out.append(ws)
    return out


def _txt(ws):
    return " ".join(w["text"] for w in sorted(ws, key=lambda w: w["x0"])).strip()


def is_ultra4k(pages):
    full = "\n".join(p["text"] for p in pages)
    return ("Account Rating" in full and "Days Late - Last 7 Years" in full) or "Ultra 4k" in full


def _is_header(ws):
    texts = [w["text"] for w in ws]
    return "TransUnion" in texts and "Experian" in texts and "Equifax" in texts


def _anchors(all_lines):
    for ws in all_lines:
        if _is_header(ws):
            xs = {}
            for w in ws:
                if w["text"] in ("TransUnion", "Experian", "Equifax"):
                    xs[w["text"]] = w["x0"]
            if len(xs) == 3:
                return xs
    return None


def _split(ws, xs):
    order = sorted([("transunion", xs["TransUnion"]), ("experian", xs["Experian"]),
                    ("equifax", xs["Equifax"])], key=lambda t: t[1])
    b1 = (order[0][1] + order[1][1]) / 2
    b2 = (order[1][1] + order[2][1]) / 2
    lab_max = order[0][1] - 35
    cols = {"transunion": [], "experian": [], "equifax": []}
    label = []
    for w in sorted(ws, key=lambda w: w["x0"]):
        x = w["x0"]
        if x < lab_max:
            label.append(w["text"])
        elif x < b1:
            cols[order[0][0]].append(w["text"])
        elif x < b2:
            cols[order[1][0]].append(w["text"])
        else:
            cols[order[2][0]].append(w["text"])
    return " ".join(label).strip(), {b: " ".join(v).strip() for b, v in cols.items()}


def scores(pages):
    for p in pages[:4]:
        t = p["text"]
        if "Credit Scores" in t:
            m = re.search(r'TransUnion\S*\s+Experian\S*\s+Equifax\S*\s*\n\s*(\d{3})\s+(\d{3})\s+(\d{3})', t)
            if m:
                return {"transunion": int(m.group(1)), "experian": int(m.group(2)), "equifax": int(m.group(3))}
    return {}


def report_date(pages):
    m = re.search(r'Credit Report Date\s*\n\s*(\d{2}/\d{2}/\d{4})', "\n".join(p["text"] for p in pages[:3]))
    return m.group(1) if m else ""


U4K_FIELDS = {
    "account number": "account_number", "balance owed": "balance_owed",
    "account rating": "account_rating", "payment status": "payment_status",
    "creditor type": "creditor_type", "account status": "account_status",
    "account type": "account_type", "credit limit": "credit_limit",
    "high balance": "high_balance", "date opened": "date_opened",
    "past due amount": "past_due",
}


def _looks_like_name(t):
    if not t or len(t) > 45:
        return False
    low = t.lower()
    bad = ["reporting di", "months show", "http", "below are", "below is",
           "this information", "revolving accounts", "installment accounts",
           "collections accounts", "public records", "inquiries", "summary",
           "positive accounts", "negative accounts", "personal information",
           "creditor contacts", "days late", "payment history"]
    if any(x in low for x in bad):
        return False
    letters = sum(c.isalpha() for c in t)
    return letters >= 2


def _clean_name(t):
    t = re.split(r'\s+\d+\s+Credit Reporting|\s+Credit Reporting Di|\s+months show', t)[0]
    t = re.sub(r'\s+[X\d]{2,}\*+\d*$', '', t)  # numero de cuenta enmascarado al final
    return t.strip()


def accounts(all_lines, xs):
    idxs = [i for i, ws in enumerate(all_lines) if _is_header(ws)]
    accts = []
    for k, i in enumerate(idxs):
        desc = _txt(all_lines[i - 1]) if i >= 1 else ""
        # solo bloques de cuenta reales: el descriptor trae Positive/Negative
        if not re.search(r'\b(Positive|Negative)\b', desc, re.I):
            continue
        # nombre: primera linea plausible hacia atras a partir de i-2
        name = ""
        for back in range(2, 6):
            if i - back < 0:
                break
            cand = _clean_name(_txt(all_lines[i - back]))
            if _looks_like_name(cand):
                name = cand
                break
        end = (idxs[k + 1] - 2) if k + 1 < len(idxs) else len(all_lines)
        body = all_lines[i + 1:end]
        acc = {"name": name, "desc": desc, "fields": {}, "dayslate": {}}
        j = 0
        while j < len(body):
            lab, _ = _split(body[j], xs)
            ll = lab.lower().strip()
            low_line = _txt(body[j]).lower()
            if ll in U4K_FIELDS and j + 1 < len(body):
                _, vv = _split(body[j + 1], xs)
                acc["fields"][U4K_FIELDS[ll]] = vv
                j += 2
                continue
            if "days late" in low_line:
                for jj in range(j + 1, min(j + 6, len(body))):
                    tt = _txt(body[jj])
                    m = re.match(r'(TransUnion|Experian|Equifax)\S*\s+(.+)$', tt)
                    if m:
                        nums = re.findall(r'\d+', m.group(2))
                        b = BURKEY[m.group(1)]
                        acc["dayslate"][b] = tuple(int(x) for x in (nums + [0, 0, 0])[:3])
                j += 1
                continue
            j += 1
        accts.append(acc)
    return accts


def _norm(s):
    return (s or "").replace("!", "ff")


def neg_tags(acc, b):
    f = acc["fields"]
    an = f.get("account_number", {}).get(b, "")
    if not an or an in ("——", "--", "–––"):
        return None, []
    ps = _norm(f.get("payment_status", {}).get(b, "")).lower()
    ar = f.get("account_rating", {}).get(b, "").lower()
    ct = f.get("creditor_type", {}).get(b, "").lower()
    dl = acc["dayslate"].get(b, (0, 0, 0))
    tags = []
    if "collection" in ct:
        tags.append("collection")
    elif "chargeo" in ps or "charge off" in ps or "charged off" in ar:
        tags.append("charge_off")
    if any(dl) or "late" in ps:
        tags.append("late_payment")
    if "derogatory" in ar and not tags:
        tags.append("charge_off")
    return an, tags


def build_inventory(accts):
    inv = {b: [] for b in BUR}
    for a in accts:
        for b in BUR:
            an, tags = neg_tags(a, b)
            if an is None:
                continue
            f = a["fields"]
            pay, raw = [], [a["name"]]
            if "charge_off" in tags:
                pay.append("Charge Off"); raw.append("charged off")
            if "collection" in tags:
                pay.append("Collection"); raw.append("collection")
            if "late_payment" in tags:
                pay.append("Late")
            inv[b].append({
                "name": a["name"], "bureau": b, "account_number": an,
                "status": f.get("account_status", {}).get(b, ""),
                "payment_status": " ".join(pay),
                "balance": f.get("balance_owed", {}).get(b, "") or "$0.00",
                "past_due": f.get("past_due", {}).get(b, "") or "$0.00",
                "credit_limit": f.get("credit_limit", {}).get(b, "") or "$0.00",
                "high_credit": f.get("high_balance", {}).get(b, "") or "$0.00",
                "monthly_payment": "$0.00", "comments": "",
                "account_type_detail": f.get("account_type", {}).get(b, ""),
                "account_type": "", "raw_lines": raw,
                "has_30_in_history": "late_payment" in tags,
                "date_opened": f.get("date_opened", {}).get(b, ""),
                "block_id": (a["name"][:10] + an[-4:]).replace(" ", ""),
            })
    return inv


def personal_info(pages, all_lines, xs):
    pi = {
        "name_by_bureau": {}, "dob_by_bureau": {}, "aka_by_bureau": {},
        "former_name_by_bureau": {},
        "current_addresses_by_bureau": {b: [] for b in BUR},
        "previous_addresses_by_bureau": {b: [] for b in BUR},
        "current_addresses": [], "previous_addresses": [], "raw_block": [],
    }
    # localizar rango de la seccion Personal Information
    start = end = None
    for i, ws in enumerate(all_lines):
        t = _txt(ws)
        if start is None and "Personal Information" in t:
            start = i
        elif start is not None and t.startswith("Summary"):
            end = i
            break
    if start is None:
        return pi
    body = all_lines[start:end or len(all_lines)]
    LABELS = {"name": "name", "also known as": "aka", "date of birth": "dob",
              "current address": "current", "previous address": "previous", "employer": "emp"}
    j = 0
    while j < len(body):
        lab, _ = _split(body[j], xs)
        ll = lab.lower().strip()
        if ll in LABELS and j + 1 < len(body):
            kind = LABELS[ll]
            # valores: 1 o 2 lineas hasta la proxima etiqueta
            vlines = []
            jj = j + 1
            while jj < len(body):
                lab2, vv = _split(body[jj], xs)
                if lab2.lower().strip() in LABELS:
                    break
                vlines.append(vv)
                jj += 1
                if kind not in ("current", "previous") and len(vlines) >= 1:
                    break
                if len(vlines) >= 2:
                    break
            for b in BUR:
                parts = [vl[b] for vl in vlines if vl.get(b) and vl[b] not in ("——", "--")]
                val = ", ".join(parts).strip(", ").strip()
                if not val:
                    continue
                if kind == "name":
                    pi["name_by_bureau"][b] = val
                elif kind == "aka":
                    pi["aka_by_bureau"][b] = val
                elif kind == "dob":
                    pi["dob_by_bureau"][b] = val
                elif kind == "current":
                    pi["current_addresses_by_bureau"][b].append(val)
                elif kind == "previous":
                    pi["previous_addresses_by_bureau"][b].append(val)
            j = jj
            continue
        j += 1
    return pi


def inquiries(pages):
    out = []
    full = "\n".join(p["text"] for p in pages)
    if "Inquiries" not in full:
        return out
    lines = full.splitlines()
    in_sec = False
    for idx, ln in enumerate(lines):
        s = ln.strip()
        if re.match(r'^Inquiries\b', s):
            in_sec = True
        if s.startswith("Creditor Contacts"):
            in_sec = False
        if not in_sec:
            continue
        m = re.match(r'^(\d{1,2}/\d{1,2}/\d{4})\s+(TransUnion|Experian|Equifax)', s)
        if m:
            company = lines[idx + 1].strip() if idx + 1 < len(lines) else ""
            out.append({"bureau": BURKEY[m.group(2)], "date": m.group(1), "company": company})
    return out
