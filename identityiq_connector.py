"""
identityiq_connector.py
=======================
Report Defence – IdentityIQ Direct Integration

Handles:
1. Programmatic login to member.identityiq.com
2. JSON credit report extraction
3. Parsing the JSON into the same format as original_parser.py

The JSON endpoint returns a JSONP response:
  JSON_CALLBACK({ "BundleComponents": { "BundleComponent": [...] } })

The main data lives in the component with Type = "MergeCreditReports"
under TrueLinkCreditReportType.
"""

from __future__ import annotations

import re
import json
import hashlib
import httpx
from typing import Any


# ─────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────

BASE_URL       = "https://member.identityiq.com"
LOGIN_URL      = f"{BASE_URL}/api/login"          # confirmed ASP.NET SPA endpoint
JSON_REPORT    = f"{BASE_URL}/CreditReport.aspx?view=json"
BUREAUS        = ["transunion", "experian", "equifax"]

BUREAU_MAP = {
    "TUC": "transunion",
    "EFX": "equifax",
    "EXP": "experian",
    "TransUnion": "transunion",
    "Equifax":    "equifax",
    "Experian":   "experian",
}

DEFAULT_HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         f"{BASE_URL}/",
    "Origin":          BASE_URL,
}


# ─────────────────────────────────────────────
# STEP 1 — LOGIN
# ─────────────────────────────────────────────

def login_identityiq(username: str, password: str, ssn_last4: str) -> httpx.Client:
    """
    Authenticate to IdentityIQ and return an httpx.Client with
    the session cookies set.

    IdentityIQ is an ASP.NET SPA. The login flow:
    1. GET / → sets initial cookies (ASP.NET_SessionId, __RequestVerificationToken)
    2. POST /api/login with credentials → sets auth session
    3. GET /api/ssn-verification with SSN → completes MFA step

    Returns authenticated client ready for JSON report fetch.
    Raises ValueError on bad credentials.
    """
    client = httpx.Client(
        base_url=BASE_URL,
        headers=DEFAULT_HEADERS,
        follow_redirects=True,
        timeout=30.0,
    )

    # ── Step 1: GET homepage to initialize session + get CSRF token ──────
    print(f"[IIQ] Step 1: GET homepage for user={username}")
    resp = client.get("/")
    print(f"[IIQ] Step 1 response: {resp.status_code}, cookies: {list(resp.cookies.keys())}")
    resp.raise_for_status()

    # Extract __RequestVerificationToken if present in HTML
    csrf_token = ""
    match = re.search(
        r'<input[^>]+name=["\']__RequestVerificationToken["\'][^>]+value=["\']([^"\']+)["\']',
        resp.text, re.I
    )
    if match:
        csrf_token = match.group(1)

    # Also check cookies for antiforgery token
    for cookie_name in resp.cookies:
        if "token" in cookie_name.lower() or "csrf" in cookie_name.lower():
            csrf_token = resp.cookies[cookie_name]
            break

    # ── Step 2: POST login ───────────────────────────────────────────────
    login_payload = {
        "username": username,
        "password": password,
    }

    login_headers = {
        "Content-Type": "application/json",
        "X-Requested-With": "XMLHttpRequest",
    }
    if csrf_token:
        login_headers["__RequestVerificationToken"] = csrf_token
        login_headers["RequestVerificationToken"]   = csrf_token

    print(f"[IIQ] Step 2: POST /api/login")
    login_resp = client.post(
        "/api/login",
        json=login_payload,
        headers=login_headers,
    )
    print(f"[IIQ] Login response: {login_resp.status_code} body={login_resp.text[:300]}")

    # Handle non-JSON or error responses
    if login_resp.status_code == 404:
        # Try alternate login endpoints
        for endpoint in ["/Login.aspx", "/api/auth/login", "/api/account/login"]:
            login_resp = client.post(
                endpoint,
                json=login_payload,
                headers=login_headers,
            )
            if login_resp.status_code != 404:
                break

    if login_resp.status_code not in (200, 201, 302):
        raise ValueError(
            f"Login failed with status {login_resp.status_code}. "
            f"Check username and password."
        )

    # Check for error in response body
    try:
        login_data = login_resp.json()
        if isinstance(login_data, dict):
            if login_data.get("success") is False or login_data.get("error"):
                raise ValueError(
                    f"Login rejected: {login_data.get('message') or login_data.get('error')}"
                )
    except (json.JSONDecodeError, ValueError):
        pass

    # ── Step 3: SSN verification (if required) ───────────────────────────
    if ssn_last4:
        ssn_endpoints = [
            "/api/ssn-verification",
            "/api/account/verify-ssn",
            "/api/verify",
        ]
        ssn_payload = {"ssnLastFour": ssn_last4, "ssn": ssn_last4}

        for endpoint in ssn_endpoints:
            ssn_resp = client.post(
                endpoint,
                json=ssn_payload,
                headers={**login_headers, "Content-Type": "application/json"},
            )
            if ssn_resp.status_code in (200, 201):
                break

    print(f"[IIQ] After login, cookies: {list(client.cookies.keys())}")
    # Verify we have a session cookie
    if "ASP.NET_SessionId" not in client.cookies:
        # Try to detect session from any cookie
        session_cookies = [k for k in client.cookies if "session" in k.lower() or "auth" in k.lower()]
        if not session_cookies:
            raise ValueError(
                "Login appeared to succeed but no session cookie was set. "
                "Credentials may be incorrect or IdentityIQ requires additional verification."
            )

    return client


# ─────────────────────────────────────────────
# STEP 2 — FETCH JSON REPORT
# ─────────────────────────────────────────────

def fetch_json_report(client: httpx.Client) -> dict:
    """
    Fetch the JSONP credit report and parse it into a Python dict.
    """
    print(f"[IIQ] Fetching JSON report...")
    resp = client.get(
        "/CreditReport.aspx",
        params={"view": "json"},
        headers={"Accept": "application/json, text/javascript, */*"},
    )
    resp.raise_for_status()

    raw = resp.text.strip()

    # Strip JSONP wrapper: JSON_CALLBACK({...})
    if raw.startswith("JSON_CALLBACK("):
        raw = raw[len("JSON_CALLBACK("):]
        if raw.endswith(")"):
            raw = raw[:-1]
    elif raw.startswith("(") and raw.endswith(")"):
        raw = raw[1:-1]

    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f"Could not parse credit report JSON: {e}")


# ─────────────────────────────────────────────
# STEP 3 — PARSE JSON INTO REPORT FORMAT
# ─────────────────────────────────────────────

def _safe(val: Any, default: str = "") -> str:
    if val is None or val == "None" or val == "null":
        return default
    return str(val).strip()


def _get_merge_report(data: dict) -> dict:
    """Extract the MergeCreditReports component."""
    components = data.get("BundleComponents", {}).get("BundleComponent", [])
    for comp in components:
        if comp.get("Type", {}).get("$") == "MergeCreditReports":
            return comp.get("TrueLinkCreditReportType", {})
    raise ValueError("MergeCreditReports component not found in JSON")


def _parse_scores(merge: dict) -> dict:
    """Extract credit scores per bureau."""
    scores = {"transunion": None, "experian": None, "equifax": None}
    borrower = merge.get("Borrower", {})
    credit_scores = borrower.get("CreditScore", [])
    if isinstance(credit_scores, dict):
        credit_scores = [credit_scores]

    # Also check VantageScore components
    components = []
    try:
        components = merge.get("_parent_components", [])
    except Exception:
        pass

    for s in credit_scores:
        score_val = _safe(s.get("@riskScore"))
        bureau_code = _safe(s.get("@bureauCode"))
        bureau_key = BUREAU_MAP.get(bureau_code, "")
        if bureau_key and score_val:
            scores[bureau_key] = int(score_val) if score_val.isdigit() else None

    return scores


def _bureau_from_tradeline(t: dict) -> str:
    """Get normalized bureau key from a tradeline."""
    bureau_raw = _safe(t.get("@bureau"))
    if bureau_raw:
        return BUREAU_MAP.get(bureau_raw, bureau_raw.lower())

    # Fallback: check Source.Bureau
    source = t.get("Source", {})
    if isinstance(source, dict):
        bureau_obj = source.get("Bureau", {})
        if isinstance(bureau_obj, dict):
            sym = _safe(bureau_obj.get("@symbol"))
            desc = _safe(bureau_obj.get("@description"))
            return BUREAU_MAP.get(sym) or BUREAU_MAP.get(desc) or ""

    return ""


def _parse_pay_status(t: dict) -> str:
    """Get payment status string from tradeline."""
    pay = t.get("PayStatus", {})
    if isinstance(pay, dict):
        return _safe(pay.get("@description"))
    return _safe(pay)


def _parse_account_condition(t: dict) -> str:
    """Get account condition/status."""
    cond = t.get("AccountCondition", {})
    if isinstance(cond, dict):
        return _safe(cond.get("@description"))
    return _safe(cond)


def _parse_open_closed(t: dict) -> str:
    obj = t.get("OpenClosed", {})
    if isinstance(obj, dict):
        return _safe(obj.get("@description"))
    return _safe(obj)


def _parse_tradelines(merge: dict) -> list[dict]:
    """
    Parse all TradeLinePartitions into raw account blocks.
    Each partition can have multiple Tradeline entries (one per bureau).
    We preserve them as separate entries with block_id.
    """
    raw_accounts = []
    partitions = merge.get("TradeLinePartition", [])
    if isinstance(partitions, dict):
        partitions = [partitions]

    acct_type_desc = ""
    for pidx, partition in enumerate(partitions):
        acct_type_desc = _safe(partition.get("@accountTypeDescription"))
        tradelines = partition.get("Tradeline", [])
        if isinstance(tradelines, dict):
            tradelines = [tradelines]

        # Group tradelines in this partition by account number
        # (same account reported by multiple bureaus)
        by_acct: dict[str, list] = {}
        for t in tradelines:
            acct_num = _safe(t.get("@accountNumber"))
            by_acct.setdefault(acct_num, []).append(t)

        for acct_num, tlines in by_acct.items():
            # Build a block_id from partition index + account number
            block_key = f"{pidx}:{acct_num}"
            block_id  = "BLK-" + hashlib.md5(block_key.encode()).hexdigest()[:8].upper()

            # Gather per-bureau data
            bureau_data: dict[str, dict] = {}
            creditor_name = ""

            for t in tlines:
                bureau = _bureau_from_tradeline(t)
                if not bureau:
                    continue

                creditor_name = creditor_name or _safe(t.get("@creditorName"))
                gt = t.get("GrantedTrade", {}) or {}
                collection = t.get("CollectionTrade", {}) or {}

                # Payment history lates
                late_30 = int(_safe(gt.get("@late30Count", "0")) or 0)
                late_60 = int(_safe(gt.get("@late60Count", "0")) or 0)
                late_90 = int(_safe(gt.get("@late90Count", "0")) or 0)

                # Original creditor for collections
                orig_creditor = _safe(collection.get("@originalCreditor"))

                # Remark
                remark_obj = t.get("Remark", {}) or {}
                remark_code = remark_obj.get("RemarkCode", {}) or {}
                comments = _safe(remark_code.get("@description")) if isinstance(remark_code, dict) else ""

                bureau_data[bureau] = {
                    "account_number":    acct_num,
                    "status":            _parse_account_condition(t),
                    "payment_status":    _parse_pay_status(t),
                    "balance":           _safe(t.get("@currentBalance")),
                    "high_credit":       _safe(t.get("@highBalance")),
                    "credit_limit":      _safe(gt.get("@creditLimit") or t.get("@creditLimit")),
                    "monthly_payment":   _safe(gt.get("@monthlyPayment")),
                    "past_due":          _safe(gt.get("@amountPastDue")),
                    "date_opened":       _safe(t.get("@dateOpened")),
                    "date_last_active":  _safe(t.get("@dateVerified") or t.get("@dateReported")),
                    "date_of_last_payment": _safe(gt.get("@dateLastPayment") or t.get("@dateLastPayment")),
                    "last_reported":     _safe(t.get("@dateReported")),
                    "no_of_months":      _safe(gt.get("@termMonths") or gt.get("@monthsReviewed")),
                    "account_type":      acct_type_desc,
                    "account_type_detail": _safe(
                        collection.get("creditType", {}).get("@description", "") if isinstance(collection.get("creditType"), dict)
                        else ""
                    ) or acct_type_desc,
                    "bureau_code":       "Individual",
                    "comments":          comments,
                    "late_30":           late_30,
                    "late_60":           late_60,
                    "late_90":           late_90,
                    "original_creditor": orig_creditor,
                    "open_closed":       _parse_open_closed(t),
                }

            if not bureau_data:
                continue

            # Add original creditor to name if present
            display_name = creditor_name
            for bd in bureau_data.values():
                if bd.get("original_creditor"):
                    display_name = f"{creditor_name} (Original Creditor: {bd['original_creditor']})"
                    break

            raw_accounts.append({
                "block_id":    block_id,
                "name":        display_name,
                "bureau_data": bureau_data,
                "partition_idx": pidx,
            })

    return raw_accounts


def _parse_inquiries(merge: dict) -> list[dict]:
    """Parse InquiryPartition into inquiry list."""
    inquiries = []
    partitions = merge.get("InquiryPartition", [])
    if isinstance(partitions, dict):
        partitions = [partitions]

    for partition in partitions:
        inq_list = partition.get("Inquiry", [])
        if isinstance(inq_list, dict):
            inq_list = [inq_list]

        for inq in inq_list:
            subscriber = inq.get("Subscriber", {}) or {}
            name = _safe(subscriber.get("@name") or inq.get("@subscriberName") or inq.get("@name"))
            date = _safe(inq.get("@date") or inq.get("@inquiryDate"))
            bureau_raw = ""
            source = inq.get("Source", {}) or {}
            bureau_obj = source.get("Bureau", {}) if isinstance(source, dict) else {}
            if isinstance(bureau_obj, dict):
                bureau_raw = _safe(bureau_obj.get("@symbol") or bureau_obj.get("@description"))
            bureau = BUREAU_MAP.get(bureau_raw, bureau_raw.lower())

            inquiries.append({
                "name":   name,
                "date":   date,
                "bureau": bureau,
            })

    return inquiries


def _parse_scores_from_components(data: dict) -> dict:
    """Extract scores from VantageScore components."""
    scores = {"transunion": None, "experian": None, "equifax": None}
    components = data.get("BundleComponents", {}).get("BundleComponent", [])

    bureau_order = ["transunion", "equifax"]  # TUC first, then EQF in bundle
    score_idx = 0

    for comp in components:
        comp_type = comp.get("Type", {}).get("$", "")
        if "VantageScore" in comp_type:
            score_obj = comp.get("CreditScoreType", {})
            val = _safe(score_obj.get("@riskScore"))
            if val and score_idx < len(bureau_order):
                bureau = bureau_order[score_idx]
                scores[bureau] = int(val) if val.isdigit() else None
                score_idx += 1

    # Experian score from Borrower if available
    merge = _get_merge_report(data)
    borrower = merge.get("Borrower", {})
    credit_scores = borrower.get("CreditScore", [])
    if isinstance(credit_scores, dict):
        credit_scores = [credit_scores]
    for s in credit_scores:
        val = _safe(s.get("@riskScore"))
        if val and val.isdigit():
            # If experian is missing, fill it
            if scores["experian"] is None:
                scores["experian"] = int(val)

    return scores


def _build_inventory(raw_accounts: list[dict]) -> dict[str, list[dict]]:
    """Build inventory_by_bureau from raw accounts."""
    inventory = {b: [] for b in BUREAUS}

    for acct in raw_accounts:
        name     = acct["name"]
        block_id = acct["block_id"]

        for bureau, data in acct["bureau_data"].items():
            if bureau not in BUREAUS:
                continue

            late_30 = data.get("late_30", 0)
            late_60 = data.get("late_60", 0)
            late_90 = data.get("late_90", 0)

            inventory[bureau].append({
                "block_id":             block_id,
                "name":                 name,
                "account_number":       data["account_number"],
                "account_type":         data["account_type"],
                "account_type_detail":  data["account_type_detail"],
                "bureau_code":          data["bureau_code"],
                "status":               data["status"],
                "payment_status":       data["payment_status"],
                "balance":              data["balance"],
                "high_credit":          data["high_credit"],
                "credit_limit":         data["credit_limit"],
                "monthly_payment":      data["monthly_payment"],
                "past_due":             data["past_due"],
                "no_of_months":         data["no_of_months"],
                "date_opened":          data["date_opened"],
                "date_last_active":     data["date_last_active"],
                "date_of_last_payment": data["date_of_last_payment"],
                "last_reported":        data["last_reported"],
                "comments":             data["comments"],
                "has_30_in_history":    late_30 > 0,
                "has_60_in_history":    late_60 > 0,
                "has_90_in_history":    late_90 > 0,
                "late_payment_codes":   [],
                "payment_history":      [],
                "possible_duplicate_group": "",
                "raw_lines":            [],
            })

    return inventory


def _is_negative(acc: dict) -> bool:
    status  = acc.get("status", "").lower()
    payment = acc.get("payment_status", "").lower()
    name    = acc.get("name", "").lower()

    if "derogatory" in status:
        return True
    if "collection" in payment or "chargeoff" in payment or "charge off" in payment:
        return True
    if "late" in payment and "current" not in payment:
        return True
    if acc.get("has_30_in_history") or acc.get("has_60_in_history") or acc.get("has_90_in_history"):
        return True
    if any(k in name for k in ["lvnv", "midland", "portfolio", "cavalry", "resurgent"]):
        if "collection" in payment or "derogatory" in status:
            return True
    return False


def _negative_type(acc: dict) -> str:
    status  = acc.get("status", "").lower()
    payment = acc.get("payment_status", "").lower()
    name    = acc.get("name", "").lower()

    if "collection" in payment or "chargeoff" in payment:
        return "collection"
    if "derogatory" in status:
        return "charge_off"
    if acc.get("has_30_in_history") or acc.get("has_60_in_history") or acc.get("has_90_in_history"):
        return "late_payment"
    return "derogatory"


def parse_identityiq_json(data: dict) -> dict:
    """
    Main parser. Takes the parsed JSON dict and returns the same
    format as build_report() in original_parser.py.
    """
    merge = _get_merge_report(data)

    # Scores
    scores = _parse_scores_from_components(data)

    # Personal info
    borrower   = merge.get("Borrower", {})
    borrower_names = borrower.get("BorrowerName", [])
    if isinstance(borrower_names, dict):
        borrower_names = [borrower_names]

    full_name = ""
    if borrower_names:
        n = borrower_names[0]
        first = _safe(n.get("@first") or n.get("@firstName"))
        last  = _safe(n.get("@last") or n.get("@lastName"))
        full_name = f"{first} {last}".strip()

    personal_info = {
        "name":    {"transunion": full_name, "experian": full_name, "equifax": full_name},
        "address": {},
        "dob":     {},
    }

    # Tradelines → raw accounts → inventory
    raw_accounts = _parse_tradelines(merge)
    inventory    = _build_inventory(raw_accounts)

    # Negatives
    negatives_by_bureau: dict[str, list] = {b: [] for b in BUREAUS}
    for bureau, accts in inventory.items():
        for acc in accts:
            if _is_negative(acc):
                enriched = dict(acc)
                enriched["negative_type"] = _negative_type(acc)
                negatives_by_bureau[bureau].append(enriched)

    # Inquiries
    inquiries = _parse_inquiries(merge)

    # Attack count (simplified — just count negatives)
    attack_count = sum(len(v) for v in negatives_by_bureau.values())

    return {
        "source": "identityiq_json",
        "scores": scores,
        "personal_info": personal_info,
        "personal_info_issues": [],
        "inventory_by_bureau": inventory,
        "negatives_by_bureau": negatives_by_bureau,
        "inquiries": inquiries,
        "inquiry_attacks": [],
        "attack_count": attack_count,
        "attacks": [],
        "letter_input_engine": {b: {} for b in BUREAUS},
        "dispute_letters": {},
        "furnisher_letters": {},
        "report_date": "",
        "raw_accounts_count": len(raw_accounts),
    }


# ─────────────────────────────────────────────
# MAIN ENTRY POINT
# ─────────────────────────────────────────────

def pull_and_parse(username: str, password: str, ssn_last4: str) -> dict:
    """
    Full pipeline:
    1. Login to IdentityIQ
    2. Fetch JSON report
    3. Parse and return in standard Report Defence format

    Raises ValueError on auth failure or parse error.
    """
    client = login_identityiq(username, password, ssn_last4)
    data   = fetch_json_report(client)
    result = parse_identityiq_json(data)
    client.close()
    return result


def parse_from_json_file(path: str) -> dict:
    """
    Parse a locally saved JSON/JSONP file (for testing without login).
    """
    raw = open(path, encoding="utf-8").read().strip()
    if raw.startswith("JSON_CALLBACK("):
        raw = raw[len("JSON_CALLBACK("):]
        if raw.endswith(")"):
            raw = raw[:-1]
    data = json.loads(raw)
    return parse_identityiq_json(data)
