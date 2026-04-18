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

    # ── Step 3: SSN verification ─────────────────────────────────────────
    # IdentityIQ requires SSN last 4 as a second factor after password login
    if ssn_last4:
        ssn_endpoints = [
            "/api/ssn-verification",
            "/api/account/verify-ssn",
            "/api/verify",
            "/api/login/ssn",
            "/api/auth/ssn",
        ]
        ssn_payload = {"ssnLastFour": ssn_last4, "ssn": ssn_last4, "last4Ssn": ssn_last4}

        for endpoint in ssn_endpoints:
            print(f"[IIQ] Step 3: POST {endpoint} with SSN")
            ssn_resp = client.post(
                endpoint,
                json=ssn_payload,
                headers={**login_headers, "Content-Type": "application/json"},
            )
            print(f"[IIQ] SSN response {endpoint}: {ssn_resp.status_code} body={ssn_resp.text[:200]}")
            if ssn_resp.status_code in (200, 201):
                print(f"[IIQ] SSN verification succeeded at {endpoint}")
                break

    print(f"[IIQ] After login, cookies: {list(client.cookies.keys())}")
    
    # If login returned HTML, check if we landed on Dashboard (success)
    # or back on Login page (failure)
    if login_resp.status_code == 200 and "<html" in login_resp.text.lower():
        resp_text_lower = login_resp.text.lower()
        if "dashboard" in resp_text_lower or "credit score" in resp_text_lower:
            print("[IIQ] Login successful - dashboard HTML detected")
        elif "invalid" in resp_text_lower or "incorrect" in resp_text_lower:
            raise ValueError("Invalid username or password")
        else:
            # May be MFA or SSN verification step
            print(f"[IIQ] Login HTML response - checking for SSN step")
    
    # Verify we have a session (ASP.NET_SessionId should exist from step 1)
    print(f"[IIQ] Session cookies present: {list(client.cookies.keys())}")

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
    print(f"[IIQ] JSON report response: status={resp.status_code} len={len(resp.text)} url={resp.url}")
    print(f"[IIQ] JSON report first 500 chars: {resp.text[:500]}")
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


# ─────────────────────────────────────────────
# MAPPING CONSTANTS (derived from IdentityIQ HTML report ground truth)
# ─────────────────────────────────────────────

# AccountCondition.@abbreviation → display status
# (The JSON uses "Derog" but IdentityIQ's HTML shows "Derogatory")
_ACCOUNT_STATUS_MAP = {
    "Open":       "Open",
    "Closed":     "Closed",
    "Paid":       "Paid",
    "Derog":      "Derogatory",
    "Derogatory": "Derogatory",
}

# PayStatusHistory.@status char → payment_history value
# C = Current (OK)
# 1/2/3/4/5/6 = 30/60/90/120/150/180 days late
# 7/8/9 = collection/chargeoff (CO)
# U / N = unknown / no data (ND)
_PAY_HISTORY_CHAR_MAP = {
    "C": "OK",
    "0": "OK",
    "1": "30",
    "2": "60",
    "3": "90",
    "4": "120",
    "5": "150",
    "6": "180",
    "7": "CO",
    "8": "CO",
    "9": "CO",
    "U": "ND",
    "N": "ND",
    "-": "--",
}

_MONTH_ABBR = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def _iso_to_display(iso_date: str) -> str:
    """Convert '2024-02-25' → '02/25/2024'. Returns '' on bad input."""
    if not iso_date:
        return ""
    try:
        y, m, d = iso_date.split("-")
        return f"{m}/{d}/{y}"
    except Exception:
        return ""


def _concat_remarks(tl: dict) -> str:
    """
    Extract Comments by concatenating ALL Remark entries on the tradeline.

    Each Remark may have:
      - RemarkCode.@description  (the standard remark text)
      - @customRemark            (free-text, used for Consumer Statements like
                                  '**CONSUMER STATEMENT** ITEM IN DISPUTE BY CONSUMER')

    Returns concatenated string (matches IdentityIQ HTML which concatenates
    without separator). Returns '' if there are no remarks.
    """
    remarks = tl.get("Remark") or []
    if isinstance(remarks, dict):
        remarks = [remarks]
    if not isinstance(remarks, list):
        return ""

    parts = []
    for r in remarks:
        if not isinstance(r, dict):
            continue
        rc = r.get("RemarkCode") or {}
        desc = ""
        if isinstance(rc, dict):
            desc = _safe(rc.get("@description"))
        # Fall back to @customRemark when RemarkCode has no description.
        # This is how Consumer Statements arrive (Experian's
        # "**CONSUMER STATEMENT** ITEM IN DISPUTE BY CONSUMER").
        if not desc:
            desc = _safe(r.get("@customRemark"))
        if desc:
            parts.append(desc)
    return "".join(parts)


def _parse_credit_limit(gt: dict) -> str:
    """
    CreditLimit lives in GrantedTrade as a nested object: {"$": "850"}.
    Returns the numeric string ('850') or '' if absent.
    """
    if not isinstance(gt, dict):
        return ""
    cl = gt.get("CreditLimit")
    if isinstance(cl, dict):
        return _safe(cl.get("$"))
    if isinstance(cl, str):
        return _safe(cl)
    return ""


def _parse_account_type_detail(gt: dict) -> str:
    """
    'Account Type - Detail' (e.g. 'Credit Card', 'Auto Loan', 'Unsecured loan',
    'Installment sales contract') comes from GrantedTrade.AccountType.@description
    — NOT from GrantedTrade.CreditType (which gives us the top-level Account Type).
    """
    if not isinstance(gt, dict):
        return ""
    at = gt.get("AccountType")
    if isinstance(at, dict):
        return _safe(at.get("@description"))
    return ""


def _parse_bureau_code(t: dict) -> str:
    """
    Bureau Code = AccountDesignator.@description
    ('Individual', 'Joint', 'Authorized User', 'Co-Signer', etc.)
    Defaults to 'Individual' if absent.
    """
    ad = t.get("AccountDesignator")
    if isinstance(ad, dict):
        desc = _safe(ad.get("@description"))
        if desc:
            return desc
    return "Individual"


def _parse_pay_status_history(gt: dict) -> tuple[list[dict], list[str]]:
    """
    Parse the Two-Year (or longer) payment history grid.

    Returns (payment_history, late_payment_codes):

      payment_history: list of {"month": "Jan", "year": "2024", "value": "OK",
                                "date": "2024-01-07"} in chronological order
                      (oldest → newest).
                      IdentityIQ JSON gives MonthlyPayStatus newest-first,
                      we flip it so the UI can show oldest-first (matching
                      the PDF pipeline and the HTML Two-Year grid).

      late_payment_codes: list of "<CODE>:<Mon>/<YYYY>" strings
                          (e.g. ["30:Dec/2025", "60:Jan/2026", "CO:Mar/2026"])
                          — same format as original_parser.py so the existing
                          FCRA attack pipeline consumes it transparently.

    Contains the FULL history available in JSON (up to ~48 months observed),
    NOT just the first 24. Frontend paginates with a "View More" control.
    """
    if not isinstance(gt, dict):
        return [], []
    psh = gt.get("PayStatusHistory")
    if not isinstance(psh, dict):
        return [], []

    mps = psh.get("MonthlyPayStatus") or []
    if isinstance(mps, dict):
        mps = [mps]
    if not isinstance(mps, list) or not mps:
        return [], []

    # IdentityIQ returns newest-first; reverse to chronological order.
    entries_newest_first = [e for e in mps if isinstance(e, dict)]
    entries_chronological = list(reversed(entries_newest_first))

    payment_history: list[dict] = []
    late_codes: list[str] = []

    for entry in entries_chronological:
        date_iso = _safe(entry.get("@date"))
        status_char = _safe(entry.get("@status"))
        value = _PAY_HISTORY_CHAR_MAP.get(status_char, status_char or "--")

        month_abbr = ""
        year_str = ""
        if date_iso:
            try:
                y, m, _ = date_iso.split("-")
                year_str = y
                mi = int(m)
                if 1 <= mi <= 12:
                    month_abbr = _MONTH_ABBR[mi - 1]
            except Exception:
                pass

        payment_history.append({
            "month": month_abbr,
            "year":  year_str,
            "value": value,
            "date":  date_iso,
        })

        if value not in ("OK", "ND", "--", ""):
            if month_abbr and year_str:
                late_codes.append(f"{value}:{month_abbr}/{year_str}")

    return payment_history, late_codes


def _parse_tradelines(merge: dict) -> list[dict]:
    """
    Parse all TradeLinePartitions into raw account blocks.

    CRITICAL GROUPING RULE:
        One TradeLinePartition = ONE tradeline (one real-world account).
        Its child Tradeline entries are the per-bureau versions of that
        same account. IdentityIQ does the tri-merge correlation for us —
        we MUST respect the partition as the unit of grouping and MUST
        NOT re-group by @creditorName or @accountNumber, because bureaus
        report the same account with different creditor aliases
        (CREDITONEBNK / CRDTONEBNK / CREDIT ONE BANK NA) and different
        account-number truncations (406095**** / 13** / 406095XXXXXX****).

    All field mappings below are derived from the IdentityIQ HTML report
    ('tradeLinePartitionBasic' template) ground truth, which is what the
    consumer is legally entitled to see under the FCRA.
    """
    raw_accounts = []
    partitions = merge.get("TradeLinePartition", [])
    if isinstance(partitions, dict):
        partitions = [partitions]

    for pidx, partition in enumerate(partitions):
        # Top-level Account Type (Revolving / Installment / Open / Collection / Mortgage)
        # HTML uses @accountTypeAbbreviation ("Revolving"), NOT @accountTypeDescription.
        acct_type_abbr = _safe(partition.get("@accountTypeAbbreviation"))

        tradelines = partition.get("Tradeline", [])
        if isinstance(tradelines, dict):
            tradelines = [tradelines]
        if not isinstance(tradelines, list) or not tradelines:
            continue

        # One block per partition — deterministic block_id from partition index.
        # Keeping it stable across pulls is important for downstream dedup.
        block_key = f"partition:{pidx}"
        block_id  = "BLK-" + hashlib.md5(block_key.encode()).hexdigest()[:8].upper()

        bureau_data: dict[str, dict] = {}
        creditor_name = ""
        original_creditor = ""

        for t in tradelines:
            bureau = _bureau_from_tradeline(t)
            if not bureau:
                continue

            # Preserve the first non-empty creditor name for display
            # (bureaus vary; any of their aliases works as the row title).
            if not creditor_name:
                creditor_name = _safe(t.get("@creditorName"))

            gt = t.get("GrantedTrade", {}) or {}
            if isinstance(gt, list):
                gt = gt[0] if gt else {}
            collection = t.get("CollectionTrade", {}) or {}
            if isinstance(collection, list):
                collection = collection[0] if collection else {}

            # Late counts (still used for quick flags even though the authoritative
            # has_X_in_history values come from the parsed payment history below).
            late_30 = int(_safe(gt.get("@late30Count", "0")) or 0)
            late_60 = int(_safe(gt.get("@late60Count", "0")) or 0)
            late_90 = int(_safe(gt.get("@late90Count", "0")) or 0)

            # Original creditor (collections / debt buyers)
            orig_here = _safe(collection.get("@originalCreditor"))
            if orig_here and not original_creditor:
                original_creditor = orig_here

            # Account status: map "Derog" → "Derogatory" to match HTML display.
            cond_abbr = _safe((t.get("AccountCondition") or {}).get("@abbreviation"))
            account_status = _ACCOUNT_STATUS_MAP.get(cond_abbr, cond_abbr)

            # Account Type - Detail: GrantedTrade.AccountType.@description.
            # For Collection Accounts the partition itself carries "Collection".
            acct_type_detail = _parse_account_type_detail(gt)
            if not acct_type_detail and acct_type_abbr == "Collection":
                acct_type_detail = "Collection"

            # Comments: concatenation of ALL Remarks (including @customRemark,
            # which is how Experian's Consumer Statement arrives).
            comments = _concat_remarks(t)

            # Full payment history + late code list (feeds the FCRA engine
            # and the frontend Two-Year grid + View More control).
            payment_history, late_codes = _parse_pay_status_history(gt)

            # Authoritative has_X_in_history flags derived from the parsed grid.
            has_30 = any(c.startswith("30:") for c in late_codes) or late_30 > 0
            has_60 = any(c.startswith("60:") for c in late_codes) or late_60 > 0
            has_90 = any(c.startswith("90:") for c in late_codes) or late_90 > 0
            has_co = any(c.startswith("CO:") for c in late_codes)

            bureau_data[bureau] = {
                "account_number":    _safe(t.get("@accountNumber")),
                "status":            account_status,
                "payment_status":    _parse_pay_status(t),  # Tradeline.PayStatus.@description
                "balance":           _safe(t.get("@currentBalance")),
                "high_credit":       _safe(t.get("@highBalance")),
                "credit_limit":      _parse_credit_limit(gt),
                "monthly_payment":   _safe(gt.get("@monthlyPayment")),
                "past_due":          _safe(gt.get("@amountPastDue")),
                "date_opened":       _safe(t.get("@dateOpened")),
                # Date Last Active = @dateAccountStatus (NOT @dateReported).
                # The HTML and PDF both show dateAccountStatus here; dateReported
                # was the bug that made us show the wrong date for closed accounts.
                "date_last_active":  _safe(t.get("@dateAccountStatus")),
                "date_of_last_payment": _safe(gt.get("@dateLastPayment")),
                "last_reported":     _safe(t.get("@dateReported")),
                "no_of_months":      _safe(gt.get("@termMonths")),
                "account_type":      acct_type_abbr,
                "account_type_detail": acct_type_detail,
                "bureau_code":       _parse_bureau_code(t),
                "comments":          comments,
                "late_30":           late_30,
                "late_60":           late_60,
                "late_90":           late_90,
                "original_creditor": orig_here,
                "open_closed":       _parse_open_closed(t),
                # New fields — full history and late codes for FCRA + UI
                "payment_history":   payment_history,
                "late_payment_codes": late_codes,
                "has_30_in_history": has_30,
                "has_60_in_history": has_60,
                "has_90_in_history": has_90,
                "has_co_in_history": has_co,
            }

        if not bureau_data:
            continue

        # Display name with original creditor suffix (matches HTML/PDF convention)
        display_name = creditor_name
        if original_creditor:
            display_name = f"{creditor_name} (Original Creditor: {original_creditor})"

        raw_accounts.append({
            "block_id":      block_id,
            "name":          display_name,
            "bureau_data":   bureau_data,
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
    """Extract credit scores per bureau from VantageScore bundle components.

    The JSON bundle always contains three VantageScore components in order:
        TUCVantageScoreV6  → transunion
        EQFVantageScoreV6  → equifax
        EXPVantageScoreV6  → experian

    We read the bureau directly from the component Type string instead of
    relying on positional ordering, which is more robust and avoids the
    previous bug where experian was missing from bureau_order and ended up
    receiving the wrong fallback score.
    """
    scores = {"transunion": None, "experian": None, "equifax": None}
    components = data.get("BundleComponents", {}).get("BundleComponent", [])

    # Map component Type prefix → internal bureau key
    _COMP_BUREAU = {
        "TUC": "transunion",
        "EQF": "equifax",
        "EXP": "experian",
    }

    for comp in components:
        comp_type = comp.get("Type", {}).get("$", "")
        if "VantageScore" not in comp_type:
            continue
        # comp_type looks like "TUCVantageScoreV6", "EQFVantageScoreV6", etc.
        bureau = None
        for prefix, key in _COMP_BUREAU.items():
            if comp_type.startswith(prefix):
                bureau = key
                break
        if bureau is None:
            continue
        score_obj = comp.get("CreditScoreType", {})
        val = _safe(score_obj.get("@riskScore"))
        if val and val.isdigit():
            scores[bureau] = int(val)

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

            # has_X_in_history: prefer the authoritative flags computed from the
            # parsed payment history; fall back to late counts for legacy safety.
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
                # Full payment history (every month available in JSON, not just
                # the first 24) — frontend shows 24 by default + "View More".
                "payment_history":      data.get("payment_history", []),
                "late_payment_codes":   data.get("late_payment_codes", []),
                "has_30_in_history":    data.get("has_30_in_history", late_30 > 0),
                "has_60_in_history":    data.get("has_60_in_history", late_60 > 0),
                "has_90_in_history":    data.get("has_90_in_history", late_90 > 0),
                "has_co_in_history":    data.get("has_co_in_history", False),
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
    # Late X Days pattern (e.g. "Late 120 Days", "Late 30 Days")
    if "late" in payment and any(str(n) in payment for n in [30,60,90,120,150,180]):
        return True
    if acc.get("has_30_in_history") or acc.get("has_60_in_history") or acc.get("has_90_in_history"):
        return True
    # Known debt buyers
    debt_buyers = ["lvnv", "midland", "portfolio", "cavalry", "resurgent",
                   "aldous", "jefferson", "asset acceptance"]
    if any(k in name for k in debt_buyers):
        return True
    return False


def _negative_type(acc: dict) -> str:
    status  = acc.get("status", "").lower()
    payment = acc.get("payment_status", "").lower()
    name    = acc.get("name", "").lower()

    debt_buyers = ["lvnv", "midland", "portfolio", "cavalry", "resurgent",
                   "aldous", "jefferson", "asset acceptance"]

    if any(k in name for k in debt_buyers):
        return "collection"
    if "collection" in payment or "chargeoff" in payment:
        return "collection"
    if "late" in payment and any(str(n) in payment for n in [30,60,90,120,150,180]):
        return "late_payment"
    if acc.get("has_30_in_history") or acc.get("has_60_in_history") or acc.get("has_90_in_history"):
        return "late_payment"
    if "derogatory" in status:
        return "charge_off"
    return "derogatory"


def parse_identityiq_json(data: dict) -> dict:
    """
    Main parser. Takes the parsed JSON dict and returns the same
    format as build_report() in original_parser.py.
    
    Runs the full FCRA attack detection pipeline from original_parser.py
    so that the JSON flow produces identical results to the PDF flow.
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

    # Report date from JSON
    report_date = _safe(merge.get("@reportDate") or "")

    # Tradelines → raw accounts → inventory
    raw_accounts = _parse_tradelines(merge)
    inventory    = _build_inventory(raw_accounts)

    # Negatives — basic detection
    negatives_by_bureau: dict[str, list] = {b: [] for b in BUREAUS}
    for bureau, accts in inventory.items():
        for acc in accts:
            if _is_negative(acc):
                enriched = dict(acc)
                enriched["negative_type"] = _negative_type(acc)
                negatives_by_bureau[bureau].append(enriched)

    # ── Run full FCRA attack pipeline from original_parser ────────────────
    try:
        from original_parser import (
            build_dofd_engine,
            build_legal_detection_engine,
            build_legal_detection_summary,
            build_attack_scoring_engine,
            build_strategy_engine,
            build_letter_input_engine,
            build_dispute_letter_engine,
            build_furnisher_letter_engine,
            build_base_tradeline_engine as _orig_base,
            detect_inquiry_attacks,
            build_inquiry_letters,
        )

        # DOFD enrichment
        negatives_by_bureau = build_dofd_engine(negatives_by_bureau, report_date)

        # Build a minimal base_tradeline_engine compatible structure
        base_tradeline_engine = []
        for acct in raw_accounts:
            bureau_entries = {}
            for bureau, bd in acct["bureau_data"].items():
                bureau_entries[bureau] = {
                    "account_number":        bd["account_number"],
                    "masked_account_number": bd["account_number"].replace("*", "X"),
                    "status":                bd["status"],
                    "payment_status":        bd["payment_status"],
                    "balance":               bd["balance"],
                    "past_due":              bd["past_due"],
                    "comments":              bd["comments"],
                }
            base_tradeline_engine.append({
                "base_tradeline_id": acct["block_id"],
                "furnisher_name":    acct["name"],
                "bureau_entries":    bureau_entries,
                "raw_lines":         [],
            })

        # Legal detection
        legal_detection_engine = build_legal_detection_engine(
            negatives_by_bureau,
            base_tradeline_engine,
            report_date=report_date,
            client_state="",
        )
        legal_detection_summary = build_legal_detection_summary(
            negatives_by_bureau,
            legal_detection_engine,
        )
        attack_scoring_engine = build_attack_scoring_engine(legal_detection_engine)
        strategy_engine       = build_strategy_engine(attack_scoring_engine)
        letter_input_engine   = build_letter_input_engine(strategy_engine, negatives_by_bureau)
        dispute_letters       = build_dispute_letter_engine(
            letter_input_engine,
            consumer_name="[CLIENT NAME]",
            report_date=report_date,
            personal_info=personal_info,
            personal_info_issues=[],
        )
        furnisher_letters = build_furnisher_letter_engine(
            letter_input_engine,
            consumer_name="[CLIENT NAME]",
            report_date=report_date,
        )

        # Inquiries
        inquiries       = _parse_inquiries(merge)
        inquiry_attacks = detect_inquiry_attacks(inquiries)
        inquiry_letters = build_inquiry_letters(inquiries,
                            consumer_name="[CLIENT NAME]",
                            report_date=report_date)

        # Collect all attacks
        attacks = []
        for bureau_attacks in legal_detection_engine.values():
            attacks.extend(bureau_attacks)

        attack_count = len(attacks)

        print(f"[IIQ Parser] Full pipeline complete: {attack_count} attacks, "
              f"{sum(len(v) for v in negatives_by_bureau.values())} negatives")

    except Exception as e:
        import traceback
        print(f"[IIQ Parser] Full pipeline failed, using basic detection: {e}")
        print(traceback.format_exc())
        inquiries             = _parse_inquiries(merge)
        inquiry_attacks       = []
        inquiry_letters       = {}
        letter_input_engine   = {b: {} for b in BUREAUS}
        dispute_letters       = {}
        furnisher_letters     = {}
        attack_count          = sum(len(v) for v in negatives_by_bureau.values())
        attacks               = []
        base_tradeline_engine = []

    return {
        "source":               "identityiq_json",
        "scores":               scores,
        "personal_info":        personal_info,
        "personal_info_issues": [],
        "inventory_by_bureau":  inventory,
        "negatives_by_bureau":  negatives_by_bureau,
        "inquiries":            inquiries,
        "inquiry_attacks":      inquiry_attacks,
        "inquiry_letters":      inquiry_letters,
        "attack_count":         attack_count,
        "attacks":              attacks,
        "letter_input_engine":  letter_input_engine,
        "dispute_letters":      dispute_letters,
        "furnisher_letters":    furnisher_letters,
        "report_date":          report_date,
        "raw_accounts_count":   len(raw_accounts),
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
