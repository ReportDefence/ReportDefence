import re
import json
import hashlib
import pdfplumber
from pathlib import Path
from typing import Any

BUREAUS = ["transunion", "experian", "equifax"]



# =========================
# PDF EXTRACTION
# =========================

def extract_text_from_pdf(pdf_path: str) -> str:
    text_parts = []

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                text_parts.append(text)

    return "\n".join(text_parts)


def normalize_text(text: str) -> str:
    text = text.replace("\r", "\n")
    text = re.sub(r"\n{2,}", "\n\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    return text.strip()


def split_lines(text: str) -> list[str]:
    return [line.strip() for line in text.split("\n") if line.strip()]


# =========================
# HELPERS
# =========================

def extract_value(line: str, label: str) -> str:
    return line.split(label, 1)[-1].strip()


def normalize_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def safe_lower(value: str) -> str:
    return (value or "").lower().strip()


def clean_balance(value: str) -> str:
    if not value:
        return ""
    value = re.sub(r"[^\d\.]", "", value)
    if value == "":
        return ""
    try:
        return str(float(value))
    except Exception:
        return value.strip()


def clean_name_key(name: str) -> str:
    name = safe_lower(name)
    name = re.sub(r"[^a-z0-9 ]", " ", name)
    return normalize_spaces(name)


def mask_stars_to_x(value: str) -> str:
    return value.replace("*", "X")


def looks_like_header_or_noise(line: str) -> bool:
    low = line.lower().strip()

    bad_exact = {
        "",
        "transunion",
        "experian",
        "equifax",
        "transunion experian equifax",
        "account history back to top",
        "two-year payment history legend",
        "risk factors",
        "personal information",
        "credit score back to top",
        "show less",
        "purchase report",
    }

    if low in bad_exact:
        return True

    bad_contains = [
        "identityiq",
        "credit report",
        "https://",
        "account #:",
        "account type:",
        "account type - detail:",
        "bureau code:",
        "account status:",
        "payment status:",
        "balance:",
        "past due:",
        "comments:",
        "date opened:",
        "date of last payment:",
        "date last active:",
        "last reported:",
        "credit score:",
        "lender rank:",
        "score scale:",
        "quick links:",
        "reference #:",
        "report date:",
        "credit report date:",
        "month ",
        "year ",
    ]

    return any(x in low for x in bad_contains)


def is_possible_creditor_name(line: str) -> bool:
    if looks_like_header_or_noise(line):
        return False

    stripped = line.strip()

    if len(stripped) < 3:
        return False

    if re.search(r"\b[A-Z]{2}\b", stripped) and re.search(r"\d{5}", stripped):
        return False

    return bool(re.fullmatch(r"[A-Za-z0-9/&\-\.\,\(\)\'\: ]{3,}", stripped))


def clean_comment_text(text: str) -> str:
    text = normalize_spaces(text)
    text = text.strip(" -")
    return text


def split_multi_values(value: str) -> list[str]:
    """
    Split a multi-bureau field value into per-bureau tokens (max 3).

    The PDF lays out bureau columns side by side on the same line, so a
    field like "Account #:" gives us e.g.:
        "1202411721**** 1202411721**** 1202411721****"
    which must become ["1202411721****", "1202411721****", "1202411721****"]

    KEY RULES (in priority order):
    1. Account numbers:  digits/letters/- followed immediately by *+ or X+
       are ONE atomic token  →  "426937203396****"  is a single token.
    2. Payment status multi-word values:  "Late 120 Days", "Collection/Chargeoff"
       are ONE atomic token.
    3. Dollar amounts, dates → atomic.
    4. Single words (Open, Closed, Derogatory …) → atomic.
    5. Cap at 3 tokens (one per bureau).
    """
    import re as _re
    value = normalize_spaces(value)
    if not value:
        return []

    # Ordered patterns — tried LEFT to RIGHT, GREEDY
    atomic_patterns = [
        # account-number-style: alphanumeric prefix + mask suffix (****  XXXX  X***  etc.)
        r"[A-Za-z0-9\-/]+[X\*]{2,}",
        # plain masked segment that starts with * or X  (e.g. "83**", "XXXXXX****")
        r"[X\*]{2,}[A-Za-z0-9\-]*",
        # payment statuses (multi-word, must come before single-word fallbacks)
        r"Collection/Chargeoff",
        r"Late\s+\d+\s+Days?",
        # dollar amounts   $1,234.00
        r"\$[\d,]+\.?\d*",
        # dates  MM/DD/YYYY  or  MM/YYYY
        r"\d{1,2}/\d{2}/\d{4}",
        r"\d{1,2}/\d{4}",
        # pure numeric
        r"\d+",
        # known single-word status values
        r"Collection/Chargeoff",
        r"Current",
        r"Closed",
        r"Open",
        r"Derogatory",
        r"Paid",
        r"Refinanced",
        r"Terminated",
        r"Unknown",
        r"Unfavorable",
        r"Fair",
        r"Good",
        r"Excellent",
        r"Individual",
        r"Joint",
        r"Authorized\s+User",
        r"Installment",
        r"Revolving",
        # alphanumeric token without mask (e.g.  "LAI0223****" already caught above;
        # this catches plain strings like "LAI0223" if no mask present)
        r"[A-Za-z0-9\-/\.]+",
    ]

    combined = "(?:" + "|".join(atomic_patterns) + ")"
    tokens = _re.findall(combined, value, flags=_re.IGNORECASE)
    tokens = [normalize_spaces(t) for t in tokens if t.strip()]

    # Cap at 3 (one per bureau max)
    return tokens[:3] if tokens else [value]


def join_continuation_lines(lines: list[str], start_index: int, max_scan: int = 8) -> tuple[str, int]:
    collected = []
    idx = start_index
    scanned = 0

    stop_markers = [
        "Account #:",
        "Account Type:",
        "Account Type - Detail:",
        "Bureau Code:",
        "Account Status:",
        "Monthly Payment:",
        "Date Opened:",
        "Balance:",
        "No. of Months (terms):",
        "High Credit:",
        "Credit Limit:",
        "Past Due:",
        "Payment Status:",
        "Last Reported:",
        "Comments:",
        "Date Last Active:",
        "Date of Last Payment:",
        "Two-Year payment history",
    ]

    while idx < len(lines) and scanned < max_scan:
        current = lines[idx].strip()

        if any(current.startswith(marker) for marker in stop_markers):
            break

        if looks_like_header_or_noise(current):
            break

        collected.append(current)
        idx += 1
        scanned += 1

    return clean_comment_text(" ".join(collected)), idx


# =========================
# ACCOUNT NAME DETECTION
# =========================

def find_account_name(lines: list[str], idx: int) -> str:
    candidates = []

    for back in range(1, 13):
        pos = idx - back
        if pos < 0:
            break

        line = lines[pos].strip()

        if is_possible_creditor_name(line):
            candidates.append(line)

    if not candidates:
        return ""

    return candidates[0]


# =========================
# RAW ACCOUNT BLOCK PARSING
# =========================

def parse_raw_account_blocks(lines: list[str]) -> list[dict[str, Any]]:
    raw_accounts = []
    i = 0

    while i < len(lines):
        line = lines[i]

        if "Account #:" in line:
            name = find_account_name(lines, i)

            block = {
                "block_id": "",
                "name": name,
                "account_number_raw": "",
                "status_raw": "",
                "account_type_raw": "",
                "account_type_detail_raw": "",
                "bureau_code_raw": "",
                "monthly_payment_raw": "",
                "no_of_months_raw": "",
                "high_credit_raw": "",
                "credit_limit_raw": "",
                "payment_raw": "",
                "balance_raw": "",
                "past_due_raw": "",
                "comments_raw": "",
                "date_opened_raw": "",
                "date_last_active_raw": "",
                "date_of_last_payment_raw": "",
                "last_reported_raw": "",
                "raw_lines": []
            }

            j = i

            while j < len(lines):
                current = lines[j].strip()
                block["raw_lines"].append(current)

                if current.startswith("Account #:"):
                    block["account_number_raw"] = extract_value(current, "Account #:")

                elif current.startswith("Account Status:"):
                    block["status_raw"] = extract_value(current, "Account Status:")

                elif current.startswith("Account Type - Detail:"):
                    block["account_type_detail_raw"] = extract_value(current, "Account Type - Detail:")

                elif current.startswith("Account Type:"):
                    block["account_type_raw"] = extract_value(current, "Account Type:")

                elif current.startswith("Bureau Code:"):
                    block["bureau_code_raw"] = extract_value(current, "Bureau Code:")

                elif current.startswith("Monthly Payment:"):
                    block["monthly_payment_raw"] = extract_value(current, "Monthly Payment:")

                elif current.startswith("No. of Months (terms):"):
                    block["no_of_months_raw"] = extract_value(current, "No. of Months (terms):")

                elif current.startswith("High Credit:"):
                    block["high_credit_raw"] = extract_value(current, "High Credit:")

                elif current.startswith("Credit Limit:"):
                    block["credit_limit_raw"] = extract_value(current, "Credit Limit:")

                elif current.startswith("Payment Status:"):
                    block["payment_raw"] = extract_value(current, "Payment Status:")

                elif current.startswith("Balance:"):
                    block["balance_raw"] = extract_value(current, "Balance:")

                elif current.startswith("Past Due:"):
                    block["past_due_raw"] = extract_value(current, "Past Due:")

                elif current.startswith("Date Opened:"):
                    block["date_opened_raw"] = extract_value(current, "Date Opened:")

                elif current.startswith("Date Last Active:"):
                    block["date_last_active_raw"] = extract_value(current, "Date Last Active:")

                elif current.startswith("Date of Last Payment:"):
                    block["date_of_last_payment_raw"] = extract_value(current, "Date of Last Payment:")

                elif current.startswith("Last Reported:"):
                    block["last_reported_raw"] = extract_value(current, "Last Reported:")

                elif current.startswith("Comments:"):
                    first_part = extract_value(current, "Comments:")
                    continuation, new_idx = join_continuation_lines(lines, j + 1)
                    block["comments_raw"] = clean_comment_text(
                        normalize_spaces(f"{first_part} {continuation}")
                    )
                    j = new_idx - 1

                if "Two-Year payment history" in current:
                    # Parse the payment history table that follows.
                    month_tokens = []
                    year_tokens  = []
                    ph_by_bureau = {}
                    k = j + 1
                    while k < len(lines) and k < j + 8:
                        cl = lines[k].strip()
                        if "Account #:" in cl or cl.startswith("http") or cl == "":
                            break
                        cl_lower = cl.lower()
                        if cl_lower.startswith("month"):
                            raw_m = cl[len("month"):].strip()
                            for group in raw_m.split():
                                for pos in range(0, len(group), 3):
                                    chunk = group[pos:pos + 3]
                                    if len(chunk) == 3 and chunk.isalpha():
                                        month_tokens.append(chunk)
                        elif cl_lower.startswith("year "):
                            year_tokens = [t for t in cl.split()
                                           if t != "Year" and t.isdigit()]
                        else:
                            for bureau_prefix in ("TransUnion", "Experian", "Equifax"):
                                if cl.startswith(bureau_prefix):
                                    rest = cl[len(bureau_prefix):].split()
                                    valid_vals = {"OK","CO","30","60","90","120","ND","--"}
                                    if rest and rest[0] in valid_vals:
                                        ph_by_bureau[bureau_prefix] = rest
                                    break
                        k += 1
                    # Build late payment summary per bureau
                    late_summary = {}
                    for bureau_name, vals in ph_by_bureau.items():
                        lates = []
                        for idx_v, val in enumerate(vals):
                            if val not in ("OK", "--", ""):
                                month = month_tokens[idx_v] if idx_v < len(month_tokens) else "?"
                                year  = year_tokens[idx_v]  if idx_v < len(year_tokens)  else "?"
                                lates.append(f"{val}:{month}/{year}")
                        if lates:
                            late_summary[bureau_name] = lates
                    block["late_payment_summary"] = late_summary
                    # Full structured history
                    ph_structured = {}
                    for bureau_name, vals in ph_by_bureau.items():
                        entries = []
                        for idx_v, val in enumerate(vals):
                            month = month_tokens[idx_v] if idx_v < len(month_tokens) else "?"
                            year  = year_tokens[idx_v]  if idx_v < len(year_tokens)  else "?"
                            entries.append({"month": month, "year": year, "value": val})
                        ph_structured[bureau_name] = entries
                    block["payment_history_structured"] = ph_structured
                    break

                j += 1

            raw_block_text = "\n".join(block["raw_lines"])
            block["block_id"] = hashlib.md5(raw_block_text.encode("utf-8")).hexdigest()[:12]

            raw_accounts.append(block)
            i = j

        i += 1

    return raw_accounts


# =========================
# EXPANSION BY BUREAU
# =========================

def expand_raw_account_to_bureaus(raw_acc: dict[str, Any]) -> list[dict[str, Any]]:
    numbers   = split_multi_values(raw_acc["account_number_raw"])
    statuses  = split_multi_values(raw_acc["status_raw"])
    payments  = split_multi_values(raw_acc["payment_raw"])
    balances  = split_multi_values(raw_acc["balance_raw"])
    past_dues = split_multi_values(raw_acc["past_due_raw"])

    # New fields — split per bureau
    bureau_codes    = split_multi_values(raw_acc.get("bureau_code_raw", ""))
    monthly_pays    = split_multi_values(raw_acc.get("monthly_payment_raw", ""))
    no_of_months    = split_multi_values(raw_acc.get("no_of_months_raw", ""))
    high_credits    = split_multi_values(raw_acc.get("high_credit_raw", ""))
    credit_limits   = split_multi_values(raw_acc.get("credit_limit_raw", ""))
    acct_type_raw   = raw_acc.get("account_type_raw", "")

    # Dates are shared across bureaus (the PDF collapses them per row)
    # We take the first value for each date field as the canonical one,
    # and store all values for cross-bureau comparison if needed.
    date_opened_raw      = raw_acc.get("date_opened_raw", "")
    date_last_active_raw = raw_acc.get("date_last_active_raw", "")
    dolp_raw             = raw_acc.get("date_of_last_payment_raw", "")
    last_reported_raw    = raw_acc.get("last_reported_raw", "")
    acct_type_detail     = raw_acc.get("account_type_detail_raw", "")

    # Split date fields per bureau (same logic as other fields)
    date_opened_vals      = split_multi_values(date_opened_raw)
    date_last_active_vals = split_multi_values(date_last_active_raw)
    dolp_vals             = split_multi_values(dolp_raw)
    last_reported_vals    = split_multi_values(last_reported_raw)

    comments = raw_acc["comments_raw"]

    max_len = max(
        len(numbers)   if numbers   else 0,
        len(statuses)  if statuses  else 0,
        len(payments)  if payments  else 0,
        len(balances)  if balances  else 0,
        len(past_dues) if past_dues else 0,
        1
    )
    max_len = min(max_len, 3)

    _bureau_display = ["TransUnion", "Experian", "Equifax"]
    late_summary      = raw_acc.get("late_payment_summary", {})
    ph_structured_raw = raw_acc.get("payment_history_structured", {})

    expanded = []
    for idx in range(max_len):
        display    = _bureau_display[idx] if idx < len(_bureau_display) else ""
        late_codes = late_summary.get(display, [])
        ph_entries = ph_structured_raw.get(display, [])
        expanded.append({
            "block_id":       raw_acc["block_id"],
            "bureau":         BUREAUS[idx],
            "name":           raw_acc["name"],
            "account_number": numbers[idx]   if idx < len(numbers)   else "",
            "status":         statuses[idx]  if idx < len(statuses)  else "",
            "payment_status": payments[idx]  if idx < len(payments)  else "",
            "balance":        balances[idx]  if idx < len(balances)  else "",
            "past_due":       past_dues[idx] if idx < len(past_dues) else "",
            "comments":       comments,
            "account_type":        acct_type_raw,
            "bureau_code":         bureau_codes[idx]  if idx < len(bureau_codes)  else "",
            "monthly_payment":     monthly_pays[idx]  if idx < len(monthly_pays)  else "",
            "no_of_months":        no_of_months[idx]  if idx < len(no_of_months)  else "",
            "high_credit":         high_credits[idx]  if idx < len(high_credits)  else "",
            "credit_limit":        credit_limits[idx] if idx < len(credit_limits) else "",
            "date_opened":       date_opened_vals[idx]      if idx < len(date_opened_vals)      else (date_opened_vals[0]      if date_opened_vals      else ""),
            "date_last_active":  date_last_active_vals[idx] if idx < len(date_last_active_vals) else (date_last_active_vals[0] if date_last_active_vals else ""),
            "date_of_last_payment": dolp_vals[idx]          if idx < len(dolp_vals)             else (dolp_vals[0]             if dolp_vals             else ""),
            "last_reported":     last_reported_vals[idx]    if idx < len(last_reported_vals)    else (last_reported_vals[0]    if last_reported_vals    else ""),
            "account_type_detail": acct_type_detail,
            "raw_lines":      raw_acc["raw_lines"],
            "late_payment_codes":   late_codes,
            "payment_history":      ph_entries,
            "has_30_in_history":    any(x.startswith("30:") for x in late_codes),
            "has_60_in_history":    any(x.startswith("60:") for x in late_codes),
            "has_90_in_history":    any(x.startswith("90:") for x in late_codes),
            "has_co_in_history":    any(x.startswith("CO:") for x in late_codes),
        })

    return expanded


def build_inventory_by_bureau(raw_accounts: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    inventory = {bureau: [] for bureau in BUREAUS}

    for raw_acc in raw_accounts:
        expanded = expand_raw_account_to_bureaus(raw_acc)

        for item in expanded:
            if item["account_number"]:
                inventory[item["bureau"]].append(item)

    return inventory


# =========================
# BASE TRADELINE ENGINE
# =========================

def build_base_tradeline_engine(raw_accounts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    base_tradelines: list[dict[str, Any]] = []

    for raw_acc in raw_accounts:
        expanded = expand_raw_account_to_bureaus(raw_acc)
        bureau_entries = {}

        for item in expanded:
            bureau_entries[item["bureau"]] = {
                "account_number": item.get("account_number", ""),
                "masked_account_number": mask_stars_to_x(item.get("account_number", "")),
                "status": item.get("status", ""),
                "payment_status": item.get("payment_status", ""),
                "balance": item.get("balance", ""),
                "past_due": item.get("past_due", ""),
                "comments": item.get("comments", ""),
            }

        base_tradelines.append({
            "base_tradeline_id": raw_acc["block_id"],
            "furnisher_name": raw_acc.get("name", ""),
            "bureau_entries": bureau_entries,
            "raw_lines": raw_acc.get("raw_lines", []),
        })

    return base_tradelines


def build_same_block_cross_bureau_summary(base_tradelines: list[dict[str, Any]]) -> list[dict[str, Any]]:
    summary = []

    for tradeline in base_tradelines:
        bureau_entries = tradeline.get("bureau_entries", {})
        present_bureaus = [b for b in BUREAUS if b in bureau_entries]
        account_numbers = {
            b: bureau_entries[b].get("account_number", "")
            for b in present_bureaus
        }

        unique_account_numbers = {
            v for v in account_numbers.values() if v
        }

        summary.append({
            "base_tradeline_id": tradeline.get("base_tradeline_id", ""),
            "furnisher_name": tradeline.get("furnisher_name", ""),
            "bureaus_present": present_bureaus,
            "account_numbers_by_bureau": account_numbers,
            "cross_bureau_number_variation": len(unique_account_numbers) > 1,
            "rule_note": (
                "Same base tradeline across bureaus. Variations in account number across bureaus are treated as normal bureau-specific representations, not automatic duplicate-reporting attacks."
            ),
        })

    return summary


# =========================
# NORMALIZATION / LEGAL PREP
# =========================

def clean_creditor_name(name: str) -> str:
    if not name:
        return ""

    name = normalize_spaces(name)

    parts = name.split()
    if len(parts) > 2 and len(parts) % 2 == 0:
        half = len(parts) // 2
        if parts[:half] == parts[half:]:
            name = " ".join(parts[:half])

    return name


def infer_missing_names(inventory: dict[str, list[dict[str, Any]]]) -> dict[str, list[dict[str, Any]]]:
    account_lookup: dict[str, str] = {}
    account_balance_lookup: dict[tuple[str, str], str] = {}

    for bureau_accounts in inventory.values():
        for acc in bureau_accounts:
            acct = normalize_spaces(acc.get("account_number", ""))
            bal = normalize_spaces(acc.get("balance", ""))
            name = normalize_spaces(acc.get("name", ""))

            if acct and name:
                account_lookup[acct] = name
                account_balance_lookup[(acct, bal)] = name

    for bureau_accounts in inventory.values():
        for acc in bureau_accounts:
            current_name = normalize_spaces(acc.get("name", ""))
            if current_name:
                continue

            acct = normalize_spaces(acc.get("account_number", ""))
            bal = normalize_spaces(acc.get("balance", ""))

            if (acct, bal) in account_balance_lookup:
                acc["name"] = account_balance_lookup[(acct, bal)]
            elif acct in account_lookup:
                acc["name"] = account_lookup[acct]

    return inventory


def clean_comments(acc: dict[str, Any]) -> dict[str, Any]:
    text = acc.get("comments", "")

    if not text:
        acc["comments"] = ""
        return acc

    text = normalize_spaces(text)

    garbage = [
        "consumer disputes after",
        "subscriber reports dispute",
        "customer disputed account",
        "account information",
        "reported by subscriber",
    ]

    for g in garbage:
        text = text.replace(g, "")

    text = normalize_spaces(text)

    parts = text.split(".")
    unique = []
    for p in parts:
        p = p.strip()
        if p and p not in unique:
            unique.append(p)

    text = ". ".join(unique)
    acc["comments"] = text
    return acc


def mark_possible_duplicate_groups(inventory: dict[str, list[dict[str, Any]]]) -> dict[str, list[dict[str, Any]]]:
    group_map: dict[str, list[str]] = {}

    for bureau_items in inventory.values():
        for item in bureau_items:
            acct = normalize_spaces(item.get("account_number", "")).lower()
            if acct:
                group_map.setdefault(acct, []).append(item["block_id"])

    account_to_group_id: dict[str, str] = {}
    counter = 1

    for acct, block_ids in group_map.items():
        unique_block_ids = sorted(set(block_ids))
        if len(unique_block_ids) > 1:
            account_to_group_id[acct] = f"DUP-{counter:03d}"
            counter += 1

    for bureau_items in inventory.values():
        for item in bureau_items:
            acct = normalize_spaces(item.get("account_number", "")).lower()
            item["possible_duplicate_group"] = account_to_group_id.get(acct, "")

    return inventory


def normalize_inventory_final(inventory: dict[str, list[dict[str, Any]]]) -> dict[str, list[dict[str, Any]]]:
    for bureau_accounts in inventory.values():
        for acc in bureau_accounts:
            acc["name"] = clean_creditor_name(acc.get("name", ""))
            clean_comments(acc)

    inventory = infer_missing_names(inventory)
    inventory = mark_possible_duplicate_groups(inventory)

    return inventory


# =========================
# DOFD ENGINE — §1681c / §605
# =========================

def parse_date_field(value: str) -> "datetime | None":
    """
    Parse a date string from a report field.
    Handles: MM/DD/YYYY, MM/YYYY, MM/YY, and dashes/empty.
    Returns the earliest date if multiple are present (first bureau value).
    """
    from datetime import datetime as _dt
    if not value or value.strip() in {"-", ""}:
        return None
    # Try first token only (first bureau value)
    token = normalize_spaces(value).split()[0].strip("-")
    for fmt in ("%m/%d/%Y", "%m/%Y", "%m/%y"):
        try:
            return _dt.strptime(token, fmt)
        except ValueError:
            continue
    return None


def estimate_dofd(acc: dict[str, Any]) -> "datetime | None":
    """
    Estimate the Date of First Delinquency (DOFD) for a negative account.

    Priority order (FCRA §1681c(c) logic):
    1. Date of Last Payment (DOLP) + 30 days → first missed payment
    2. Date Last Active — only if it predates Last Reported by 60+ days
       AND predates the collector's Date Opened (to reject collector-updated values)
    3. None — cannot estimate, flag as dofd_unknown

    Critical rule for collectors:
    When Date Last Active = Last Reported (±30 days), the collector is simply
    updating the "active" date to today to keep the tradeline alive. This is
    NOT the actual delinquency date. We must reject this value and flag the
    account for DOFD verification demand.
    """
    from datetime import timedelta

    dolp        = parse_date_field(acc.get("date_of_last_payment", ""))
    last_active = parse_date_field(acc.get("date_last_active", ""))
    date_opened = parse_date_field(acc.get("date_opened", ""))
    last_rep    = parse_date_field(acc.get("last_reported", ""))

    # Strategy 1: DOLP + 30 days = first month without payment (most reliable)
    if dolp:
        return dolp + timedelta(days=30)

    # Strategy 2: Date Last Active
    # Reject if it looks like the collector is refreshing the date:
    #   - DLA is within 60 days of Last Reported (collector is updating it today)
    #   - DLA is on/after the collector's Date Opened (collector's own open date)
    if last_active:
        is_collector_refresh = (
            last_rep and abs((last_active - last_rep).days) <= 60
        )
        is_after_collector_opened = (
            date_opened and last_active >= date_opened
        )

        if is_collector_refresh or is_after_collector_opened:
            # DLA is unreliable — collector is manipulating it
            # Return None so dofd_confidence = 'unknown'
            return None

        return last_active + timedelta(days=30)

    return None


def calculate_fcra_expiration(dofd: "datetime") -> "datetime":
    """
    Calculate the FCRA §1681c(c) expiration date.
    7-year clock starts 180 days after DOFD.
    """
    from datetime import timedelta
    seven_yr_start = dofd + timedelta(days=180)
    # Use relativedelta for exact year calculation
    try:
        from dateutil.relativedelta import relativedelta
        return seven_yr_start + relativedelta(years=7)
    except ImportError:
        return seven_yr_start + timedelta(days=7 * 365 + 2)  # approx


def build_dofd_engine(
    negatives_by_bureau: dict[str, list[dict[str, Any]]],
    report_date_str: str = "",
) -> dict[str, list[dict[str, Any]]]:
    """
    For every negative account, calculate:
    - dofd_estimated: best estimate of Date of First Delinquency
    - fcra_expiration: date the account must drop off the report
    - days_until_expiration: positive = still valid, negative = EXPIRED
    - is_obsolete: True if report_date >= fcra_expiration
    - dofd_confidence: 'high' (DOLP available) | 'medium' (DLA fallback) | 'unknown'
    - re_aging_flag: True if collector's Date Opened > DOFD estimate by > 90 days
      (potential §1681c violation — collector is using its own open date as DOFD)

    Returns a dict of bureau -> list of enriched negative accounts.
    """
    from datetime import datetime as _dt

    report_date = None
    if report_date_str:
        report_date = parse_date_field(report_date_str)
    if not report_date:
        report_date = _dt.today()

    result: dict[str, list[dict[str, Any]]] = {}

    for bureau, accounts in negatives_by_bureau.items():
        enriched_list = []

        for acc in accounts:
            enriched = dict(acc)

            dofd = estimate_dofd(acc)

            # Confidence level
            dolp        = parse_date_field(acc.get("date_of_last_payment", ""))
            last_active = parse_date_field(acc.get("date_last_active", ""))
            last_rep    = parse_date_field(acc.get("last_reported", ""))
            date_opened = parse_date_field(acc.get("date_opened", ""))

            if dolp:
                confidence = "high"
            elif last_active:
                # Was DLA rejected by estimate_dofd?
                is_refresh = last_rep and abs((last_active - last_rep).days) <= 60
                is_after_opened = date_opened and last_active >= date_opened
                if is_refresh or is_after_opened:
                    confidence = "unknown"
                else:
                    confidence = "medium"
            else:
                confidence = "unknown"

            # Detect suspected DLA refresh (collector keeping tradeline "active")
            dla_suspected_refresh = False
            if last_active and last_rep and abs((last_active - last_rep).days) <= 60:
                dla_suspected_refresh = True

            if dofd:
                expiration = calculate_fcra_expiration(dofd)
                days_left = (expiration - report_date).days
                is_obsolete = days_left < 0

                # Re-aging check: if collector opened AFTER DOFD + 90 days
                date_opened = parse_date_field(acc.get("date_opened", ""))
                re_aging_flag = False
                re_aging_gap_days = None
                if date_opened and date_opened > dofd:
                    gap = (date_opened - dofd).days
                    if gap > 90:
                        re_aging_flag = True
                        re_aging_gap_days = gap

                enriched.update({
                    "dofd_estimated":            dofd.strftime("%m/%Y"),
                    "dofd_confidence":           confidence,
                    "fcra_expiration":           expiration.strftime("%m/%Y"),
                    "days_until_expiration":     days_left,
                    "is_obsolete":               is_obsolete,
                    "re_aging_flag":             re_aging_flag,
                    "re_aging_gap_days":         re_aging_gap_days,
                    "dla_suspected_refresh":     dla_suspected_refresh,
                    "dofd_verification_required": confidence == "unknown",
                    "fcra_section":              "15 USC 1681c(a)(4) & 1681c(c)",
                })
            else:
                enriched.update({
                    "dofd_estimated":            None,
                    "dofd_confidence":           "unknown",
                    "fcra_expiration":           None,
                    "days_until_expiration":     None,
                    "is_obsolete":               False,
                    "re_aging_flag":             False,
                    "re_aging_gap_days":         None,
                    "dla_suspected_refresh":     dla_suspected_refresh,
                    "dofd_verification_required": True,
                    "fcra_section":              "15 USC 1681c(a)(4) & 1681c(c)",
                })

            enriched_list.append(enriched)

        result[bureau] = enriched_list

    return result


def detect_obsolete_account_attacks(
    bureau: str,
    accounts: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Detect accounts that have exceeded the FCRA 7-year reporting limit.
    These must be removed under §1681c(a)(4) regardless of any other dispute.
    """
    attacks = []

    for acc in accounts:
        if not acc.get("is_obsolete"):
            continue

        expiration = acc.get("fcra_expiration", "unknown")
        dofd       = acc.get("dofd_estimated", "unknown")
        confidence = acc.get("dofd_confidence", "unknown")
        days_over  = abs(acc.get("days_until_expiration", 0))

        attacks.append(
            build_attack_record(
                attack_type="obsolete_account_7yr_limit",
                bureau=bureau,
                accounts=[acc],
                strategy_tags=[
                    "FCRA_1681c_a_4",
                    "FCRA_1681c_c",
                    "7_year_limit",
                    "mandatory_deletion",
                ],
                reason=(
                    f"{acc.get('name', '')} account {acc.get('account_number', '')} "
                    f"has exceeded the FCRA 7-year reporting period. "
                    f"Estimated DOFD: {dofd} (confidence: {confidence}). "
                    f"FCRA expiration: {expiration}. "
                    f"This account is approximately {days_over} days past its legal reporting limit "
                    f"and must be deleted under 15 USC 1681c(a)(4)."
                ),
            )
        )

    return attacks


def detect_re_aging_attacks(
    bureau: str,
    accounts: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Detect potential re-aging: collector's Date Opened is significantly
    later than the estimated DOFD, suggesting the collector may be using
    its own open date to artificially extend the reporting period.
    This violates §1681c(c) and is a common FDCPA violation.
    """
    attacks = []

    for acc in accounts:
        if not acc.get("re_aging_flag"):
            continue

        gap      = acc.get("re_aging_gap_days", 0)
        dofd     = acc.get("dofd_estimated", "unknown")
        opened   = acc.get("date_opened", "unknown")
        expiry   = acc.get("fcra_expiration", "unknown")

        attacks.append(
            build_attack_record(
                attack_type="potential_re_aging",
                bureau=bureau,
                accounts=[acc],
                strategy_tags=[
                    "FCRA_1681c_c",
                    "FCRA_1681e_b",
                    "re_aging",
                    "dofd_manipulation",
                ],
                reason=(
                    f"{acc.get('name', '')} opened its collection account on {opened}, "
                    f"which is {gap} days after the estimated DOFD of {dofd}. "
                    f"This gap suggests the collector may be using its own open date "
                    f"to reset the 7-year clock, artificially extending the reporting period "
                    f"beyond the FCRA expiration of {expiry}. "
                    f"Under 15 USC 1681c(c), the 7-year period runs from the original DOFD, "
                    f"not from when the collector acquired the debt."
                ),
            )
        )

    return attacks



def is_negative(acc):
    status  = safe_lower(acc.get("status", ""))
    payment = safe_lower(acc.get("payment_status", ""))
    name    = safe_lower(acc.get("name", ""))
    raw     = " ".join(acc.get("raw_lines", [])).lower()

    # Original negatives
    if "derogatory" in status:
        return True
    if "collection" in payment or "chargeoff" in payment:
        return True
    if "late" in payment and "current" not in payment:
        return True

    # Paid collection — Status=Paid but was a collection/chargeoff
    if "paid" in status and (
        "collection" in raw or "chargeoff" in raw or "charged off" in raw
        or "profit and loss" in raw
    ):
        return True

    # Child support — §1681s-1 special account, always negative if past due
    acct_detail = safe_lower(acc.get("account_type_detail", ""))
    if any(k in acct_detail for k in ["child support", "family support", "spousal support"]):
        if any(k in payment for k in ["late", "collection", "chargeoff", "past due"]):
            return True
        past_due_val = acc.get("past_due", "").replace("$","").replace(",","").strip()
        if past_due_val and past_due_val not in ("0", "0.00", "-", ""):
            return True

    # Repossession — voluntary or involuntary, including "taken back" language
    if any(k in raw for k in [
        "repossess", "voluntary surrender", "involuntary repo", "surrender",
        "merchandise was taken back", "taken back by credit grantor",
        "vehicle was repossessed", "collateral was repossessed"
    ]):
        return True

    # Bankruptcy — included-in-BK accounts still reporting
    if any(k in raw for k in ["included in bankruptcy", "included in bk", "discharged in bankruptcy"]):
        return True

    # Student loan — only if derogatory indicators present
    if any(k in name for k in ["dept of ed", "navient", "sallie mae", "mohela", "nelnet",
                                 "fedloan", "great lakes", "aidvantage"]):
        if any(k in raw for k in ["default", "derogatory", "collection", "chargeoff",
                                    "late", "past due", "deferment violation"]):
            return True

    # Charge-off with deficiency balance — account shows balance after chargeoff
    if ("chargeoff" in payment or "charged off" in raw or "profit and loss" in raw):
        return True

    # Late payment detected in two-year payment history table
    if acc.get("has_30_in_history") or acc.get("has_60_in_history") or acc.get("has_90_in_history"):
        return True

    return False


def normalize_negative_type(acc: dict[str, Any]) -> str | None:
    status   = safe_lower(acc.get("status", ""))
    payment  = safe_lower(acc.get("payment_status", ""))
    comments = safe_lower(acc.get("comments", ""))
    name     = safe_lower(acc.get("name", ""))
    raw      = " ".join(acc.get("raw_lines", [])).lower()
    balance  = acc.get("balance", "")
    past_due = acc.get("past_due", "")

    # ── 0. Child support — §1681s-1 special rules ────────────────────────
    acct_detail = safe_lower(acc.get("account_type_detail", ""))
    if any(k in acct_detail for k in ["child support", "family support", "spousal support"]):
        return "child_support"

    # ── 1. Repossession ──────────────────────────────────────────────────
    if any(k in raw for k in [
        "repossess", "voluntary surrender", "involuntary repo",
        "merchandise was taken back", "taken back by credit grantor",
        "vehicle was repossessed", "collateral was repossessed"
    ]):
        return "repossession"

    # ── 2. Bankruptcy included-in-BK ────────────────────────────────────
    if any(k in raw for k in [
        "included in bankruptcy", "included in bk", "discharged in bankruptcy",
        "chapter 7", "chapter 13", "bankruptcy"
    ]):
        return "bankruptcy"

    # ── 3. Student loan derogatory ───────────────────────────────────────
    student_servicers = [
        "dept of ed", "department of education", "navient", "sallie mae",
        "mohela", "nelnet", "fedloan", "great lakes", "aidvantage",
        "granite state", "edfinancial"
    ]
    if any(k in name for k in student_servicers):
        if any(k in raw for k in ["default", "derogatory", "collection", "late", "past due"]):
            return "student_loan"

    # ── 4. Known debt buyer — always collection regardless of payment status ──
    debt_buyers = [
        "lvnv", "portfolio", "cavalry", "midland", "resurgent",
        "jefferson capital", "asset acceptance", "national collegiate",
        "portfolio rc", "portfolio recovery"
    ]
    if any(k in name for k in debt_buyers):
        return "collection"

    # ── 5. Paid collection / settled — BEFORE charge_off check ──────────
    if "paid" in status and (
        "collection" in raw or "chargeoff" in raw or "charged off" in raw
        or "profit and loss" in raw or "collection" in payment
    ):
        return "paid_collection"

    if any(k in raw for k in ["settled", "settled for less", "partial payment"]):
        if "collection" in raw or "chargeoff" in raw or "derogatory" in status:
            return "paid_collection"

    # ── 6. Collection (standard) ─────────────────────────────────────────
    if "collection" in payment or "collection" in raw or "collection" in comments:
        return "collection"

    # ── 7. Charge-off with deficiency balance ────────────────────────────
    # Only flag as deficiency if it's an auto loan or similar installment
    # with a non-zero balance — credit card charge-offs are plain charge_off
    has_chargeoff = (
        "chargeoff" in payment or "collection/chargeoff" in payment
        or "charged off" in raw or "profit and loss" in raw
        or "charge off" in payment
    )
    if has_chargeoff:
        balance_clean = balance.replace("$", "").replace(",", "").replace(".00", "").strip()
        acct_type = acc.get("account_type_detail", "").lower() if hasattr(acc, "get") else ""
        is_auto_or_loan = any(k in acct_type for k in ["auto", "mortgage", "loan"])
        if balance_clean and balance_clean not in ("0", "-", "") and is_auto_or_loan:
            return "charge_off_deficiency"
        return "charge_off"

    # ── 8. Late payment ──────────────────────────────────────────────────
    if "late" in payment:
        return "late_payment"
    # Late payment detected in two-year payment history table
    if acc.get("has_30_in_history") or acc.get("has_60_in_history") or acc.get("has_90_in_history"):
        return "late_payment"

    # ── 9. Generic derogatory ────────────────────────────────────────────
    if "derogatory" in status:
        return "derogatory"

    return None


def build_negative_inventory_by_bureau(inventory: dict[str, list[dict[str, Any]]]) -> dict[str, list[dict[str, Any]]]:
    negatives = {bureau: [] for bureau in BUREAUS}

    for bureau, items in inventory.items():
        for item in items:
            if not is_negative(item):
                continue

            negative_type = normalize_negative_type(item)

            if negative_type:
                enriched = dict(item)
                enriched["negative_type"] = negative_type
                negatives[bureau].append(enriched)

    return negatives


# =========================
# LEGAL DETECTION ENGINE
# =========================

def build_attack_record(
    attack_type: str,
    bureau: str,
    accounts: list[dict[str, Any]],
    strategy_tags: list[str],
    reason: str,
) -> dict[str, Any]:
    return {
        "attack_type": attack_type,
        "bureau": bureau,
        "reason": reason,
        "strategy_tags": strategy_tags,
        "accounts": accounts,
    }


def detect_duplicate_account_number_attacks(bureau: str, accounts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = {}

    for acc in accounts:
        acct = normalize_spaces(acc.get("account_number", ""))
        if not acct:
            continue
        groups.setdefault(acct, []).append(acc)

    attacks = []

    for acct, items in groups.items():
        block_ids = {item.get("block_id", "") for item in items}
        if len(block_ids) > 1:
            attacks.append(
                build_attack_record(
                    attack_type="duplicate_account_number",
                    bureau=bureau,
                    accounts=items,
                    strategy_tags=[
                        "FCRA_1681e_b",
                        "FCRA_1681i",
                        "duplicate_reporting",
                    ],
                    reason=f"Same account number appears in multiple separate blocks: {acct}",
                )
            )

    return attacks


def detect_multi_furnisher_same_balance_attacks(bureau: str, accounts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = {}

    for acc in accounts:
        balance_key = clean_balance(acc.get("balance", ""))
        if not balance_key or balance_key in {"0", "0.0"}:
            continue
        groups.setdefault(balance_key, []).append(acc)

    attacks = []

    for balance_key, items in groups.items():
        block_ids = {item.get("block_id", "") for item in items}
        if len(items) > 1 and len(block_ids) > 1:
            attacks.append(
                build_attack_record(
                    attack_type="multi_furnisher_same_balance",
                    bureau=bureau,
                    accounts=items,
                    strategy_tags=[
                        "FCRA_1681e_b",
                        "FCRA_1681i",
                        "FCRA_1681s_2_b",
                        "chain_of_title_demand",
                    ],
                    reason=f"Multiple separate negative tradelines report the same balance: {balance_key}",
                )
            )

    return attacks


def detect_collector_original_creditor_pattern_attacks(bureau: str, accounts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = {}

    for acc in accounts:
        balance_key = clean_balance(acc.get("balance", ""))
        if not balance_key or balance_key in {"0", "0.0"}:
            continue
        groups.setdefault(balance_key, []).append(acc)

    attacks = []

    for balance_key, items in groups.items():
        block_ids = {item.get("block_id", "") for item in items}
        has_original_creditor_style = any("(original creditor:" in safe_lower(item.get("name", "")) for item in items)
        has_collector_style = any(
            any(marker in safe_lower(item.get("name", "")) for marker in ["portfolio", "lvnv", "cavalry", "midland", "resurgent"])
            for item in items
        )

        if has_original_creditor_style and has_collector_style and len(items) > 1 and len(block_ids) > 1:
            attacks.append(
                build_attack_record(
                    attack_type="collector_original_creditor_pattern",
                    bureau=bureau,
                    accounts=items,
                    strategy_tags=[
                        "FCRA_1681e_b",
                        "FCRA_1681i",
                        "FCRA_1681s_2_b",
                        "chain_of_title_demand",
                        "collector_authority_challenge",
                    ],
                    reason=f"Collector/original-creditor reporting pattern detected around balance: {balance_key}",
                )
            )

    return attacks


def detect_dofd_unknown_attacks(
    bureau: str,
    accounts: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Flag negative accounts where DOFD cannot be determined.
    These accounts require the bureau/furnisher to disclose the DOFD
    so the consumer can verify the 7-year reporting period.

    Special case: if Date Last Active matches Last Reported (suspected refresh),
    flag as potential re-aging even if not yet provably so.
    """
    attacks = []

    for acc in accounts:
        if not acc.get("dofd_verification_required"):
            continue
        # Skip if already flagged as re-aging (separate attack)
        if acc.get("re_aging_flag"):
            continue

        dla_refresh = acc.get("dla_suspected_refresh", False)

        if dla_refresh:
            reason = (
                f"{acc.get('name', '')} account {acc.get('account_number', '')} "
                f"shows a Date Last Active that matches its Last Reported date, "
                f"suggesting the collector is refreshing this date to keep the tradeline "
                f"appearing current. The actual Date of First Delinquency (DOFD) cannot "
                f"be determined from the data reported. Under 15 USC 1681c(c), the 7-year "
                f"reporting period runs from the DOFD — the furnisher must disclose the "
                f"DOFD and provide documentation of the original delinquency date."
            )
            tags = [
                "FCRA_1681c_c",
                "FCRA_1681e_b",
                "FCRA_1681i_a",
                "dofd_disclosure_demand",
                "suspected_date_refresh",
            ]
        else:
            reason = (
                f"{acc.get('name', '')} account {acc.get('account_number', '')} "
                f"does not provide sufficient date information to verify the "
                f"Date of First Delinquency (DOFD) or the FCRA 7-year reporting period. "
                f"Under 15 USC 1681c(c), the bureau must be able to verify the DOFD. "
                f"If the furnisher cannot provide this information, the account "
                f"cannot be verified and must be deleted under 15 USC 1681i(a)(5)."
            )
            tags = [
                "FCRA_1681c_c",
                "FCRA_1681i_a_5",
                "FCRA_1681e_b",
                "dofd_disclosure_demand",
                "not_verifiable",
            ]

        attacks.append(
            build_attack_record(
                attack_type="dofd_unknown_verification_required",
                bureau=bureau,
                accounts=[acc],
                strategy_tags=tags,
                reason=reason,
            )
        )

    return attacks


def detect_child_support_attacks(bureau: str, accounts: list[dict]) -> list[dict]:
    attacks = []
    for acc in accounts:
        if acc.get("negative_type") != "child_support":
            continue
        balance  = acc.get("balance", "")
        past_due = acc.get("past_due", "")
        payment  = acc.get("payment_status", "")
        attacks.append(build_attack_record(
            attack_type="child_support_derogatory",
            bureau=bureau, accounts=[acc],
            strategy_tags=["FCRA_1681s_1", "FCRA_1681e_b", "FCRA_1681i_a"],
            reason=(
                f"{acc.get('name','')} account {acc.get('account_number','')} "
                f"is a child/family support obligation reported as past due "
                f"(balance: {balance}, past due: {past_due}, status: {payment}). "
                f"Under 15 U.S.C. §1681s-1, child support agencies may only "
                f"report overdue support — the reported amount and status must "
                f"accurately reflect only the delinquent portion as certified "
                f"by the state agency. Full itemization and agency certification required."
            ),
        ))
    return attacks



    attacks = []
    seen_loans: dict[str, list] = {}
    for acc in accounts:
        if acc.get("negative_type") != "student_loan":
            continue
        # Group by name root to detect multiple servicers same loan
        name_root = acc.get("name", "").split()[0].upper()
        seen_loans.setdefault(name_root, []).append(acc)

    for root, accs in seen_loans.items():
        if len(accs) > 1:
            for acc in accs:
                attacks.append(build_attack_record(
                    attack_type="student_loan_multiple_servicer",
                    bureau=bureau, accounts=[acc],
                    strategy_tags=["FCRA_1681e_b", "FCRA_1681i_a", "student_loan"],
                    reason=(
                        f"{acc.get('name','')} account {acc.get('account_number','')} "
                        f"appears to be reported by multiple servicers for the same "
                        f"underlying loan, creating duplicate derogatory reporting."
                    ),
                ))
        else:
            for acc in accs:
                attacks.append(build_attack_record(
                    attack_type="student_loan_status_inaccurate",
                    bureau=bureau, accounts=[acc],
                    strategy_tags=["FCRA_1681e_b", "FCRA_1681i_a", "student_loan"],
                    reason=(
                        f"{acc.get('name','')} account {acc.get('account_number','')} "
                        f"is reporting a derogatory student loan status that requires "
                        f"verification of servicer authority, payment history, and "
                        f"correct deferment or repayment plan status."
                    ),
                ))
    return attacks


def detect_bankruptcy_attacks(bureau: str, accounts: list[dict]) -> list[dict]:
    attacks = []
    for acc in accounts:
        if acc.get("negative_type") != "bankruptcy":
            continue
        raw = " ".join(acc.get("raw_lines", [])).lower()
        # Check if account still shows active balance after BK discharge
        balance = acc.get("balance", "")
        bal_clean = balance.replace("$","").replace(",","").replace(".00","").strip()
        has_balance = bal_clean and bal_clean not in ("0", "-", "")

        if has_balance:
            attacks.append(build_attack_record(
                attack_type="bankruptcy_included_still_active",
                bureau=bureau, accounts=[acc],
                strategy_tags=["FCRA_1681c_a_1", "FCRA_1681e_b", "bankruptcy"],
                reason=(
                    f"{acc.get('name','')} account {acc.get('account_number','')} "
                    f"was included in a bankruptcy proceeding but continues to show "
                    f"an active balance of {balance}. Discharged accounts must reflect "
                    f"a zero balance and discharged status under 11 U.S.C. \u00a7524."
                ),
            ))
        else:
            # Check 10yr/7yr reporting period
            attacks.append(build_attack_record(
                attack_type="bankruptcy_included_still_active",
                bureau=bureau, accounts=[acc],
                strategy_tags=["FCRA_1681c_a_1", "FCRA_1681e_b", "bankruptcy"],
                reason=(
                    f"{acc.get('name','')} account {acc.get('account_number','')} "
                    f"is associated with a bankruptcy and must accurately reflect the "
                    f"discharged status. Under 15 U.S.C. \u00a71681c(a)(1), Chapter 7 "
                    f"bankruptcies may be reported for 10 years and Chapter 13 for 7 years "
                    f"from filing. The reporting period and status must be verified."
                ),
            ))
    return attacks


def detect_repossession_attacks(bureau: str, accounts: list[dict]) -> list[dict]:
    attacks = []
    for acc in accounts:
        if acc.get("negative_type") != "repossession":
            continue
        balance = acc.get("balance", "")
        bal_clean = balance.replace("$","").replace(",","").replace(".00","").strip()
        has_deficiency = bal_clean and bal_clean not in ("0", "-", "")

        attack_type = "repossession_deficiency_unverified" if has_deficiency else "repossession_deficiency_unverified"
        attacks.append(build_attack_record(
            attack_type=attack_type,
            bureau=bureau, accounts=[acc],
            strategy_tags=["FCRA_1681e_b", "FCRA_1681i_a", "UCC_Art9", "repossession"],
            reason=(
                f"{acc.get('name','')} account {acc.get('account_number','')} "
                f"reflects a vehicle repossession. Under UCC Article 9, the creditor "
                f"must apply the net proceeds of any vehicle sale to the outstanding "
                f"balance before reporting a deficiency. "
                + (f"The reported balance of {balance} must be verified with "
                   f"documentation of the sale and how proceeds were applied." if has_deficiency
                   else "The account must accurately reflect whether a deficiency exists "
                        "after the vehicle sale and sale proceeds were credited.")
            ),
        ))
    return attacks


def detect_charge_off_deficiency_attacks(bureau: str, accounts: list[dict]) -> list[dict]:
    attacks = []
    for acc in accounts:
        if acc.get("negative_type") != "charge_off_deficiency":
            continue
        balance = acc.get("balance", "")
        attacks.append(build_attack_record(
            attack_type="charge_off_balance_inflated",
            bureau=bureau, accounts=[acc],
            strategy_tags=["FCRA_1681e_b", "FCRA_1681s_2_a_1", "charge_off"],
            reason=(
                f"{acc.get('name','')} account {acc.get('account_number','')} "
                f"is reported as charged off with a balance of {balance}. "
                f"The reported balance must reflect only the legitimate deficiency "
                f"and may not include fees or interest added after the charge-off date "
                f"unless contractually permitted. Full itemized accounting is required."
            ),
        ))
    return attacks


def detect_paid_collection_attacks(bureau: str, accounts: list[dict]) -> list[dict]:
    attacks = []
    for acc in accounts:
        if acc.get("negative_type") != "paid_collection":
            continue
        attacks.append(build_attack_record(
            attack_type="paid_collection_still_derogatory",
            bureau=bureau, accounts=[acc],
            strategy_tags=["FCRA_1681e_b", "FCRA_1681s_2_a_1", "paid_collection"],
            reason=(
                f"{acc.get('name','')} account {acc.get('account_number','')} "
                f"has been paid or settled (balance: {acc.get('balance','$0.00')}) "
                f"but continues to be reported with a derogatory status. "
                f"A paid or settled collection must reflect its resolved status. "
                f"Reporting it as derogatory after payment is inaccurate under "
                f"15 U.S.C. \u00a71681e(b)."
            ),
        ))
    return attacks


def detect_student_loan_attacks(bureau: str, accounts: list[dict]) -> list[dict]:
    attacks = []
    seen_loans: dict[str, list] = {}
    for acc in accounts:
        if acc.get("negative_type") != "student_loan":
            continue
        name_root = acc.get("name", "").split()[0].upper()
        seen_loans.setdefault(name_root, []).append(acc)

    for root, accs in seen_loans.items():
        if len(accs) > 1:
            for acc in accs:
                attacks.append(build_attack_record(
                    attack_type="student_loan_multiple_servicer",
                    bureau=bureau, accounts=[acc],
                    strategy_tags=["FCRA_1681e_b", "FCRA_1681i_a", "student_loan"],
                    reason=(
                        f"{acc.get('name','')} account {acc.get('account_number','')} "
                        f"appears to be reported by multiple servicers for the same "
                        f"underlying loan, creating duplicate derogatory reporting."
                    ),
                ))
        else:
            for acc in accs:
                attacks.append(build_attack_record(
                    attack_type="student_loan_status_inaccurate",
                    bureau=bureau, accounts=[acc],
                    strategy_tags=["FCRA_1681e_b", "FCRA_1681i_a", "student_loan"],
                    reason=(
                        f"{acc.get('name','')} account {acc.get('account_number','')} "
                        f"is reporting a derogatory student loan status that requires "
                        f"verification of servicer authority, payment history, and "
                        f"correct deferment or repayment plan status."
                    ),
                ))
    return attacks


def _parse_dollar(s: str) -> float:
    try:
        return float(str(s).replace("$","").replace(",","").strip())
    except:
        return 0.0


def detect_intra_account_inconsistencies(bureau: str, accounts: list[dict[str, Any]], report_date: str = "") -> list[dict[str, Any]]:
    """
    Detect logical impossibilities within a single account's own fields.
    These are §1681e(b) violations — the data reported is self-contradictory.

    Patterns detected:
      1. date_opened > date_last_active  (opened after last activity — impossible)
      2. past_due > balance              (owe more past-due than total — impossible)
      3. balance > credit_limit × 2     (revolving — extreme overage, likely error)
      4. balance > high_credit on installment (loan balance exceeds original amount)
      5. status=Open + payment=Chargeoff (open account cannot be charged off)
      6. status=Paid + past_due > $0    (paid account cannot have past-due)
      7. status=Closed + balance > $0   (non-collection closed with balance)
      8. payment=Current + status=Derogatory (current payment but derogatory status)
      9. monthly_payment > $0 on collection  (no payment schedule on collection)
    """
    attacks = []

    for acc in accounts:
        name    = acc.get("name", "")
        acct    = acc.get("account_number", "")
        status  = acc.get("status", "").lower()
        payment = acc.get("payment_status", "").lower()
        balance = _parse_dollar(acc.get("balance", ""))
        past_due= _parse_dollar(acc.get("past_due", ""))
        high_cr = _parse_dollar(acc.get("high_credit", ""))
        cr_lim  = _parse_dollar(acc.get("credit_limit", ""))
        monthly = _parse_dollar(acc.get("monthly_payment", ""))
        acct_det= acc.get("account_type_detail", "").lower()
        acct_typ= acc.get("account_type", "").lower()
        opened  = acc.get("date_opened", "")
        dla     = acc.get("date_last_active", "")

        d_open  = parse_date_field(opened)
        d_dla   = parse_date_field(dla)

        is_collection   = "collection" in acct_det or "collection" in acct_typ
        is_chargeoff    = "chargeoff" in payment or "collection/chargeoff" in payment
        is_installment  = "loan" in acct_det or "installment" in acct_typ

        # 1. Opened after last active (impossible chronology)
        if d_open and d_dla and d_open > d_dla:
            attacks.append(build_attack_record(
                attack_type="opened_after_last_active",
                bureau=bureau, accounts=[acc],
                strategy_tags=["FCRA_1681e_b"],
                reason=(
                    f"{name} account {acct} shows a date opened of {opened} "
                    f"but a date last active of {dla} — which is earlier. "
                    f"An account cannot have activity before it was opened. "
                    f"This is a chronological impossibility that indicates "
                    f"inaccurate date reporting."
                ),
            ))

        # 2. Past due exceeds balance
        if past_due > balance > 0:
            attacks.append(build_attack_record(
                attack_type="past_due_exceeds_balance",
                bureau=bureau, accounts=[acc],
                strategy_tags=["FCRA_1681e_b"],
                reason=(
                    f"{name} account {acct} reports a past-due amount of "
                    f"{acc.get('past_due','')} but a total balance of only "
                    f"{acc.get('balance','')}. The amount past due cannot "
                    f"exceed the total balance. This is a mathematical "
                    f"impossibility and indicates inaccurate reporting."
                ),
            ))

        # 3. Balance exceeds credit limit significantly (revolving)
        if cr_lim > 0 and balance > cr_lim * 1.3 and not is_installment:
            attacks.append(build_attack_record(
                attack_type="balance_exceeds_credit_limit",
                bureau=bureau, accounts=[acc],
                strategy_tags=["FCRA_1681e_b"],
                reason=(
                    f"{name} account {acct} shows a balance of "
                    f"{acc.get('balance','')} that significantly exceeds the "
                    f"credit limit of {acc.get('credit_limit','')}. Even with "
                    f"fees and interest, this level of overage indicates the "
                    f"balance or limit is being reported inaccurately."
                ),
            ))

        # 4. Balance exceeds high credit on installment loan
        if high_cr > 0 and balance > high_cr and is_installment and not is_chargeoff:
            attacks.append(build_attack_record(
                attack_type="balance_exceeds_high_credit",
                bureau=bureau, accounts=[acc],
                strategy_tags=["FCRA_1681e_b"],
                reason=(
                    f"{name} account {acct} shows a current balance of "
                    f"{acc.get('balance','')} that exceeds the original loan "
                    f"amount of {acc.get('high_credit','')}. On an installment "
                    f"loan, the balance can only decrease over time. A balance "
                    f"above the original amount is a reporting error."
                ),
            ))

        # 5. Open status + Collection/Chargeoff payment
        if "open" in status and is_chargeoff:
            attacks.append(build_attack_record(
                attack_type="open_status_chargeoff_conflict",
                bureau=bureau, accounts=[acc],
                strategy_tags=["FCRA_1681e_b"],
                reason=(
                    f"{name} account {acct} is reported with a status of "
                    f"'Open' while the payment status is "
                    f"'{acc.get('payment_status','')}'. An account that has "
                    f"been charged off or placed in collection cannot "
                    f"simultaneously be 'Open' — the account was closed when "
                    f"it was charged off. This classification conflict is inaccurate."
                ),
            ))

        # 6. Paid status + past due > $0
        if "paid" in status and past_due > 0:
            attacks.append(build_attack_record(
                attack_type="paid_status_with_past_due",
                bureau=bureau, accounts=[acc],
                strategy_tags=["FCRA_1681e_b"],
                reason=(
                    f"{name} account {acct} is marked as 'Paid' but also "
                    f"shows a past-due amount of {acc.get('past_due','')}. "
                    f"A paid account cannot have an outstanding past-due balance. "
                    f"These two data points are mutually contradictory."
                ),
            ))

        # 7. Closed status + balance > $0 (non-collection, non-chargeoff)
        if "closed" in status and balance > 0 and not is_chargeoff and not is_collection:
            attacks.append(build_attack_record(
                attack_type="closed_with_balance",
                bureau=bureau, accounts=[acc],
                strategy_tags=["FCRA_1681e_b"],
                reason=(
                    f"{name} account {acct} has a status of 'Closed' but is "
                    f"reporting a balance of {acc.get('balance','')}. A closed "
                    f"account that is not in collection or charged off should "
                    f"reflect a zero balance. The balance being reported here "
                    f"may not be accurate."
                ),
            ))

        # 8. Current payment + Derogatory status
        if "current" in payment and "derogatory" in status:
            attacks.append(build_attack_record(
                attack_type="current_payment_derogatory_status",
                bureau=bureau, accounts=[acc],
                strategy_tags=["FCRA_1681e_b"],
                reason=(
                    f"{name} account {acct} shows a payment status of 'Current' "
                    f"— meaning no payment is late — but an account status of "
                    f"'Derogatory'. If all payments are current, the account "
                    f"cannot be derogatory. These two classifications are "
                    f"contradictory and indicate a reporting error."
                ),
            ))

        # 9. Monthly payment > $0 on collection account
        if monthly > 0 and is_collection and not ("child support" in acct_det or "family support" in acct_det):
            attacks.append(build_attack_record(
                attack_type="monthly_payment_on_collection",
                bureau=bureau, accounts=[acc],
                strategy_tags=["FCRA_1681e_b"],
                reason=(
                    f"{name} account {acct} is a collection account reporting "
                    f"a monthly payment of {acc.get('monthly_payment','')}. "
                    f"Collection accounts do not have an ongoing payment schedule "
                    f"— the debt has been transferred and there is no creditor "
                    f"expecting monthly payments. This field should be zero."
                ),
            ))

    return attacks


def detect_cross_bureau_field_conflicts(
    bureau: str,
    accounts: list[dict[str, Any]],
    all_bureaus_inventory: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    """
    Detect cross-bureau inconsistencies in fields we don't currently check:
    date_opened, account_type_detail, high_credit, credit_limit, terms.
    """
    attacks = []

    for acc in accounts:
        name     = acc.get("name", "")
        acct_num = acc.get("account_number", "")

        # Find matching accounts in other bureaus
        matches = {}
        for b, b_accs in all_bureaus_inventory.items():
            if b == bureau: continue
            for b_acc in b_accs:
                if (b_acc.get("name","") == name and
                    b_acc.get("account_number","") == acct_num):
                    matches[b] = b_acc

        if not matches: continue

        all_entries = {bureau: acc, **matches}

        def p(s): return _parse_dollar(s)

        # date_opened conflict > 60 days
        dates_opened = {}
        for b, e in all_entries.items():
            d = parse_date_field(e.get("date_opened",""))
            if d: dates_opened[b] = (d, e.get("date_opened",""))
        if len(dates_opened) > 1:
            dvals = [d for d,_ in dates_opened.values()]
            diff = (max(dvals) - min(dvals)).days
            if diff > 60:
                attacks.append(build_attack_record(
                    attack_type="cross_bureau_date_opened_conflict",
                    bureau=bureau, accounts=[acc],
                    strategy_tags=["FCRA_1681e_b"],
                    reason=(
                        f"{name} account {acct_num} shows different date opened "
                        f"values across bureaus: "
                        + ", ".join(f"{b}={s}" for b,(_,s) in dates_opened.items())
                        + f". The date an account was opened cannot be different "
                        f"depending on which bureau is reporting it — only one "
                        f"date is correct."
                    ),
                ))

        # account_type_detail conflict
        acct_types = {}
        for b, e in all_entries.items():
            v = e.get("account_type_detail","").strip()
            if v and v not in ("-",""):
                acct_types[b] = v
        unique_types = set(acct_types.values())
        if len(unique_types) > 1:
            attacks.append(build_attack_record(
                attack_type="cross_bureau_account_type_conflict",
                bureau=bureau, accounts=[acc],
                strategy_tags=["FCRA_1681e_b"],
                reason=(
                    f"{name} account {acct_num} is classified differently "
                    f"across bureaus: "
                    + ", ".join(f"{b}='{v}'" for b,v in acct_types.items())
                    + ". The account type cannot vary by bureau — "
                    f"this indicates at least one bureau is reporting an "
                    f"incorrect classification."
                ),
            ))

        # credit_limit conflict
        cl_vals = {b: p(e.get("credit_limit","")) for b,e in all_entries.items()}
        cl_nonzero = {b: v for b,v in cl_vals.items() if v > 0}
        if len(set(cl_nonzero.values())) > 1:
            attacks.append(build_attack_record(
                attack_type="cross_bureau_credit_limit_conflict",
                bureau=bureau, accounts=[acc],
                strategy_tags=["FCRA_1681e_b"],
                reason=(
                    f"{name} account {acct_num} shows different credit limits "
                    f"across bureaus: "
                    + ", ".join(f"{b}=${v:,.2f}" for b,v in cl_nonzero.items())
                    + ". A credit limit is set by the creditor and cannot be "
                    f"different at different bureaus. One or more bureaus is "
                    f"reporting an incorrect limit."
                ),
            ))

        # high_credit conflict
        hc_vals = {b: p(e.get("high_credit","")) for b,e in all_entries.items()}
        hc_nonzero = {b: v for b,v in hc_vals.items() if v > 0}
        if len(set(hc_nonzero.values())) > 1:
            spread = max(hc_nonzero.values()) - min(hc_nonzero.values())
            if spread > 100:  # ignore tiny rounding differences
                attacks.append(build_attack_record(
                    attack_type="cross_bureau_high_credit_conflict",
                    bureau=bureau, accounts=[acc],
                    strategy_tags=["FCRA_1681e_b"],
                    reason=(
                        f"{name} account {acct_num} shows different high credit "
                        f"amounts across bureaus: "
                        + ", ".join(f"{b}=${v:,.2f}" for b,v in hc_nonzero.items())
                        + ". The original loan amount or highest balance cannot "
                        f"differ by bureau — this discrepancy indicates an "
                        f"accuracy problem in at least one bureau's file."
                    ),
                ))

    return attacks


def detect_inquiry_attacks(inquiries: list[dict[str, Any]], accounts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Detect inquiry-level inconsistencies:
    1. Same creditor, same bureau, same day — duplicate inquiry
    2. Large cluster same day (>5) — may indicate unauthorized batch pull
    """
    attacks = []
    if not inquiries:
        return attacks

    from collections import defaultdict
    by_date_bureau: dict[tuple, list] = defaultdict(list)
    by_name_bureau_date: dict[tuple, list] = defaultdict(list)

    for inq in inquiries:
        name   = inq.get("creditor_name","")
        bureau = inq.get("bureau","").lower()
        date   = inq.get("date","")
        key_date   = (date, bureau)
        key_exact  = (name, bureau, date)
        by_date_bureau[key_date].append(inq)
        by_name_bureau_date[key_exact].append(inq)

    # Duplicate exact inquiries
    seen_dups = set()
    for (name, bureau, date), inqs in by_name_bureau_date.items():
        if len(inqs) > 1:
            dedup_key = (name, bureau, date)
            if dedup_key not in seen_dups:
                seen_dups.add(dedup_key)
                attacks.append({
                    "attack_type": "duplicate_inquiry_same_creditor",
                    "bureau":      bureau,
                    "creditor":    name,
                    "date":        date,
                    "count":       len(inqs),
                    "laws":        ["15 USC 1681b", "15 USC 1681n"],
                    "reason": (
                        f"{name} pulled my credit report {len(inqs)} times "
                        f"at {bureau.title()} on {date}. Each credit inquiry "
                        f"requires a separate permissible purpose under "
                        f"15 U.S.C. §1681b. Multiple pulls in one day from the "
                        f"same creditor without separate applications constitute "
                        f"an unauthorized inquiry and must be removed."
                    ),
                })

    # Large cluster same day (5+ different creditors)
    seen_clusters = set()
    for (date, bureau), inqs in by_date_bureau.items():
        unique_names = set(i.get("creditor_name","") for i in inqs)
        if len(unique_names) >= 5 and (date, bureau) not in seen_clusters:
            seen_clusters.add((date, bureau))
            attacks.append({
                "attack_type": "inquiry_cluster_same_day",
                "bureau":      bureau,
                "date":        date,
                "count":       len(unique_names),
                "creditors":   list(unique_names)[:5],
                "laws":        ["15 USC 1681b"],
                "reason": (
                    f"My {bureau.title()} credit report was pulled by "
                    f"{len(unique_names)} different creditors on {date}. "
                    f"While rate shopping for a single loan may justify "
                    f"multiple inquiries, each inquiry must still have an "
                    f"independent permissible purpose under 15 U.S.C. §1681b. "
                    f"I am requesting that each creditor confirm the permissible "
                    f"purpose for their inquiry or that unauthorized pulls be removed."
                ),
            })

    return attacks


def detect_late_collection_conflict_attacks(bureau: str, accounts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Detect accounts where payment status and account type are mutually exclusive.

    Three patterns:
      TYPE A — account_type=Collection + payment_status=Late X Days
               A collection has no active payment obligation — it cannot be "late."
               Two mutually exclusive classifications on the same account.

      TYPE B — payment_status=Collection/Chargeoff + separate late payment history
               One delinquency event cannot generate both a charge-off AND independent
               late-payment notations. Double-derogatory for a single event.

      TYPE C — account_type=Collection but payment_status shows Late (no CO code)
               Collection account disguised as a late payment — misclassification
               that distorts what the account actually is.
    """
    attacks = []

    for acc in accounts:
        payment     = safe_lower(acc.get("payment_status", ""))
        acct_detail = safe_lower(acc.get("account_type_detail", ""))
        acct_type   = safe_lower(acc.get("account_type", ""))
        raw         = " ".join(acc.get("raw_lines", [])).lower()
        name        = acc.get("name", "")
        acct_num    = acc.get("account_number", "")

        is_collection_type  = "collection" in acct_detail or "collection" in acct_type
        has_late_payment    = "late" in payment and "current" not in payment
        has_chargeoff_pay   = "chargeoff" in payment or "collection/chargeoff" in payment
        has_chargeoff_raw   = "charged off" in raw or "profit and loss" in raw

        # TYPE A: Collection account type + Late X Days payment status
        if is_collection_type and has_late_payment and not has_chargeoff_pay:
            attacks.append(build_attack_record(
                attack_type="collection_late_payment_conflict",
                bureau=bureau,
                accounts=[acc],
                strategy_tags=["FCRA_1681e_b", "FCRA_1681s_2_a_1", "FCRA_1681i_a"],
                reason=(
                    f"{name} account {acct_num} is classified as a collection "
                    f"account but is also being reported with a payment status of "
                    f"'{acc.get('payment_status','')}'. These two classifications "
                    f"are mutually exclusive. A collection account has already "
                    f"defaulted and been transferred — there is no active payment "
                    f"obligation, so it cannot be 'late.' Reporting both inflates "
                    f"the negative impact of a single delinquency event."
                ),
            ))

        # TYPE B: Charge-off + also has late payment notations in raw for same event
        elif has_chargeoff_pay and has_late_payment:
            attacks.append(build_attack_record(
                attack_type="late_collection_conflict",
                bureau=bureau,
                accounts=[acc],
                strategy_tags=["FCRA_1681e_b", "FCRA_1681i_a", "classification_conflict"],
                reason=(
                    f"{name} account {acct_num} shows both a charge-off/collection "
                    f"status and a late payment classification. A single delinquency "
                    f"event cannot be reported as both a charge-off and a separate "
                    f"late payment — these represent the same failure being double-counted."
                ),
            ))

        # TYPE B alt: Collection/Chargeoff + raw has additional late language beyond CO
        elif has_chargeoff_pay and ("late" in raw) and is_collection_type:
            attacks.append(build_attack_record(
                attack_type="late_collection_conflict",
                bureau=bureau,
                accounts=[acc],
                strategy_tags=["FCRA_1681e_b", "FCRA_1681i_a", "classification_conflict"],
                reason=(
                    f"{name} account {acct_num} is reported as a charged-off "
                    f"collection but the reporting also includes late payment "
                    f"language for the same underlying default. This creates "
                    f"duplicate negative impact from a single event."
                ),
            ))

    return attacks


def build_legal_attacks_from_account_number_groups(bureau: str, accounts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = {}

    for acc in accounts:
        acct = normalize_spaces(acc.get("account_number", ""))
        if not acct:
            continue
        groups.setdefault(acct, []).append(acc)

    attacks = []

    for acc_number, items in groups.items():
        block_ids = {item.get("block_id", "") for item in items}
        if len(items) > 1 and len(block_ids) > 1:
            balances = {normalize_spaces(item.get("balance", "")) for item in items}
            names = {normalize_spaces(item.get("name", "")) for item in items}

            if len(balances) == 1:
                attacks.append(
                    build_attack_record(
                        attack_type="same_account_number_same_balance",
                        bureau=bureau,
                        accounts=items,
                        strategy_tags=[
                            "FCRA_1681e_b",
                            "FCRA_1681i",
                            "duplicate_reporting",
                            "chain_of_title_demand",
                        ],
                        reason=f"Same account number appears under multiple tradelines with the same balance: {acc_number}",
                    )
                )
            elif len(names) > 1:
                attacks.append(
                    build_attack_record(
                        attack_type="same_account_number_different_furnisher",
                        bureau=bureau,
                        accounts=items,
                        strategy_tags=[
                            "FCRA_1681e_b",
                            "FCRA_1681i",
                            "FCRA_1681s_2_b",
                        ],
                        reason=f"Same account number is associated with multiple furnisher names: {acc_number}",
                    )
                )

    return attacks


def is_collector_name(name: str) -> bool:
    """True if the furnisher name is a debt collector / debt buyer."""
    markers = ["lvnv", "portfolio", "cavalry", "midland", "resurgent", "asset acceptance",
                "unifin", "amsher", "convergent", "national credit", "enhanced recovery"]
    n = safe_lower(name)
    return any(m in n for m in markers)


def has_original_creditor_label(name: str) -> bool:
    """True if name contains '(original creditor:...')' pattern."""
    return "(original creditor:" in safe_lower(name)


def detect_cross_bureau_inconsistency_attacks(
    negatives_by_bureau: dict[str, list[dict[str, Any]]],
    base_tradelines: list[dict[str, Any]] | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """
    Cross-bureau analysis pass.

    CRITICAL RULE: Account number variation across bureaus for the SAME
    tradeline block is NORMAL and COSMETIC — not an attack.
    Example: NAVY FCU reports 406095**** (TU), 83** (EXP), 406095XXXXXX**** (EQ)
    — these are the same account, just masked differently per bureau.

    We MUST use base_tradeline_id (PDF block grouping) as the anchor for
    same-account detection, NOT the account number string.

    What IS an attack:
    - Same block, different BALANCE across bureaus
    - Same block, different PAYMENT STATUS across bureaus
    - Same block, different ACCOUNT STATUS across bureaus
    - Same block with meaningful furnisher name shift (not just masking variation)

    Separate blocks with same account# / same balance = handled by the
    intra-bureau detectors (duplicate_account_number, same_account_number_same_balance).
    """
    result: dict[str, list[dict[str, Any]]] = {b: [] for b in BUREAUS}

    if not base_tradelines:
        return result

    # Build a lookup: block_id -> bureau_entry for quick access
    # We only care about blocks that have negative entries
    neg_block_ids: set[str] = set()
    for accounts in negatives_by_bureau.values():
        for acc in accounts:
            bid = acc.get("block_id", "")
            if bid:
                neg_block_ids.add(bid)

    # Map block_id -> the acc objects per bureau from negatives_by_bureau
    block_to_neg_accs: dict[str, dict[str, dict[str, Any]]] = {}
    for bureau, accounts in negatives_by_bureau.items():
        for acc in accounts:
            bid = acc.get("block_id", "")
            if not bid:
                continue
            block_to_neg_accs.setdefault(bid, {})[bureau] = acc

    for bt in base_tradelines:
        btid = bt.get("base_tradeline_id", "")
        if btid not in neg_block_ids:
            continue

        bureau_entries = bt.get("bureau_entries", {})
        present_bureaus = [b for b in BUREAUS if b in bureau_entries]

        if len(present_bureaus) < 2:
            continue

        furnisher_name = bt.get("furnisher_name", "")

        # Collect values per bureau directly from base_tradeline bureau_entries
        balances      = {b: clean_balance(bureau_entries[b].get("balance", ""))       for b in present_bureaus}
        payment_stats = {b: safe_lower(bureau_entries[b].get("payment_status", ""))   for b in present_bureaus}
        acct_statuses = {b: safe_lower(bureau_entries[b].get("status", ""))           for b in present_bureaus}

        # For attack records we need the acc from negatives_by_bureau (has negative_type etc)
        neg_accs = block_to_neg_accs.get(btid, {})

        def acc_for(bureau: str) -> dict[str, Any]:
            """Return the negative acc for this bureau, or a minimal stub."""
            if bureau in neg_accs:
                return neg_accs[bureau]
            # Stub from base_tradeline data — bureau may not be negative
            return {
                "block_id": btid,
                "name": furnisher_name,
                "account_number": bureau_entries.get(bureau, {}).get("account_number", ""),
                "balance": bureau_entries.get(bureau, {}).get("balance", ""),
                "payment_status": bureau_entries.get(bureau, {}).get("payment_status", ""),
                "status": bureau_entries.get(bureau, {}).get("status", ""),
                "bureau": bureau,
            }

        # -----------------------------------------------------------------
        # 1. BALANCE CONFLICT (material — directly affects credit scoring)
        # -----------------------------------------------------------------
        unique_bals = {v for v in balances.values() if v and v not in {"0", "0.0"}}
        if len(unique_bals) > 1:
            bal_desc = ", ".join(f"{b}=${v}" for b, v in balances.items())
            for bureau in present_bureaus:
                if bureau not in neg_accs:
                    continue
                result[bureau].append(
                    build_attack_record(
                        attack_type="cross_bureau_balance_conflict",
                        bureau=bureau,
                        accounts=[acc_for(bureau)],
                        strategy_tags=["FCRA_1681e_b", "FCRA_1681i", "balance_conflict"],
                        reason=(
                            f"{furnisher_name} reports different balances across bureaus "
                            f"for the same account: {bal_desc}. "
                            "At least one bureau is receiving inaccurate data."
                        ),
                    )
                )

        # -----------------------------------------------------------------
        # 2. PAYMENT STATUS CONFLICT
        #    Normalize "collection/chargeoff" variants so minor wording diffs
        #    don't trigger false positives.
        # -----------------------------------------------------------------
        def normalize_pay(p: str) -> str:
            p = p.lower().strip()
            if "collection" in p or "chargeoff" in p or "charge off" in p:
                return "collection_chargeoff"
            if "late" in p:
                # "late 120 days" vs "late 90 days" are different — keep as-is
                return p
            return p

        norm_pays = {b: normalize_pay(payment_stats[b]) for b in present_bureaus}
        unique_pays = {v for v in norm_pays.values() if v}
        if len(unique_pays) > 1:
            pay_desc = ", ".join(f"{b}={payment_stats[b]!r}" for b in present_bureaus)
            for bureau in present_bureaus:
                if bureau not in neg_accs:
                    continue
                result[bureau].append(
                    build_attack_record(
                        attack_type="cross_bureau_payment_status_conflict",
                        bureau=bureau,
                        accounts=[acc_for(bureau)],
                        strategy_tags=["FCRA_1681e_b", "FCRA_1681i", "payment_status_conflict"],
                        reason=(
                            f"{furnisher_name} reports different payment statuses across bureaus "
                            f"for the same account: {pay_desc}. "
                            "Inconsistent classification is a material accuracy violation."
                        ),
                    )
                )

        # -----------------------------------------------------------------
        # 3. ACCOUNT STATUS CONFLICT (Open vs Closed vs Derogatory etc.)
        # -----------------------------------------------------------------
        unique_stats = {v for v in acct_statuses.values() if v}
        if len(unique_stats) > 1:
            stat_desc = ", ".join(f"{b}={acct_statuses[b]!r}" for b in present_bureaus)
            for bureau in present_bureaus:
                if bureau not in neg_accs:
                    continue
                result[bureau].append(
                    build_attack_record(
                        attack_type="cross_bureau_account_status_conflict",
                        bureau=bureau,
                        accounts=[acc_for(bureau)],
                        strategy_tags=["FCRA_1681e_b", "FCRA_1681i", "account_status_conflict"],
                        reason=(
                            f"{furnisher_name} reports different account statuses across bureaus "
                            f"for the same account: {stat_desc}."
                        ),
                    )
                )

    return result


def detect_single_bureau_collector_pattern(
    bureau: str,
    accounts: list[dict[str, Any]],
    all_negatives: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    """
    Detect collector/original-creditor WITHIN a bureau where the label
    '(Original Creditor: X)' is embedded in the collector name itself —
    meaning a single account already self-declares the chain of title.

    This is valid even with a single tradeline because the name itself
    is evidence of a chain-of-title transfer that requires verification.
    """
    attacks = []

    for acc in accounts:
        name = acc.get("name", "")
        if has_original_creditor_label(name) and is_collector_name(name):
            attacks.append(
                build_attack_record(
                    attack_type="collector_original_creditor_self_declared",
                    bureau=bureau,
                    accounts=[acc],
                    strategy_tags=[
                        "FCRA_1681e_b",
                        "FCRA_1681i",
                        "FCRA_1681s_2_b",
                        "chain_of_title_demand",
                        "collector_authority_challenge",
                    ],
                    reason=(
                        f"{name} self-declares an original creditor in its own name "
                        f"(account {acc.get('account_number', '')}), "
                        "requiring documented chain-of-title and proof of lawful reporting authority."
                    ),
                )
            )

    return attacks


def detect_absent_bureau_inconsistency(
    bureau: str,
    accounts: list[dict[str, Any]],
    all_negatives: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    """
    If a negative account appears in 2 bureaus but NOT in a third,
    flag it on the bureaus where it IS present as a reporting inconsistency.
    The absent bureau is inconsistent with the others — the furnisher
    should be reporting to all 3 or explaining why not.
    """
    # Build set of (acct, name) pairs per bureau
    present: dict[str, set[str]] = {}
    for b, accs in all_negatives.items():
        present[b] = {normalize_spaces(a.get("account_number", "")) for a in accs if a.get("account_number")}

    attacks = []
    all_bureaus = set(BUREAUS)

    for acc in accounts:
        acct = normalize_spaces(acc.get("account_number", ""))
        if not acct:
            continue
        bureaus_with_acct = {b for b in BUREAUS if acct in present.get(b, set())}
        missing_bureaus = all_bureaus - bureaus_with_acct

        if len(bureaus_with_acct) >= 2 and missing_bureaus:
            attacks.append(
                build_attack_record(
                    attack_type="absent_bureau_reporting_inconsistency",
                    bureau=bureau,
                    accounts=[acc],
                    strategy_tags=[
                        "FCRA_1681e_b",
                        "FCRA_1681i",
                        "selective_bureau_reporting",
                    ],
                    reason=(
                        f"Account {acct} ({acc.get('name', '')}) is reported in "
                        f"{sorted(bureaus_with_acct)} but absent in {sorted(missing_bureaus)}. "
                        "Selective bureau reporting creates a material inconsistency."
                    ),
                )
            )

    return attacks



def detect_late_payment_attacks(bureau: str, accounts: list) -> list:
    """Dispute 30/60/90-day late marks found in two-year payment history."""
    attacks = []
    for acc in accounts:
        late_codes  = acc.get("late_payment_codes", [])
        actual_lates = [c for c in late_codes if not c.startswith("CO:")]
        if not actual_lates:
            continue
        name     = acc.get("name", "")
        acct_num = acc.get("account_number", "")
        payment  = acc.get("payment_status", "").lower()
        if "collection" in payment or "chargeoff" in payment:
            continue
        worst = "30"
        for code in actual_lates:
            val = code.split(":")[0]
            if val in ("90","120") and worst in ("30","60"):
                worst = val
            elif val == "60" and worst == "30":
                worst = val
        status    = acc.get("status","").lower()
        is_closed = any(k in status for k in ("closed","paid","refinanced"))
        late_str  = ", ".join(actual_lates)
        if is_closed:
            reason = (
                f"{name} (account {acct_num}) is closed/paid but still shows a "
                f"{worst}-day late in its history ({late_str}). The Date of First "
                f"Delinquency must be correctly reported under 15 U.S.C. §1681c(a)(4) "
                f"so the 7-year clock can be verified. I am requesting original payment "
                f"records confirming the late date and the correct DOFD."
            )
        else:
            reason = (
                f"{name} (account {acct_num}) shows a {worst}-day late payment in "
                f"its history ({late_str}). Under 15 U.S.C. §1681e(b), I am requesting "
                f"documentation — original payment records showing when payment was due "
                f"and when it was received. If this cannot be verified, the late mark "
                f"must be removed."
            )
        attacks.append(build_attack_record(
            attack_type="late_payment_history_dispute",
            bureau=bureau,
            accounts=[acc],
            strategy_tags=["FCRA_1681e_b","FCRA_1681s_2_a_1","FCRA_1681i"],
            reason=reason,
        ))
    return attacks


def detect_cross_bureau_late_date_conflict(
    bureau: str,
    accounts: list,
    all_bureaus_negatives: dict,
) -> list:
    """Flag when the same late payment is reported in different months across bureaus."""
    attacks = []
    for acc in accounts:
        late_codes   = acc.get("late_payment_codes", [])
        actual_lates = [c for c in late_codes if not c.startswith("CO:")]
        if not actual_lates:
            continue
        name     = acc.get("name", "")
        acct_num = acc.get("account_number", "")
        bureau_late_map = {bureau: set(actual_lates)}
        for b, b_accs in all_bureaus_negatives.items():
            if b == bureau:
                continue
            for b_acc in b_accs:
                if b_acc.get("name","") == name and b_acc.get("account_number","") == acct_num:
                    other = [x for x in b_acc.get("late_payment_codes",[]) if not x.startswith("CO:")]
                    if other:
                        bureau_late_map[b] = set(other)
        if len(bureau_late_map) < 2:
            continue
        all_sets = list(bureau_late_map.values())
        if set.union(*all_sets) == set.intersection(*all_sets):
            continue  # all bureaus agree
        detail = " | ".join(f"{b}: {sorted(v)}" for b,v in sorted(bureau_late_map.items()))
        attacks.append(build_attack_record(
            attack_type="cross_bureau_payment_history_date_conflict",
            bureau=bureau,
            accounts=[acc],
            strategy_tags=["FCRA_1681e_b","FCRA_1681s_2_a_1"],
            reason=(
                f"{name} (account {acct_num}) reports late payments in different months "
                f"across bureaus: {detail}. A payment can only be late on one specific "
                f"date — inconsistent reporting is inaccurate under 15 U.S.C. §1681e(b). "
                f"The creditor must provide original payment records and correct the "
                f"reporting to show the same month on all bureaus."
            ),
        ))
    return attacks

def build_legal_detection_engine(
    negatives_by_bureau: dict[str, list[dict[str, Any]]],
    base_tradelines: list[dict[str, Any]] | None = None,
    report_date: str = "",
    client_state: str = "",
) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {}

    # --- Pass 1: Per-bureau intra-bureau attacks ---
    for bureau, accounts in negatives_by_bureau.items():
        bureau_attacks: list[dict[str, Any]] = []

        bureau_attacks.extend(detect_duplicate_account_number_attacks(bureau, accounts))
        bureau_attacks.extend(detect_multi_furnisher_same_balance_attacks(bureau, accounts))
        bureau_attacks.extend(detect_collector_original_creditor_pattern_attacks(bureau, accounts))
        bureau_attacks.extend(detect_late_collection_conflict_attacks(bureau, accounts))
        bureau_attacks.extend(build_legal_attacks_from_account_number_groups(bureau, accounts))
        bureau_attacks.extend(
            detect_single_bureau_collector_pattern(bureau, accounts, negatives_by_bureau)
        )
        bureau_attacks.extend(
            detect_absent_bureau_inconsistency(bureau, accounts, negatives_by_bureau)
        )

        # --- DOFD attacks: §1681c obsolete + re-aging + unknown ---
        bureau_attacks.extend(detect_obsolete_account_attacks(bureau, accounts))
        bureau_attacks.extend(detect_re_aging_attacks(bureau, accounts))
        bureau_attacks.extend(detect_dofd_unknown_attacks(bureau, accounts))
        bureau_attacks.extend(detect_late_payment_attacks(bureau, accounts))
        bureau_attacks.extend(
            detect_cross_bureau_late_date_conflict(bureau, accounts, negatives_by_bureau)
        )

        # --- Intra-account field contradiction attacks ---
        bureau_attacks.extend(detect_intra_account_inconsistencies(bureau, accounts))

        # --- Cross-bureau field conflicts (needs full inventory) ---
        bureau_attacks.extend(detect_cross_bureau_field_conflicts(bureau, accounts, negatives_by_bureau))

        # --- New account type attacks ---
        bureau_attacks.extend(detect_child_support_attacks(bureau, accounts))
        bureau_attacks.extend(detect_student_loan_attacks(bureau, accounts))
        bureau_attacks.extend(detect_student_loan_complex_attacks(
            bureau, accounts,
            all_bureaus_inventory=negatives_by_bureau,
            report_date=report_date,
        ))
        bureau_attacks.extend(detect_bankruptcy_attacks(bureau, accounts))
        bureau_attacks.extend(detect_repossession_attacks(bureau, accounts))
        bureau_attacks.extend(detect_charge_off_deficiency_attacks(bureau, accounts))
        bureau_attacks.extend(detect_paid_collection_attacks(bureau, accounts))

        # --- Medical debt attacks ---
        bureau_attacks.extend(detect_medical_debt_attacks(
            bureau, accounts,
            report_date=report_date,
            client_state=client_state,
        ))

        result[bureau] = bureau_attacks

    # --- Pass 2: Cross-bureau analysis anchored on base_tradeline_id ---
    cross_bureau_attacks = detect_cross_bureau_inconsistency_attacks(
        negatives_by_bureau, base_tradelines
    )
    for bureau, attacks in cross_bureau_attacks.items():
        result.setdefault(bureau, []).extend(attacks)

    # --- Deduplicate: same attack_type + same account_number + same bureau ---
    for bureau in result:
        seen: set[tuple[str, str]] = set()
        deduped: list[dict[str, Any]] = []
        for attack in result[bureau]:
            for acc in attack.get("accounts", []):
                key = (attack.get("attack_type", ""), acc.get("account_number", ""))
                if key not in seen:
                    seen.add(key)
                    deduped.append(attack)
                    break
        result[bureau] = deduped

    return result


def build_legal_detection_summary(
    negatives_by_bureau: dict[str, list[dict[str, Any]]],
    legal_detection_engine: dict[str, list[dict[str, Any]]]
) -> dict[str, dict[str, Any]]:
    summary: dict[str, dict[str, Any]] = {}

    for bureau in BUREAUS:
        accounts = negatives_by_bureau.get(bureau, [])
        attacks = legal_detection_engine.get(bureau, [])

        attack_type_counts: dict[str, int] = {}
        unique_accounts_in_attacks = set()

        for attack in attacks:
            attack_type = attack.get("attack_type", "")
            if attack_type:
                attack_type_counts[attack_type] = attack_type_counts.get(attack_type, 0) + 1

            for acc in attack.get("accounts", []):
                block_id = acc.get("block_id", "")
                acct_num = acc.get("account_number", "")
                unique_accounts_in_attacks.add((block_id, acct_num))

        summary[bureau] = {
            "accounts_evaluated": len(accounts),
            "attacks_found": len(attacks),
            "unique_accounts_in_attacks": len(unique_accounts_in_attacks),
            "attack_types_found": sorted(list(attack_type_counts.keys())),
            "attack_type_counts": attack_type_counts,
        }

    return summary


# =========================
# ATTACK SCORING ENGINE
# =========================

def get_attack_severity_score(attack_type: str) -> int:
    mapping = {
        "duplicate_account_number": 90,
        "same_account_number_same_balance": 96,
        "same_account_number_different_furnisher": 93,
        "multi_furnisher_same_balance": 94,
        "collector_original_creditor_pattern": 95,
        "collector_original_creditor_self_declared": 92,
        "late_collection_conflict": 88,
        "cross_bureau_balance_conflict": 91,
        "cross_bureau_payment_status_conflict": 89,
        "cross_bureau_furnisher_identity_shift": 90,
        "cross_bureau_account_status_conflict": 88,
        "absent_bureau_reporting_inconsistency": 82,
        "obsolete_account_7yr_limit": 99,
        "potential_re_aging": 97,
        "dofd_unknown_verification_required": 88,
        "student_loan_multiple_servicer": 91,
        "student_loan_status_inaccurate": 85,
        "bankruptcy_included_still_active": 96,
        "bankruptcy_reporting_period_exceeded": 99,
        "repossession_deficiency_unverified": 90,
        "repossession_proceeds_not_credited": 92,
        "charge_off_balance_inflated": 88,
        "paid_collection_still_derogatory": 86,
    }
    return mapping.get(attack_type, 70)


def get_attack_confidence_score(attack: dict[str, Any]) -> int:
    attack_type = attack.get("attack_type", "")
    account_count = len(attack.get("accounts", []))
    base = 80

    if attack_type in {
        "same_account_number_same_balance",
        "duplicate_account_number",
        "collector_original_creditor_pattern",
    }:
        base = 95
    elif attack_type == "multi_furnisher_same_balance":
        base = 90
    elif attack_type == "late_payment_history_dispute":
        base = 82
    elif attack_type == "cross_bureau_payment_history_date_conflict":
        base = 85
    elif attack_type == "late_collection_conflict":
        base = 85

    if account_count >= 2:
        base += 2

    return min(base, 99)


def get_attack_priority(severity_score: int) -> str:
    if severity_score >= 95:
        return "critical"
    if severity_score >= 90:
        return "high"
    if severity_score >= 80:
        return "medium"
    return "low"


def get_recommended_round(severity_score: int, attack_type: str) -> str:
    if attack_type in {
        "same_account_number_same_balance",
        "collector_original_creditor_pattern",
        "collector_original_creditor_self_declared",
        "multi_furnisher_same_balance",
        "cross_bureau_balance_conflict",
        "cross_bureau_payment_status_conflict",
        "cross_bureau_furnisher_identity_shift",
        "cross_bureau_account_status_conflict",
    }:
        return "round_2"
    if attack_type in {
        "duplicate_account_number",
        "late_collection_conflict",
        "absent_bureau_reporting_inconsistency",
        "same_account_number_different_furnisher",
        "obsolete_account_7yr_limit",
        "potential_re_aging",
        "dofd_unknown_verification_required",
        "requires_basic_verification",
        "late_payment_history_dispute",
        "cross_bureau_payment_history_date_conflict",
    }:
        return "round_1"
    return "round_1"


def get_recommended_methods(attack_type: str) -> list[str]:
    mapping = {
        "duplicate_account_number": [
            "bureau_dispute",
            "direct_furnisher_dispute",
        ],
        "same_account_number_same_balance": [
            "bureau_dispute",
            "chain_of_title_demand",
            "direct_furnisher_dispute",
        ],
        "same_account_number_different_furnisher": [
            "bureau_dispute",
            "direct_furnisher_dispute",
        ],
        "multi_furnisher_same_balance": [
            "bureau_dispute",
            "chain_of_title_demand",
            "direct_furnisher_dispute",
        ],
        "collector_original_creditor_pattern": [
            "bureau_dispute",
            "chain_of_title_demand",
            "direct_furnisher_dispute",
            "acdv_audit_trail_request",
        ],
        "collector_original_creditor_self_declared": [
            "bureau_dispute",
            "chain_of_title_demand",
            "direct_furnisher_dispute",
            "acdv_audit_trail_request",
        ],
        "late_collection_conflict": [
            "bureau_dispute",
            "direct_furnisher_dispute",
        ],
        "cross_bureau_balance_conflict": [
            "bureau_dispute",
            "direct_furnisher_dispute",
        ],
        "cross_bureau_payment_status_conflict": [
            "bureau_dispute",
            "direct_furnisher_dispute",
        ],
        "cross_bureau_furnisher_identity_shift": [
            "bureau_dispute",
            "direct_furnisher_dispute",
            "chain_of_title_demand",
        ],
        "cross_bureau_account_status_conflict": [
            "bureau_dispute",
            "direct_furnisher_dispute",
        ],
        "obsolete_account_7yr_limit": [
            "bureau_dispute",
            "mandatory_deletion_demand",
        ],
        "potential_re_aging": [
            "bureau_dispute",
            "direct_furnisher_dispute",
            "dofd_verification_demand",
        ],
        "dofd_unknown_verification_required": [
            "bureau_dispute",
            "direct_furnisher_dispute",
            "dofd_disclosure_demand",
        ],
        "late_payment_history_dispute": [
        "15 USC 1681e(b)",
        "15 USC 1681s-2(a)(1)",
        "15 USC 1681i(a)",
    ],
    "cross_bureau_payment_history_date_conflict": [
        "15 USC 1681e(b)",
        "15 USC 1681s-2(a)(1)",
        "15 USC 1681i(a)",
    ],
    "absent_bureau_reporting_inconsistency": [
            "bureau_dispute",
            "direct_furnisher_dispute",
        ],
    }
    return mapping.get(attack_type, ["bureau_dispute"])


def build_attack_scoring_engine(legal_detection_engine: dict[str, list[dict[str, Any]]]) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {}

    for bureau, attacks in legal_detection_engine.items():
        scored_attacks = []

        for attack in attacks:
            severity_score = get_attack_severity_score(attack.get("attack_type", ""))
            confidence_score = get_attack_confidence_score(attack)
            priority = get_attack_priority(severity_score)
            recommended_round = get_recommended_round(severity_score, attack.get("attack_type", ""))
            recommended_methods = get_recommended_methods(attack.get("attack_type", ""))

            enriched = dict(attack)
            enriched["severity_score"] = severity_score
            enriched["confidence_score"] = confidence_score
            enriched["priority"] = priority
            enriched["recommended_round"] = recommended_round
            enriched["recommended_methods"] = recommended_methods

            scored_attacks.append(enriched)

        scored_attacks.sort(
            key=lambda x: (
                x.get("severity_score", 0),
                x.get("confidence_score", 0),
                len(x.get("accounts", []))
            ),
            reverse=True
        )

        result[bureau] = scored_attacks

    return result


# =========================
# STRATEGY ENGINE
# =========================

def get_laws_for_attack(attack_type: str) -> list[str]:
    mapping = {
        "duplicate_account_number": [
            "15 USC 1681e(b)",
            "15 USC 1681i(a)",
        ],
        "same_account_number_same_balance": [
            "15 USC 1681e(b)",
            "15 USC 1681i(a)",
            "15 USC 1681s-2(b)",
        ],
        "same_account_number_different_furnisher": [
            "15 USC 1681e(b)",
            "15 USC 1681i(a)",
            "15 USC 1681s-2(b)",
        ],
        "multi_furnisher_same_balance": [
            "15 USC 1681e(b)",
            "15 USC 1681i(a)",
            "15 USC 1681s-2(b)",
        ],
        "collector_original_creditor_pattern": [
            "15 USC 1681e(b)",
            "15 USC 1681i(a)",
            "15 USC 1681s-2(b)",
        ],
        "collector_original_creditor_self_declared": [
            "15 USC 1681e(b)",
            "15 USC 1681i(a)",
            "15 USC 1681s-2(b)",
        ],
        "late_collection_conflict": [
            "15 USC 1681e(b)",
            "15 USC 1681i(a)",
        ],
        "collection_late_payment_conflict": [
            "15 USC 1681e(b)",
            "15 USC 1681s-2(a)(1)",
            "15 USC 1681i(a)",
        ],
        "cross_bureau_balance_conflict": [
            "15 USC 1681e(b)",
            "15 USC 1681i(a)",
            "15 USC 1681s-2(b)",
        ],
        "cross_bureau_payment_status_conflict": [
            "15 USC 1681e(b)",
            "15 USC 1681i(a)",
            "15 USC 1681s-2(b)",
        ],
        "cross_bureau_furnisher_identity_shift": [
            "15 USC 1681e(b)",
            "15 USC 1681i(a)",
            "15 USC 1681s-2(b)",
        ],
        "cross_bureau_account_status_conflict": [
            "15 USC 1681e(b)",
            "15 USC 1681i(a)",
            "15 USC 1681s-2(b)",
        ],
        "obsolete_account_7yr_limit": [
            "15 USC 1681c(a)(4)",
            "15 USC 1681c(c)",
        ],
        "potential_re_aging": [
            "15 USC 1681c(c)",
            "15 USC 1681e(b)",
            "15 USC 1681s-2(a)(1)",
        ],
        "dofd_unknown_verification_required": [
            "15 USC 1681c(c)",
            "15 USC 1681i(a)(5)",
            "15 USC 1681e(b)",
            "15 USC 1681s-2(b)",
        ],
        "late_payment_history_dispute": [
        "15 USC 1681e(b)",
        "15 USC 1681s-2(a)(1)",
        "15 USC 1681i(a)",
    ],
    "cross_bureau_payment_history_date_conflict": [
        "15 USC 1681e(b)",
        "15 USC 1681s-2(a)(1)",
        "15 USC 1681i(a)",
    ],
    "absent_bureau_reporting_inconsistency": [
            "15 USC 1681e(b)",
            "15 USC 1681i(a)",
        ],
        # Student loan complex attacks
        "student_loan_duplicate_tradeline": [
            "15 USC 1681e(b)",
            "15 USC 1681s-2(a)(1)",
            "15 USC 1681i(a)",
        ],
        "student_loan_transferred_still_active": [
            "15 USC 1681e(b)",
            "15 USC 1681s-2(a)(1)",
        ],
        "student_loan_deferment_late_payment": [
            "15 USC 1681e(b)",
            "15 USC 1681s-2(a)(1)",
            "15 USC 1681s-2(b)",
        ],
        "student_loan_paid_still_reporting": [
            "15 USC 1681e(b)",
            "15 USC 1681s-2(a)(1)",
        ],
        "student_loan_discharged_still_active": [
            "15 USC 1681e(b)",
            "15 USC 1681s-2(a)(1)",
            "15 USC 1681c(a)(1)",
        ],
        "student_loan_default_inaccurate": [
            "15 USC 1681e(b)",
            "15 USC 1681s-2(a)(1)",
            "15 USC 1681i(a)",
        ],
        "student_loan_balance_inflated": [
            "15 USC 1681e(b)",
            "15 USC 1681s-2(a)(1)",
        ],
        # Reinsertion (comparison engine)
        "reinsertion_violation": [
            "15 USC 1681i(a)(5)(B)",
            "15 USC 1681n",
            "15 USC 1681e(b)",
        ],
        # Medical debt attacks
        "medical_debt_under_500": [
            "15 USC 1681e(b)",
            "15 USC 1681i(a)",
        ],
        "paid_medical_collection": [
            "15 USC 1681e(b)",
            "15 USC 1681i(a)",
            "15 USC 1681s-2(a)(1)",
        ],
        "medical_debt_premature": [
            "15 USC 1681e(b)",
            "15 USC 1681i(a)",
        ],
        "medical_debt_state_law": [
            "15 USC 1681e(b)",
            "15 USC 1681i(a)",
        ],
        "medical_debt_accuracy": [
            "15 USC 1681e(b)",
            "15 USC 1681i(a)",
        ],
        # Intra-account inconsistencies
        "opened_after_last_active": [
            "15 USC 1681e(b)",
        ],
        "past_due_exceeds_balance": [
            "15 USC 1681e(b)",
        ],
        "balance_exceeds_credit_limit": [
            "15 USC 1681e(b)",
        ],
        "balance_exceeds_high_credit": [
            "15 USC 1681e(b)",
        ],
        "open_status_chargeoff_conflict": [
            "15 USC 1681e(b)",
            "15 USC 1681s-2(a)(1)",
        ],
        "paid_status_with_past_due": [
            "15 USC 1681e(b)",
        ],
        "closed_with_balance": [
            "15 USC 1681e(b)",
        ],
        "current_payment_derogatory_status": [
            "15 USC 1681e(b)",
            "15 USC 1681s-2(a)(1)",
        ],
        "monthly_payment_on_collection": [
            "15 USC 1681e(b)",
        ],
        # Cross-bureau field conflicts
        "cross_bureau_date_opened_conflict": [
            "15 USC 1681e(b)",
        ],
        "cross_bureau_account_type_conflict": [
            "15 USC 1681e(b)",
        ],
        "cross_bureau_credit_limit_conflict": [
            "15 USC 1681e(b)",
        ],
        "cross_bureau_high_credit_conflict": [
            "15 USC 1681e(b)",
        ],
        # New account type attacks
        "child_support_derogatory": [
            "15 USC 1681s-1",
            "15 USC 1681e(b)",
            "15 USC 1681i(a)",
            "15 USC 1681s-2(b)",
        ],
        "student_loan_multiple_servicer": [
            "15 USC 1681e(b)",
            "15 USC 1681i(a)",
            "15 USC 1681s-2(b)",
        ],
        "student_loan_status_inaccurate": [
            "15 USC 1681e(b)",
            "15 USC 1681i(a)",
            "15 USC 1681s-2(b)",
        ],
        "bankruptcy_included_still_active": [
            "15 USC 1681c(a)(1)",
            "15 USC 1681e(b)",
            "11 USC 524",
        ],
        "bankruptcy_reporting_period_exceeded": [
            "15 USC 1681c(a)(1)",
            "15 USC 1681e(b)",
        ],
        "repossession_deficiency_unverified": [
            "15 USC 1681e(b)",
            "15 USC 1681i(a)",
            "15 USC 1681s-2(b)",
        ],
        "repossession_proceeds_not_credited": [
            "15 USC 1681e(b)",
            "15 USC 1681s-2(a)(1)",
        ],
        "charge_off_balance_inflated": [
            "15 USC 1681e(b)",
            "15 USC 1681s-2(a)(1)",
            "15 USC 1681s-2(b)",
        ],
        "paid_collection_still_derogatory": [
            "15 USC 1681e(b)",
            "15 USC 1681s-2(a)(1)",
            "15 USC 1681s-2(b)",
        ],
    }
    return mapping.get(attack_type, ["15 USC 1681e(b)", "15 USC 1681i(a)"])


def get_strategy_summary(attack: dict[str, Any]) -> str:
    attack_type = attack.get("attack_type", "")

    summaries = {
        "duplicate_account_number": (
            "The same account number is being reported in multiple separate tradelines, "
            "which raises a maximum-possible-accuracy issue and requires deletion if not fully verified."
        ),
        "same_account_number_same_balance": (
            "The same account number and the same balance are being reported across multiple tradelines, "
            "which creates a strong duplicate-reporting and ownership-verification problem."
        ),
        "same_account_number_different_furnisher": (
            "The same account number appears under different furnisher identities, "
            "which requires strict verification of reporting authority and ownership."
        ),
        "multi_furnisher_same_balance": (
            "Multiple separate negative tradelines are reporting the same balance, "
            "which creates a material inconsistency and requires full reinvestigation and ownership proof."
        ),
        "collector_original_creditor_pattern": (
            "A collector/original-creditor pattern is present around the same balance, "
            "which supports a chain-of-title challenge and furnisher-authority attack."
        ),
        "collector_original_creditor_self_declared": (
            "The furnisher's own name declares an original creditor, confirming a chain-of-title transfer. "
            "The collector must prove lawful assignment and reporting authority under 15 USC 1681s-2(b)."
        ),
        "late_collection_conflict": (
            "The tradeline uses late-payment language while also showing collection indicators, "
            "which is a classification conflict and an accuracy problem."
        ),
        "cross_bureau_balance_conflict": (
            "The same account reports different balances across credit bureaus. "
            "At least one bureau is receiving inaccurate data from the furnisher, "
            "which violates the maximum possible accuracy standard under 15 USC 1681e(b)."
        ),
        "cross_bureau_payment_status_conflict": (
            "The same account reports different payment statuses across credit bureaus. "
            "Inconsistent classification across bureaus is a material accuracy violation."
        ),
        "cross_bureau_furnisher_identity_shift": (
            "The same account appears under different furnisher names across credit bureaus. "
            "This furnisher identity shift requires verification of who holds the legal right to report."
        ),
        "cross_bureau_account_status_conflict": (
            "The same account reports different account statuses across credit bureaus "
            "(e.g. Closed on one bureau, Open or Derogatory on another). "
            "Inconsistent status classification is a material accuracy violation under 15 USC 1681e(b)."
        ),
        "obsolete_account_7yr_limit": (
            "This account has exceeded the FCRA 7-year maximum reporting period "
            "calculated from the Date of First Delinquency (DOFD) under 15 USC 1681c(c). "
            "Reporting an obsolete account is a violation of 15 USC 1681c(a)(4) and requires "
            "mandatory deletion. No reinvestigation is needed — the account is time-barred."
        ),
        "potential_re_aging": (
            "The collector's Date Opened is significantly later than the estimated DOFD, "
            "which suggests the collector may be using its own acquisition date as the DOFD "
            "to artificially extend the 7-year reporting period. This is a re-aging violation "
            "under 15 USC 1681c(c). The furnisher must provide documentation of the original "
            "DOFD from the original creditor."
        ),
        "dofd_unknown_verification_required": (
            "The Date of First Delinquency (DOFD) cannot be determined from the information "
            "reported. The FCRA 7-year reporting period under 15 USC 1681c(c) runs from the DOFD. "
            "Without a verifiable DOFD, neither the bureau nor the consumer can confirm the "
            "account is within the legal reporting window. If the furnisher cannot provide "
            "the DOFD, the account cannot be verified and must be deleted under 15 USC 1681i(a)(5)."
        ),
        "absent_bureau_reporting_inconsistency": (
            "A negative account appears in some bureaus but is absent from others. "
            "Selective bureau reporting creates a material inconsistency that must be explained by the furnisher."
        ),
    }

    return summaries.get(
        attack_type,
        "The tradeline presents material reporting inconsistencies requiring reinvestigation."
    )


def build_strategy_engine(attack_scoring_engine: dict[str, list[dict[str, Any]]]) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {}

    for bureau, attacks in attack_scoring_engine.items():
        strategies = []

        for attack in attacks:
            strategies.append({
                "attack_type": attack.get("attack_type", ""),
                "priority": attack.get("priority", ""),
                "recommended_round": attack.get("recommended_round", ""),
                "recommended_methods": attack.get("recommended_methods", []),
                "laws": get_laws_for_attack(attack.get("attack_type", "")),
                "strategy_summary": get_strategy_summary(attack),
                "accounts": attack.get("accounts", []),
            })

        result[bureau] = strategies

    return result


# =========================
# LETTER INPUT ENGINE
# =========================

def build_account_reason_from_strategy(strategy_item: dict[str, Any], account: dict[str, Any]) -> str:
    attack_type = strategy_item.get("attack_type", "")
    acct_num = account.get("account_number", "")
    furnisher = account.get("name", "")

    reasons = {
        "duplicate_account_number": (
            f"{furnisher} is reporting account {acct_num} in multiple separate tradelines, "
            "which creates a clear accuracy and reinvestigation problem."
        ),
        "same_account_number_same_balance": (
            f"{furnisher} is tied to account {acct_num}, which is being reported with the same balance across multiple tradelines, "
            "requiring deletion unless full ownership and reporting authority are proven."
        ),
        "same_account_number_different_furnisher": (
            f"{furnisher} is associated with account {acct_num}, but the same account number is also being reported under a different furnisher identity, "
            "which must be resolved through documented verification."
        ),
        "multi_furnisher_same_balance": (
            f"{furnisher} is reporting the same balance as another negative tradeline, "
            "which creates a multi-furnisher inconsistency requiring deletion if not fully verified."
        ),
        "collector_original_creditor_pattern": (
            f"{furnisher} appears in a collector/original-creditor reporting pattern, "
            "which requires chain of title and proof of lawful reporting authority."
        ),
        "collector_original_creditor_self_declared": (
            f"{furnisher} declares an original creditor directly in its own name on account {acct_num}. "
            "The reporting entity must prove the lawful chain of assignment and authority to report under 15 USC 1681s-2(b)."
        ),
        "late_collection_conflict": (
            f"{furnisher} is reporting late-payment language while the tradeline also reflects collection indicators, "
            "which is materially inconsistent and inaccurate."
        ),
        "cross_bureau_balance_conflict": (
            f"{furnisher} is reporting account {acct_num} with a different balance on this bureau than on others. "
            "The furnisher must report the same accurate balance to all bureaus."
        ),
        "cross_bureau_payment_status_conflict": (
            f"{furnisher} is reporting account {acct_num} with a different payment status on this bureau than on others. "
            "The inconsistent classification across bureaus is a material accuracy violation."
        ),
        "cross_bureau_furnisher_identity_shift": (
            f"Account {acct_num} appears under the name {furnisher} on this bureau but under a different identity on others. "
            "The furnisher must clarify its legal identity and authority to report."
        ),
        "cross_bureau_account_status_conflict": (
            f"{furnisher} reports account {acct_num} with a different account status on this bureau than on others. "
            "The furnisher must report a consistent and accurate account status to all bureaus."
        ),
        "obsolete_account_7yr_limit": (
            f"{furnisher} account {acct_num} has exceeded the FCRA maximum 7-year reporting period "
            f"under 15 USC 1681c(a)(4). This account is time-barred from appearing on any consumer "
            f"report and must be deleted immediately. No reinvestigation is required — "
            f"the statute mandates deletion."
        ),
        "potential_re_aging": (
            f"{furnisher} appears to have reset the reporting clock on account {acct_num} "
            f"by using its own acquisition date rather than the original Date of First Delinquency. "
            f"This re-aging practice violates 15 USC 1681c(c). The furnisher must provide the "
            f"original DOFD from the original creditor along with documentation of the chain of title."
        ),
        "dofd_unknown_verification_required": (
            f"{furnisher} account {acct_num} does not disclose a verifiable Date of First Delinquency. "
            f"Without the DOFD, it is impossible to confirm this account is within the FCRA 7-year "
            f"reporting window under 15 USC 1681c(c). The furnisher must provide the DOFD or the "
            f"account must be deleted as unverifiable under 15 USC 1681i(a)(5)."
        ),
        "absent_bureau_reporting_inconsistency": (
            f"{furnisher} account {acct_num} is reported as negative on this bureau but does not appear on all other bureaus. "
            "The furnisher must either report consistently to all bureaus or cease reporting."
        ),
    }

    return reasons.get(
        attack_type,
        f"{furnisher} is reporting materially inconsistent information on account {acct_num}."
    )


def get_attack_rank_for_letter_input(attack_type: str) -> int:
    rank = {
        "obsolete_account_7yr_limit":          101,
        "potential_re_aging":                   99,
        "bankruptcy_reporting_period_exceeded": 99,
        "same_account_number_same_balance":    100,
        "bankruptcy_included_still_active":     96,
        "collector_original_creditor_pattern":  95,
        "collector_original_creditor_self_declared": 94,
        "repossession_proceeds_not_credited":   92,
        "child_support_derogatory":             87,
        "student_loan_multiple_servicer":       91,
        "multi_furnisher_same_balance":         93,
        "same_account_number_different_furnisher": 92,
        "repossession_deficiency_unverified":   90,
        "duplicate_account_number":             90,
        "cross_bureau_balance_conflict":        89,
        "cross_bureau_furnisher_identity_shift": 88,
        "dofd_unknown_verification_required":   88,
        "charge_off_balance_inflated":          88,
        "cross_bureau_payment_status_conflict": 87,
        "paid_collection_still_derogatory":     86,
        "cross_bureau_account_status_conflict": 86,
        "student_loan_status_inaccurate":       85,
        "collection_late_payment_conflict":       92,
        "student_loan_discharged_still_active":  97,
        "student_loan_default_inaccurate":        96,
        "student_loan_duplicate_tradeline":       94,
        "student_loan_transferred_still_active":  92,
        "student_loan_deferment_late_payment":    91,
        "student_loan_balance_inflated":          88,
        "student_loan_paid_still_reporting":      86,
        "reinsertion_violation":         99,  # highest priority — willful violation
        "medical_debt_under_500":        98,  # bureau violating its own policy
        "paid_medical_collection":       97,
        "medical_debt_state_law":        96,
        "medical_debt_premature":        88,
        "medical_debt_accuracy":         72,
        "past_due_exceeds_balance":             95,
        "open_status_chargeoff_conflict":        93,
        "paid_status_with_past_due":             93,
        "current_payment_derogatory_status":     91,
        "opened_after_last_active":              90,
        "balance_exceeds_high_credit":           89,
        "balance_exceeds_credit_limit":          86,
        "closed_with_balance":                   82,
        "monthly_payment_on_collection":         80,
        "zero_balance_with_past_due":            82,
        "cross_bureau_date_opened_conflict":     84,
        "cross_bureau_account_type_conflict":    83,
        "cross_bureau_credit_limit_conflict":    82,
        "cross_bureau_high_credit_conflict":     81,
        "collection_late_payment_conflict":         92,
        "late_collection_conflict":             85,
        "absent_bureau_reporting_inconsistency": 80,
        "requires_basic_verification":          40,
    }
    return rank.get(attack_type, 50)


def build_letter_input_engine(
    strategy_engine: dict[str, list[dict[str, Any]]],
    negatives_by_bureau: dict[str, list[dict[str, Any]]] | None = None,
) -> dict[str, dict[str, list[dict[str, Any]]]]:
    """
    Build the final letter input structure per bureau.

    For each bureau:
    - Pulls all attacked accounts from the strategy engine.
    - Also includes any negative account that has NO attack detected yet,
      flagging it as 'requires_basic_verification' (round 1 — do not assume
      anything, just demand the bureau verify the data under 15 USC 1681i).
    - Groups by negative type: collections_chargeoffs | late_payments | other_derogatory.
    - Deduplicates per (furnisher_name, account_number), keeping highest-ranked attack.
    """
    result: dict[str, dict[str, list[dict[str, Any]]]] = {}

    for bureau, strategies in strategy_engine.items():
        grouped: dict[str, list[dict[str, Any]]] = {
            "collections_chargeoffs": [],
            "late_payments": [],
            "other_derogatory": [],
        }

        dedupe_map: dict[tuple[str, str], dict[str, Any]] = {}

        # --- Pass 1: accounts that have an identified attack ---
        for strategy_item in strategies:
            attack_type = strategy_item.get("attack_type", "")
            laws = strategy_item.get("laws", [])
            recommended_round = strategy_item.get("recommended_round", "")
            methods = strategy_item.get("recommended_methods", [])
            attack_rank = get_attack_rank_for_letter_input(attack_type)

            for account in strategy_item.get("accounts", []):
                entry = {
                    "furnisher_name":        account.get("name", ""),
                    "account_number":        account.get("account_number", ""),
                    "masked_account_number": mask_stars_to_x(account.get("account_number", "")),
                    "negative_type":         account.get("negative_type", ""),
                    "attack_type":           attack_type,
                    "laws":                  laws,
                    "recommended_round":     recommended_round,
                    "recommended_methods":   methods,
                    "reason":                build_account_reason_from_strategy(strategy_item, account),
                    "attack_rank":           attack_rank,
                    # DOFD context — always carried through for letter generation
                    "dofd_estimated":            account.get("dofd_estimated"),
                    "dofd_confidence":           account.get("dofd_confidence", "unknown"),
                    "fcra_expiration":           account.get("fcra_expiration"),
                    "days_until_expiration":     account.get("days_until_expiration"),
                    "is_obsolete":               account.get("is_obsolete", False),
                    "re_aging_flag":             account.get("re_aging_flag", False),
                    "re_aging_gap_days":         account.get("re_aging_gap_days"),
                    "dofd_verification_required": account.get("dofd_verification_required", False),
                    "dla_suspected_refresh":     account.get("dla_suspected_refresh", False),
                    # Date fields for reference
                    "date_of_last_payment":      account.get("date_of_last_payment", ""),
                    "date_last_active":          account.get("date_last_active", ""),
                    "date_opened":               account.get("date_opened", ""),
                    "last_reported":             account.get("last_reported", ""),
                }
                key = (entry["furnisher_name"], entry["account_number"])
                existing = dedupe_map.get(key)
                if existing is None or entry["attack_rank"] > existing["attack_rank"]:
                    dedupe_map[key] = entry

        # --- Pass 2: negative accounts with NO attack (basic verification) ---
        if negatives_by_bureau is not None:
            for acc in negatives_by_bureau.get(bureau, []):
                key = (normalize_spaces(acc.get("name", "")), normalize_spaces(acc.get("account_number", "")))
                if key not in dedupe_map:
                    negative_type = acc.get("negative_type", "derogatory")
                    furnisher = acc.get("name", "")
                    acct_num = acc.get("account_number", "")
                    entry = {
                        "furnisher_name":        furnisher,
                        "account_number":        acct_num,
                        "masked_account_number": mask_stars_to_x(acct_num),
                        "negative_type":         negative_type,
                        "attack_type":           "requires_basic_verification",
                        "laws":                  ["15 USC 1681i(a)", "15 USC 1681e(b)"],
                        "recommended_round":     "round_1",
                        "recommended_methods":   ["bureau_dispute", "direct_furnisher_dispute"],
                        "reason": (
                            f"{furnisher} is reporting a negative item (account {acct_num}) "
                            "that has not been independently verified. Under 15 USC 1681i(a), "
                            "the bureau must conduct a reasonable reinvestigation of this item."
                        ),
                        "attack_rank":               40,
                        "dofd_estimated":            acc.get("dofd_estimated"),
                        "dofd_confidence":           acc.get("dofd_confidence", "unknown"),
                        "fcra_expiration":           acc.get("fcra_expiration"),
                        "days_until_expiration":     acc.get("days_until_expiration"),
                        "is_obsolete":               acc.get("is_obsolete", False),
                        "re_aging_flag":             acc.get("re_aging_flag", False),
                        "re_aging_gap_days":         acc.get("re_aging_gap_days"),
                        "dofd_verification_required": acc.get("dofd_verification_required", False),
                        "dla_suspected_refresh":     acc.get("dla_suspected_refresh", False),
                        "date_of_last_payment":      acc.get("date_of_last_payment", ""),
                        "date_last_active":          acc.get("date_last_active", ""),
                        "date_opened":               acc.get("date_opened", ""),
                        "last_reported":             acc.get("last_reported", ""),
                    }
                    dedupe_map[key] = entry

        # --- Finalize and group ---
        for entry in dedupe_map.values():
            entry.pop("attack_rank", None)
            negative_type = entry.get("negative_type", "")

            if negative_type in {"collection", "charge_off"}:
                grouped["collections_chargeoffs"].append(entry)
            elif negative_type == "late_payment":
                grouped["late_payments"].append(entry)
            else:
                grouped["other_derogatory"].append(entry)

        for group_name in grouped:
            grouped[group_name].sort(
                key=lambda x: (x.get("furnisher_name", ""), x.get("account_number", ""))
            )

        result[bureau] = grouped

    return result





# =========================
# LETTER GENERATION ENGINE
# =========================

BUREAU_ADDRESSES = {
    "transunion": {
        "name": "TransUnion",
        "address": "PO Box 2000\nChester, PA 19016",
    },
    "experian": {
        "name": "Experian",
        "address": "P.O. Box 4500\nAllen, TX 75013",
    },
    "equifax": {
        "name": "Equifax Information Services",
        "address": "P.O. Box 740256\nAtlanta, GA 30374",
    },
}


def _format_date_long(report_date_str: str) -> str:
    """Returns date as 'January 7, 2026' — how a person writes it."""
    from datetime import datetime as _dt
    d = parse_date_field(report_date_str)
    if d:
        return d.strftime("%B %d, %Y").replace(" 0", " ")
    return _dt.today().strftime("%B %d, %Y").replace(" 0", " ")


import random as _random

# Four structurally distinct opening paragraphs.
# Same legal content, different sentence order and wording.
# The system picks one per letter so no two letters are identical.
_OPENING_TEMPLATES_R1 = [
    # Version A — starts with the personal situation
    (
        "Hi,\n\n"
        "I recently went through my credit report and found {count} that I do not "
        "believe {they_verb} being reported correctly. I am writing to formally dispute "
        "{these_items} and to ask that you reinvestigate them. The Fair Credit "
        "Reporting Act gives me the right to dispute inaccurate or unverifiable "
        "information, and it requires you to complete that investigation within "
        "30 days (15 U.S.C. \u00a71681i). It also requires that any information "
        "you report be as accurate as possible (15 U.S.C. \u00a71681e(b))."
        "\n\n"
        "For each account I am disputing, I am asking that you contact the company "
        "reporting it and require them to provide actual proof — the original "
        "agreement, a full payment history, a breakdown of the balance, the exact "
        "date I first fell behind, and where the account was transferred or sold, "
        "documentation of that transfer. If you cannot get that proof and verify "
        "the account fully, it needs to come off my report. I am also asking that "
        "you send me the results of your investigation in writing, including which "
        "companies you contacted and what they provided (15 U.S.C. \u00a71681i(a))."
    ),
    # Version B — starts with the legal right, then gets personal
    (
        "Hi,\n\n"
        "I am writing to dispute {count} on my credit report. Under the Fair Credit "
        "Reporting Act — specifically 15 U.S.C. \u00a71681i — I have the right to "
        "ask you to reinvestigate information I believe is inaccurate or cannot be "
        "verified. I am exercising that right now regarding {these_items} listed below."
        "\n\n"
        "I am asking that for each account, you require the company reporting it to "
        "show real documentation: the original signed agreement, complete billing and "
        "payment records, an itemized breakdown of what is owed, the date I first "
        "missed a payment, and — if the account was sold to a collector — proof of "
        "that transfer. If any account cannot be fully backed up with documentation, "
        "it must be deleted under 15 U.S.C. \u00a71681i(a)(5). The law also requires "
        "that you follow reasonable procedures to make sure what you report is "
        "accurate (15 U.S.C. \u00a71681e(b)). Please send me your results in writing "
        "when the investigation is complete."
    ),
    # Version C — conversational, verification-first
    (
        "Hi,\n\n"
        "I looked over my credit report and I have questions about {count}. "
        "I do not believe {they_verb} being reported correctly, and I am asking "
        "that you look into {these_items}."
        "\n\n"
        "The Fair Credit Reporting Act (15 U.S.C. \u00a71681i and \u00a71681e(b)) "
        "gives me the right to request this and requires you to complete it within "
        "30 days. What I am asking for specifically is that you go back to each "
        "company reporting these accounts and ask them for documentation — not just "
        "a yes or no answer that the information is correct. I want to see that they "
        "can provide the original agreement, a full history of payments, an "
        "explanation of the balance, the date I first went delinquent, and proof "
        "of ownership if the account changed hands. Anything they cannot verify "
        "with actual records needs to be removed. Please also let me know in writing "
        "what you found and who you contacted."
    ),
    # Version D — brief opening, weight on the account list
    (
        "Hi,\n\n"
        "I am disputing {count} on my credit report that I believe "
        "{they_verb} not accurate or cannot be verified. I am asking that you "
        "reinvestigate {these_items} under 15 U.S.C. \u00a71681i."
        "\n\n"
        "For each account below, I need you to require the reporting company to "
        "provide full documentation — original agreement, complete payment records, "
        "itemized balance, the exact date I first fell behind, and assignment "
        "records if the debt changed hands. If they cannot produce that, the "
        "account is not verifiable and must be deleted under 15 U.S.C. \u00a71681i(a)(5). "
        "You are also required to follow reasonable accuracy procedures under "
        "15 U.S.C. \u00a71681e(b). Please provide written results including the "
        "name and contact information of anyone you reached out to."
    ),
    # Version E — focuses on accuracy obligation, good for other_derogatory group
    (
        "Hi,\n\n"
        "I am writing to formally dispute {these_items} on my credit report. "
        "The Fair Credit Reporting Act requires that every piece of information "
        "on my credit file be accurate, complete, and verifiable "
        "(15 U.S.C. \u00a71681e(b)). The accounts listed below do not meet "
        "that standard."
        "\n\n"
        "I am asking that you reinvestigate {these_items} within 30 days as "
        "required by 15 U.S.C. \u00a71681i(a). For each account, please require "
        "the furnisher to provide documentation that fully supports every field "
        "being reported — including the original agreement, complete payment history, "
        "exact balance breakdown, and the date I first fell behind. If any account "
        "cannot be verified with actual documentation, it must be corrected or "
        "deleted under 15 U.S.C. \u00a71681i(a)(5). Please send me written results "
        "including who you contacted and what they provided."
    ),
    # Version F — focuses on deletion demand, strong closer
    (
        "Hi,\n\n"
        "I recently reviewed my credit report and found {these_items} that "
        "{they_verb} being reported inaccurately. I am disputing {these_items} "
        "under the Fair Credit Reporting Act and asking that you investigate "
        "and remove anything that cannot be fully verified."
        "\n\n"
        "Under 15 U.S.C. \u00a71681i(a), you must complete this reinvestigation "
        "within 30 days. Under 15 U.S.C. \u00a71681i(a)(5), any item that "
        "cannot be verified must be deleted. Under 15 U.S.C. \u00a71681e(b), "
        "you are required to maintain maximum possible accuracy on all "
        "information in my file. I am asking that the companies reporting "
        "these accounts provide complete documentation — original agreements, "
        "full payment records, and any assignment records if the debt was sold "
        "or transferred. Please send me your written investigation results."
    ),
]

_OPENING_TEMPLATES_R2 = [
    # Version A
    (
        "Hi,\n\n"
        "I disputed {count} on my credit report previously and I am not satisfied "
        "with the outcome. I am writing again to push for a real investigation — "
        "not just a form response that the information was verified."
        "\n\n"
        "Under 15 U.S.C. \u00a71681i(a), the law requires an actual reasonable "
        "reinvestigation, which means going back to the reporting company and "
        "reviewing documentation — not just sending an automated inquiry and "
        "accepting whatever answer comes back. I am specifically requesting, under "
        "15 U.S.C. \u00a71681i(a)(6)(B)(iii), that you tell me the procedure you "
        "used, the name and contact information of every company you reached out to, "
        "and what documentation you relied on. I am also noting that continuing to "
        "report information that cannot be verified, after a properly submitted "
        "dispute, can create liability under 15 U.S.C. \u00a71681n. "
        "I am keeping all records."
    ),
    # Version B
    (
        "Hi,\n\n"
        "I am following up on a dispute I submitted earlier about {count} "
        "on my credit file. I did not feel the prior investigation was thorough "
        "enough, so I am asking that you take another look — this time with "
        "actual documentation from the reporting companies."
        "\n\n"
        "I know under 15 U.S.C. \u00a71681i(a) that a reasonable reinvestigation "
        "is required and that just confirming the data with the furnisher is not "
        "enough. I want real records reviewed. Under 15 U.S.C. \u00a71681i(a)(6)(B)(iii) "
        "I am asking that you provide me with a written description of your "
        "investigation process and the contact information for every company you "
        "reached out to. If any of these accounts cannot be verified with real "
        "documentation, they need to be removed. I want you to know I am keeping "
        "a record of all correspondence in case I need to pursue this further "
        "under 15 U.S.C. \u00a71681n."
    ),
    # Version C
    (
        "Hi,\n\n"
        "I previously disputed some accounts on my credit report and I am "
        "writing back because the issues were not resolved to my satisfaction. "
        "I am asking you to take a closer look at {count}."
        "\n\n"
        "What I need is a real investigation — not an automated check where "
        "someone at the reporting company clicks confirm and nothing gets reviewed. "
        "The Fair Credit Reporting Act (15 U.S.C. \u00a71681i(a)) requires a "
        "reasonable reinvestigation, and I expect that standard to be met. "
        "I am also requesting under 15 U.S.C. \u00a71681i(a)(6)(B)(iii) that "
        "you send me a description of exactly how each account was investigated, "
        "including who was contacted and what they provided. Any account that "
        "cannot be fully verified must be deleted. I am aware of the remedies "
        "available to me under 15 U.S.C. \u00a71681n if unverifiable information "
        "continues to be reported."
    ),
]


# Fixed template index per bureau+round — guarantees no two letters share an opening.
_TEMPLATE_INDEX = {
    ("transunion", "round_1"): 0,
    ("experian",   "round_1"): 1,
    ("equifax",    "round_1"): 2,
    ("transunion", "round_2"): 0,
    ("experian",   "round_2"): 1,
    ("equifax",    "round_2"): 2,
}


def _pick_opening(templates: list, items: list, bureau: str, round_key: str) -> str:
    """Pick a template by fixed index (no randomness — guaranteed unique per bureau+round)."""
    idx = _TEMPLATE_INDEX.get((bureau, round_key), 0) % len(templates)
    tpl = templates[idx]
    n           = len(items)
    count       = f"{n} account{'s' if n != 1 else ''}"
    verb        = "are" if n != 1 else "is"
    they_verb   = "they are" if n != 1 else "it is"
    these_items = "these accounts" if n != 1 else "this account"
    return tpl.format(count=count, verb=verb, they_verb=they_verb, these_items=these_items)


# Reason variation seeds — ensures same attack type gets slightly different
# phrasing when it appears more than once in the same letter.
_VARIATION_OPENERS = [
    "Looking at this account, ",
    "When I reviewed this entry, I noticed ",
    "On this account, ",
    "Regarding this entry — ",
    "For this account, ",
]

_VARIATION_CLOSERS_BASIC = [
    "If they cannot provide that, this account must be removed.",
    "Without those records, this account cannot be verified and needs to come off.",
    "If that documentation does not exist, this account should not be on my report.",
    "Any part they cannot back up with real records needs to be deleted.",
]


def _account_reason(item: dict[str, Any], variation_idx: int = 0) -> str:
    """
    Specific reason paragraph for one account.
    variation_idx drives subtle wording differences so identical attack types
    don't produce identical text within the same letter.
    """
    furnisher   = item.get("furnisher_name", "")
    attack_type = item.get("attack_type", "")
    neg_type    = item.get("negative_type", "")
    dofd        = item.get("dofd_estimated")
    fcra_exp    = item.get("fcra_expiration")
    confidence  = item.get("dofd_confidence", "unknown")
    dla_refresh = item.get("dla_suspected_refresh", False)
    balance     = item.get("balance", "")
    acct        = item.get("account_number", "")

    vi = variation_idx % len(_VARIATION_OPENERS)
    vc = variation_idx % len(_VARIATION_CLOSERS_BASIC)
    opener  = _VARIATION_OPENERS[vi]
    v_close = _VARIATION_CLOSERS_BASIC[vc]

    if attack_type == "obsolete_account_7yr_limit":
        reason = (
            f"This account has been on my report too long. Based on when I first "
            f"fell behind — around {dofd} — the Fair Credit Reporting Act "
            f"(15 U.S.C. \u00a71681c(a)(4)) only allows this type of account to "
            f"stay on a credit report for seven years from 180 days after that date. "
            f"By that math, this should have come off around {fcra_exp}. "
            f"It needs to be deleted."
        )

    elif neg_type == "child_support":
        reason = (
            f"I am disputing the accuracy of this child/family support account. "
            f"Under 15 U.S.C. \u00a71681s-1, a child support agency may only report "
            f"overdue support obligations, and only the delinquent portion as "
            f"certified by the state. The reported balance of {balance} and "
            f"past due amount of {item.get('past_due', '')} must be verified with "
            f"a current certification from the state child support agency "
            f"confirming the exact delinquent amount. If the amount reported "
            f"includes current support that is not yet overdue, or if the "
            f"certification is outdated, this must be corrected or removed."
        )

    elif neg_type == "student_loan":
        reason = (
            f"I am disputing the accuracy of this student loan account. "
            f"Student loan servicers frequently change, and accounts are often "
            f"reported by multiple servicers simultaneously for the same underlying "
            f"loan — which is inaccurate. I am asking that {furnisher} provide: "
            f"(1) proof that they are the current authorized servicer, "
            f"(2) the complete payment history from loan origination, "
            f"(3) the correct status reflecting any deferment, forbearance, "
            f"or income-driven repayment plan, and (4) confirmation that this "
            f"loan has not been reported by another servicer for the same period. "
            f"If this cannot be verified, it must be removed."
        )

    elif neg_type == "bankruptcy":
        reason = (
            f"I am disputing the accuracy of how this account is being reported "
            f"in connection with a bankruptcy proceeding. Under the Fair Credit "
            f"Reporting Act, Chapter 7 bankruptcies may be reported for up to "
            f"10 years and Chapter 13 for up to 7 years from the filing date "
            f"(15 U.S.C. \u00a71681c(a)(1)). Accounts included in a bankruptcy "
            f"must reflect their discharged status and may not continue to show "
            f"an active balance or derogatory payment history after discharge. "
            f"I am asking that {furnisher} confirm the correct bankruptcy chapter, "
            f"filing date, discharge date, and that the account status accurately "
            f"reflects the discharge."
        )

    elif neg_type == "repossession":
        reason = (
            f"I am disputing the accuracy of this repossession account. "
            f"When a creditor repossesses and sells a vehicle, they are required "
            f"under the Uniform Commercial Code (UCC Article 9) to apply the "
            f"net proceeds of the sale to the outstanding balance and notify "
            f"the consumer of the sale. The deficiency balance — if any — "
            f"may only reflect the remaining amount after the sale proceeds "
            f"are properly credited. I am asking that {furnisher} provide: "
            f"(1) documentation of the repossession, (2) proof that the vehicle "
            f"was sold and the sale price, (3) an itemized accounting showing "
            f"how the sale proceeds were applied, and (4) confirmation that any "
            f"reported balance reflects only the legitimate deficiency. "
            f"If this cannot be verified, the account must be removed."
        )

    elif neg_type == "charge_off_deficiency":
        reason = (
            f"I am disputing the balance reported for this charged-off account. "
            f"When an account is charged off, the original creditor writes it off "
            f"as a loss — however, the balance being reported must reflect the "
            f"actual amount owed, not an inflated figure that includes fees or "
            f"interest added after the charge-off date. I am asking that "
            f"{furnisher} provide a complete itemized accounting of this balance "
            f"showing the original charged-off amount, any interest or fees added "
            f"after charge-off, and any payments made. The reported balance of "
            f"{balance} must be verifiable with primary documentation. "
            f"If it cannot be, this account must be corrected or removed."
        )

    elif neg_type == "paid_collection":
        reason = (
            f"This account shows a balance of zero and has been paid or settled, "
            f"yet it continues to be reported with a derogatory status. "
            f"A paid or settled collection account must accurately reflect its "
            f"current status. Under 15 U.S.C. \u00a71681e(b), reporting a "
            f"derogatory classification on an account that has been resolved "
            f"is inaccurate. I am asking that {furnisher} update the status "
            f"to accurately reflect that this account has been paid or settled, "
            f"and confirm the correct Date of First Delinquency for the 7-year "
            f"reporting clock. If the current reporting is inaccurate, it must "
            f"be corrected immediately."
        )

    elif attack_type == "potential_re_aging":
        v = variation_idx % 4
        if v == 0:
            reason = (
                f"I noticed that the date this company shows as when the account "
                f"started is much later than when I actually stopped paying — "
                f"which was around {dofd}. The seven-year clock under the Fair "
                f"Credit Reporting Act runs from when I first missed a payment, "
                f"not from when a collector picked up the account. If they are "
                f"using their own date, that pushes the expiration further than "
                f"the law allows. This account should have expired around {fcra_exp}. "
                f"I am asking that the original date of first delinquency be "
                f"confirmed with the original creditor."
            )
        elif v == 1:
            reason = (
                f"Something does not add up with the dates on this account. "
                f"My last payment to the original creditor was around {dofd}, "
                f"but the date this company is showing is considerably later — "
                f"which looks like their acquisition date, not my actual delinquency date. "
                f"Under 15 U.S.C. \u00a71681c(c), the reporting period starts from "
                f"my original date of first delinquency. Using a later date would "
                f"extend the account past its legal expiration of around {fcra_exp}. "
                f"I need the correct date verified."
            )
        elif v == 2:
            reason = (
                f"The date being reported for this account does not match when I "
                f"actually fell behind on the original obligation. My records indicate "
                f"the delinquency started around {dofd}, which means the FCRA "
                f"seven-year window would close around {fcra_exp}. I am requesting "
                f"that this company document the original date of first delinquency "
                f"with records from the original creditor — not their own opening date."
            )
        else:
            reason = (
                f"This account appears to be using a date that extends the "
                f"reporting period past what the law allows. The original "
                f"delinquency was around {dofd}, which gives an FCRA expiration "
                f"of approximately {fcra_exp}. If the company is reporting a more "
                f"recent date as the start of delinquency, that is re-aging — "
                f"a violation of 15 U.S.C. \u00a71681c(c). I am requesting "
                f"documentation of the original date of first delinquency."
            )

    elif attack_type == "dofd_unknown_verification_required":
        v = variation_idx % 4
        if dla_refresh:
            if v < 2:
                reason = (
                    f"The 'date last active' on this account matches almost exactly "
                    f"when it was last reported to the bureau. That looks like the "
                    f"company is refreshing that date to make the account appear more "
                    f"current than it actually is. The seven-year reporting period "
                    f"has to be measured from when I first missed a payment — not from "
                    f"the last time they updated the record. I am asking that "
                    f"{furnisher} show the original date of first delinquency with "
                    f"backup documentation. If they cannot do that, this account "
                    f"cannot be verified."
                )
            else:
                reason = (
                    f"The date being used on this account appears to reflect when "
                    f"{furnisher} last updated the record, not when I actually fell "
                    f"behind. That distinction matters — 15 U.S.C. \u00a71681c(c) "
                    f"requires the reporting window to run from my original date of "
                    f"first delinquency, not from a furnisher refresh date. I am "
                    f"requesting the original delinquency date with primary "
                    f"documentation from the original creditor."
                )
        else:
            if v < 2:
                reason = (
                    f"I cannot determine from what is reported when I actually first "
                    f"fell behind on this account. That date matters because it controls "
                    f"how long this can legally stay on my report under 15 U.S.C. "
                    f"\u00a71681c(c). I am asking that {furnisher} disclose the "
                    f"original date of first delinquency with supporting records. "
                    f"If they cannot provide it, the account cannot be verified."
                )
            else:
                reason = (
                    f"The date of first delinquency for this account is not clearly "
                    f"disclosed. Without it, I cannot confirm that this account is "
                    f"within its legal seven-year reporting window under the FCRA. "
                    f"I am asking that {furnisher} provide the original date from "
                    f"the original creditor's records. If that date cannot be "
                    f"established, this account cannot be reported."
                )

    elif attack_type in {
        "collector_original_creditor_self_declared",
        "collector_original_creditor_pattern",
    }:
        if variation_idx % 2 == 0:
            reason = (
                f"This is being reported by a collection company and I want to "
                f"know whether they actually have the right to put this on my "
                f"credit report. I am asking that {furnisher} provide the "
                f"original signed agreement, a complete record showing how this "
                f"account was transferred from the original creditor to them, "
                f"and documentation proving their legal authority to report it. "
                f"If they cannot show all of that, this account cannot be verified."
            )
        else:
            reason = (
                f"I dispute whether {furnisher} has the authority to report "
                f"this account on my credit file. For a collection account, "
                f"that means producing the original contract, proof of the "
                f"full chain of assignment from the original creditor, and "
                f"documentation of their right to report. Without all of "
                f"that in hand, this cannot be considered verified."
            )

    elif attack_type in {
        "same_account_number_same_balance",
        "duplicate_account_number",
    }:
        bal_note = f" with a balance of {balance}" if balance and balance not in {"$0.00","0","0.0"} else ""
        v = variation_idx % 6
        if v == 0:
            reason = (
                f"This account number{bal_note} is showing up more than once on "
                f"my report under different company names. As far as I know this "
                f"is one debt, not two separate ones. Listing it twice makes my "
                f"report look worse than it actually is. I am asking that you "
                f"determine whether these entries are the same obligation and "
                f"remove whichever one cannot be verified as a separate distinct debt."
            )
        elif v == 1:
            reason = (
                f"I see this same account{bal_note} appearing under more than one "
                f"name on my report. That does not seem right — if it is the "
                f"same debt it should only appear once, under whoever currently "
                f"holds it. I am asking that duplicate entries be investigated "
                f"and any that cannot be shown to be a separate obligation "
                f"be deleted."
            )
        elif v == 2:
            reason = (
                f"The account number{bal_note} tied to this entry matches another "
                f"entry already on my report. A single obligation cannot create "
                f"two valid negative tradelines simultaneously. I am asking that "
                f"you identify which reporting entity holds the legal right to "
                f"report this account and remove the one that cannot prove it."
            )
        elif v == 3:
            reason = (
                f"This account{bal_note} is being reported by more than one "
                f"company under the same account number. Only one entity can "
                f"legally own and report the same debt at any given time. "
                f"I am requesting that each reporting party provide proof of "
                f"ownership and authority, and that any entry which cannot be "
                f"independently verified be removed from my report."
            )
        elif v == 4:
            reason = (
                f"There are multiple entries on my credit report that appear to "
                f"refer to the same debt{bal_note}. The presence of duplicate "
                f"tradelines for a single obligation inflates the negative impact "
                f"on my report. I am requesting that this account be verified "
                f"as a distinct and separate obligation — if it cannot be, "
                f"this entry must be deleted."
            )
        else:
            reason = (
                f"This appears to be the same account{bal_note} already reported "
                f"elsewhere on my credit file under a different company name. "
                f"Reporting the same debt twice is inaccurate under 15 U.S.C. "
                f"\u00a71681e(b). I am asking that this entry be verified as "
                f"a legally distinct obligation, and removed if it is a "
                f"duplicate of another tradeline."
            )

    elif attack_type == "multi_furnisher_same_balance":
        reason = (
            f"{opener}multiple companies are reporting what looks like "
            f"the same amount. If this is one debt, only one company should "
            f"be reporting it — whoever actually holds it right now. "
            f"I am asking that each company show independent proof of ownership "
            f"and their right to report. {v_close}"
        )

    elif attack_type == "cross_bureau_balance_conflict":
        reason = (
            f"This account shows a different balance depending on which "
            f"bureau you look at. One of those numbers has to be wrong — "
            f"the balance cannot be two things at once. I am asking that "
            f"the correct balance be confirmed and the wrong one corrected."
        )

    elif attack_type == "cross_bureau_payment_status_conflict":
        v = variation_idx % 3
        if v == 0:
            reason = (
                f"The payment status on this account is different depending on "
                f"which bureau is reporting it. That does not make sense — the "
                f"status should be the same everywhere. I am asking that the "
                f"accurate payment status be determined and reported consistently."
            )
        elif v == 1:
            reason = (
                f"This account is being reported with a different payment status "
                f"at each bureau. The bureaus are showing conflicting information "
                f"about the same account, which cannot all be correct. I am "
                f"asking that the accurate status be verified and that all three "
                f"bureaus reflect the same correct information."
            )
        else:
            reason = (
                f"The payment classification for this account varies across the "
                f"bureaus. An account cannot have multiple payment statuses "
                f"simultaneously — only one can be accurate. I am disputing "
                f"whichever reporting is incorrect and requesting that the "
                f"accurate status be applied consistently."
            )

    elif attack_type == "cross_bureau_account_status_conflict":
        reason = (
            f"This account has a different status — open, closed, or derogatory "
            f"— depending on which bureau you check. One of them is wrong. "
            f"I am asking that the correct status be confirmed and the "
            f"discrepancy fixed."
        )

    elif attack_type == "opened_after_last_active":
        reason = (
            f"This account shows an open date of {item.get('date_opened','')} "
            f"but a date last active of {item.get('date_last_active','')} — "
            f"which is earlier than when it was supposedly opened. An account "
            f"cannot have activity before it existed. This chronological "
            f"impossibility indicates that at least one of these dates is "
            f"being reported inaccurately."
        )

    elif attack_type == "past_due_exceeds_balance":
        reason = (
            f"This account reports a past-due amount of "
            f"{item.get('past_due','')} on a total balance of only "
            f"{item.get('balance','')}. The past-due amount cannot exceed the "
            f"total balance — you cannot owe more past-due than the full "
            f"outstanding debt. This is a mathematical impossibility and "
            f"indicates that either the balance or the past-due figure is wrong."
        )

    elif attack_type == "balance_exceeds_credit_limit":
        reason = (
            f"This account shows a balance of {item.get('balance','')} that "
            f"substantially exceeds the reported credit limit of "
            f"{item.get('credit_limit','')}. While fees and interest can push a "
            f"balance slightly over limit, the extent of this overage indicates "
            f"that either the balance or the credit limit is being reported "
            f"inaccurately. I am requesting verification of both figures."
        )

    elif attack_type == "balance_exceeds_high_credit":
        reason = (
            f"This installment account shows a current balance of "
            f"{item.get('balance','')} that exceeds the original loan amount "
            f"of {item.get('high_credit','')}. On an installment loan, "
            f"the balance can only decrease over time as payments are made — "
            f"it cannot grow beyond the original principal. This indicates "
            f"a reporting error in either the balance or the original amount."
        )

    elif attack_type == "open_status_chargeoff_conflict":
        reason = (
            f"This account is reported as 'Open' in its account status, "
            f"but the payment status is '{item.get('payment_status','')}'. "
            f"An account that has been charged off or placed in collection "
            f"is by definition no longer open — it was closed when it was "
            f"charged off. Reporting it as both open and charged-off is "
            f"contradictory and inaccurate."
        )

    elif attack_type == "paid_status_with_past_due":
        reason = (
            f"This account has a status of 'Paid' but is also reporting "
            f"a past-due amount of {item.get('past_due','')}. An account "
            f"that has been paid cannot simultaneously have an outstanding "
            f"past-due balance. These two data points directly contradict "
            f"each other and indicate an error in the reporting."
        )

    elif attack_type == "closed_with_balance":
        reason = (
            f"This account shows a status of 'Closed' but is reporting "
            f"a balance of {item.get('balance','')}. A closed account that "
            f"is not in collection or charged off should reflect a zero "
            f"balance — the account was closed, meaning the relationship "
            f"with the creditor ended. I am asking that this discrepancy "
            f"be investigated and the balance be corrected."
        )

    elif attack_type == "current_payment_derogatory_status":
        reason = (
            f"This account shows a payment status of 'Current' — meaning "
            f"no payment is overdue — but an account status of 'Derogatory'. "
            f"If all payments are being made on time, the account cannot be "
            f"classified as derogatory. These two classifications directly "
            f"contradict each other. I am asking that the accurate status "
            f"be determined and the incorrect one corrected or removed."
        )

    elif attack_type == "monthly_payment_on_collection":
        reason = (
            f"This collection account is reporting a monthly payment of "
            f"{item.get('monthly_payment','')}. Collection accounts do not "
            f"have an ongoing payment schedule — the original creditor "
            f"relationship has ended and the debt has been transferred. "
            f"There is no creditor expecting monthly payments, making "
            f"this field inaccurate."
        )

    elif attack_type == "cross_bureau_date_opened_conflict":
        reason = (
            f"The date this account was opened is being reported differently "
            f"across bureaus. The date an account was opened is a historical "
            f"fact set by the original creditor — it cannot legitimately vary "
            f"by bureau. I am asking that the correct opening date be verified "
            f"and reported consistently."
        )

    elif attack_type == "cross_bureau_account_type_conflict":
        reason = (
            f"This account is classified differently depending on which "
            f"bureau is reporting it. The account type is a factual "
            f"characteristic of the account that cannot change by bureau. "
            f"At least one bureau is reporting an incorrect classification, "
            f"which must be corrected."
        )

    elif attack_type == "cross_bureau_credit_limit_conflict":
        reason = (
            f"The credit limit for this account differs across bureaus. "
            f"A credit limit is set by the creditor and applies to the "
            f"account — it cannot be a different number at different bureaus. "
            f"I am asking that the accurate credit limit be confirmed and "
            f"all bureaus report it correctly."
        )

    elif attack_type == "cross_bureau_high_credit_conflict":
        reason = (
            f"The high credit amount — which reflects the original loan amount "
            f"or highest balance — is being reported differently across bureaus. "
            f"This is a factual characteristic of the account that should be "
            f"consistent. The discrepancy indicates inaccurate reporting at "
            f"one or more bureaus."
        )

    elif attack_type == "student_loan_duplicate_tradeline":
        reason = (
            f"This student loan appears to be reported more than once — the same "
            f"loan is showing up as a separate tradeline with the same balance and "
            f"opening date. This is consistent with the widespread servicer transfer "
            f"errors that the Department of Education has acknowledged affecting over "
            f"a million borrowers. When a loan transfers between servicers, only the "
            f"current servicer should be reporting an active balance. Duplicate "
            f"reporting inflates my total debt and is inaccurate under 15 U.S.C. "
            f"§1681e(b). I am requesting removal of the duplicate entry."
        )

    elif attack_type == "student_loan_transferred_still_active":
        reason = (
            f"This student loan servicer appears to still be reporting this account "
            f"as active after the loan was transferred to a new servicer. When a "
            f"federal student loan is transferred, the previous servicer must update "
            f"their tradeline to show a zero balance and a 'transferred' or 'closed' "
            f"status. Continuing to report an active balance after transfer doubles "
            f"the debt on my credit report and violates 15 U.S.C. §1681s-2(a)(1). "
            f"I am requesting that this tradeline be corrected to reflect the transfer."
        )

    elif attack_type == "student_loan_deferment_late_payment":
        reason = (
            f"This student loan is reporting a late payment during what appears to "
            f"be a deferment or forbearance period. When a student loan is in an "
            f"authorized deferment or forbearance, no payment is legally due — "
            f"therefore no payment can be late. This error is consistent with the "
            f"documented servicer billing mistakes following the COVID-19 payment "
            f"pause, which the CFPB reported affected millions of borrowers. Reporting "
            f"a late payment when no payment was due is a violation of 15 U.S.C. "
            f"§1681e(b). I am requesting removal of this late payment notation."
        )

    elif attack_type == "student_loan_paid_still_reporting":
        reason = (
            f"This student loan account shows a paid or closed status but continues "
            f"to report an outstanding balance. A loan that has been paid in full "
            f"must reflect a zero balance. The failure to update the balance after "
            f"payoff is a furnisher accuracy violation under 15 U.S.C. §1681s-2(a)(1). "
            f"I am requesting that the balance be corrected to zero to accurately "
            f"reflect the current status of this account."
        )

    elif attack_type == "student_loan_discharged_still_active":
        v = variation_idx % 2
        if v == 0:
            reason = (
                f"This student loan appears to have been discharged or forgiven, "
                f"yet continues to show an active balance with a derogatory status. "
                f"Loans discharged through PSLF, total and permanent disability, "
                f"borrower defense, or other programs must be reported with a zero "
                f"balance and appropriate discharged status. Failure to update the "
                f"reporting after discharge violates 15 U.S.C. §1681s-2(a)(1). "
                f"I am requesting verification of this loan's current status and "
                f"correction of the credit reporting to reflect any discharge."
            )
        else:
            reason = (
                f"This account shows indicators of a loan discharge or forgiveness "
                f"event but the credit reporting has not been updated to reflect it. "
                f"The servicer is required under 15 U.S.C. §1681s-2(a)(1) to report "
                f"accurate information — including updating a tradeline to zero balance "
                f"and discharged status after any qualifying forgiveness event. I am "
                f"disputing the accuracy of this account and requesting documentation "
                f"of the current loan status directly from the Department of Education "
                f"or the loan servicer."
            )

    elif attack_type == "student_loan_default_inaccurate":
        reason = (
            f"This student loan is reporting a default status that may be inaccurate. "
            f"When a federal student loan is successfully rehabilitated, consolidated "
            f"out of default, or restored to good standing — including under the "
            f"Department of Education's Fresh Start program that restored eligibility "
            f"for approximately 3 million borrowers in 2023-2024 — the servicer is "
            f"required to remove the default notation from the credit report. "
            f"Under 15 U.S.C. §1681s-2(a)(1), furnishers must report accurate "
            f"information. I am requesting verification of the current loan status "
            f"and correction of any inaccurate default notation."
        )

    elif attack_type == "student_loan_balance_inflated":
        reason = (
            f"This student loan is reporting a current balance that significantly "
            f"exceeds the original loan amount. While interest accrual is expected, "
            f"the magnitude of this discrepancy is consistent with documented "
            f"servicer billing errors — including improper interest capitalization, "
            f"processing errors during servicer transfers, and fees applied in error. "
            f"The CFPB has documented widespread billing errors across major servicers. "
            f"Under 15 U.S.C. §1681e(b), only accurate information may be reported. "
            f"I am requesting a complete itemized statement of all interest, fees, "
            f"and charges comprising this balance, and correction of any errors found."
        )

    elif attack_type == "reinsertion_violation":
        v = variation_idx % 2
        if v == 0:
            reason = (
                f"This account was previously deleted from my credit report "
                f"following a dispute. It has since reappeared without the "
                f"written notice required by 15 U.S.C. §1681i(a)(5)(B). "
                f"Before reinserting a deleted item, the bureau must notify "
                f"the consumer within 5 business days and certify that the "
                f"furnisher has verified the information. Neither requirement "
                f"was met. This reinsertion is a willful violation subject to "
                f"statutory damages of $100 to $1,000 per occurrence plus "
                f"punitive damages under 15 U.S.C. §1681n. I am demanding "
                f"immediate re-deletion and filing a CFPB complaint."
            )
        else:
            reason = (
                f"This account was removed from my credit report after a "
                f"prior dispute. It has been reinserted without following "
                f"the mandatory procedure under 15 U.S.C. §1681i(a)(5)(B), "
                f"which requires: (1) consumer notice within 5 business days "
                f"of reinsertion, and (2) certification by the furnisher that "
                f"the information is complete and accurate. This procedure was "
                f"not followed. Reinsertion without notice is among the most "
                f"serious FCRA violations — I am demanding deletion and "
                f"reserving all rights under 15 U.S.C. §1681n."
            )

    elif attack_type == "medical_debt_under_500":
        bal = item.get("balance", "")
        v   = variation_idx % 2
        if v == 0:
            reason = (
                f"This is a medical collection with a reported balance of {bal}. "
                f"In April 2023, all three major credit bureaus — Equifax, Experian, "
                f"and TransUnion — publicly committed to removing all medical "
                f"collection accounts with balances under $500 from consumer credit "
                f"reports. This account falls below that threshold and should not "
                f"be on my report. I am requesting its immediate removal per the "
                f"bureau's own stated policy."
            )
        else:
            reason = (
                f"This medical collection has a balance of {bal}, which is below "
                f"the $500 threshold established by all three bureaus in April 2023. "
                f"Under that voluntary commitment, sub-$500 medical debts are no "
                f"longer reportable regardless of payment status. Continuing to "
                f"report this account is inconsistent with the bureau's own policy "
                f"and violates the accuracy standard under 15 U.S.C. §1681e(b)."
            )

    elif attack_type == "paid_medical_collection":
        v = variation_idx % 2
        if v == 0:
            reason = (
                f"This is a paid or settled medical collection. As of July 2022, "
                f"all three major credit bureaus committed to removing paid medical "
                f"collection accounts from consumer credit reports. This account "
                f"reflects a zero balance or a paid status yet continues to appear "
                f"as a derogatory item. I am requesting its removal per the bureau's "
                f"own stated policy and 15 U.S.C. §1681e(b) accuracy requirements."
            )
        else:
            reason = (
                f"This medical collection has been paid or settled and should have "
                f"been removed from my credit file. In July 2022, all three bureaus "
                f"publicly committed to no longer reporting paid medical debt. "
                f"Keeping this account on my report after payment is inaccurate "
                f"and contrary to the bureau's stated policy. I am asking that "
                f"it be deleted immediately."
            )

    elif attack_type == "medical_debt_premature":
        opened = item.get("date_opened", "")
        reason = (
            f"This medical collection was opened {opened} — less than 12 months "
            f"ago. In 2022, all three credit bureaus committed to a 12-month "
            f"waiting period before any medical debt may appear on a consumer "
            f"credit report. This account is being reported before that window "
            f"has elapsed. I am requesting that it be removed until the "
            f"12-month period has fully passed."
        )

    elif attack_type == "medical_debt_state_law":
        reason = (
            f"This is a medical collection being reported for a consumer in a "
            f"state that has enacted legal protections prohibiting medical debt "
            f"from appearing on consumer credit reports. Reporting this account "
            f"is not permitted under applicable state law. Under 15 U.S.C. "
            f"§1681e(b), the bureau must maintain maximum possible accuracy, "
            f"which includes compliance with state law restrictions. I am "
            f"requesting immediate removal of this account."
        )

    elif attack_type == "medical_debt_accuracy":
        v = variation_idx % 3
        if v == 0:
            reason = (
                f"This is a medical collection and I am disputing its accuracy. "
                f"Medical debt is uniquely prone to billing errors — insurance "
                f"disputes, incorrect coding, surprise bills, and balance inflation "
                f"are documented systemic problems. Under 15 U.S.C. §1681e(b), "
                f"this bureau must maintain maximum possible accuracy. I am "
                f"requesting full verification: itemized bill, proof that "
                f"insurance was properly applied, original creditor name, "
                f"and the exact amount owed at the time of default."
            )
        elif v == 1:
            reason = (
                f"I am disputing this medical collection. Medical billing is "
                f"notoriously inaccurate — the CFPB has documented that medical "
                f"debt is a poor predictor of creditworthiness and is often "
                f"reported incorrectly. I am asking that {furnisher} provide "
                f"a complete itemized statement, evidence that my insurance was "
                f"properly billed and applied, and documentation that this amount "
                f"is accurately stated. If the underlying bill cannot be fully "
                f"verified, this account must be removed."
            )
        else:
            reason = (
                f"This medical collection requires verification under 15 U.S.C. "
                f"§1681i(a). Medical bills are frequently the result of insurance "
                f"billing errors, coordination failures, or amounts disputed with "
                f"the provider. I am requesting that the furnisher provide the "
                f"original itemized bill, confirmation of insurance payments and "
                f"adjustments applied, and validation that the reported balance "
                f"is accurate. Without that documentation, this account cannot "
                f"be verified and must be deleted."
            )

    elif attack_type == "collection_late_payment_conflict":
        pay_status  = item.get("payment_status", "")
        acct_detail = item.get("account_type_detail", "")
        v = variation_idx % 3
        if v == 0:
            reason = (
                f"This account is identified as a collection account but is also "
                f"being reported with a payment status of '{pay_status}'. Those two "
                f"classifications cannot coexist. A collection account represents a "
                f"debt that has already defaulted and been transferred — there is no "
                f"active payment obligation remaining, so it cannot simultaneously be "
                f"late on payments to a creditor. Reporting both inflates the credit "
                f"damage from a single event and is inaccurate under 15 U.S.C. "
                f"\u00a71681e(b). I am asking that this be corrected so the account "
                f"reflects only one accurate classification."
            )
        elif v == 1:
            reason = (
                f"The payment status of '{pay_status}' on this account conflicts "
                f"directly with its account type of '{acct_detail}'. An account "
                f"cannot be a collection — meaning it has already defaulted and been "
                f"transferred from the original creditor — while also being in an "
                f"active late-payment status. These are mutually exclusive states. "
                f"I am requesting that the inaccurate classification be corrected "
                f"or, if the account cannot be accurately described, that it be deleted."
            )
        else:
            reason = (
                f"I am disputing the classification of this account as both a "
                f"collection and a '{pay_status}' account simultaneously. Once a "
                f"debt is transferred to a collection agency, the original payment "
                f"schedule no longer exists — there are no more monthly payments to "
                f"be late on. The late payment notation is inaccurate and creates "
                f"a double-negative from a single delinquency event. Under "
                f"15 U.S.C. \u00a71681e(b), only accurate information may be reported."
            )

    elif attack_type == "late_collection_conflict":
        reason = (
            f"The way this account is classified does not add up. It appears "
            f"to show both a late payment status and collection-type language "
            f"at the same time, which are contradictory. I am asking that "
            f"the correct classification be applied based on what the account "
            f"actually is."
        )

    elif attack_type == "absent_bureau_reporting_inconsistency":
        v = variation_idx % 4
        if v == 0:
            reason = (
                f"This account is showing as negative on some bureau reports "
                f"but not on others. If whoever is reporting this has a valid "
                f"basis to do so, I would expect it to show up consistently. "
                f"I am asking that you verify whether what is being reported "
                f"here is accurate and complete."
            )
        elif v == 1:
            reason = (
                f"I noticed this account appears on my report here but not "
                f"across all three bureaus. That inconsistency makes me question "
                f"whether the reporting is accurate. I am asking that this be "
                f"verified and, if it cannot be confirmed as accurate and "
                f"complete, that it be removed."
            )
        elif v == 2:
            reason = (
                f"This account is not being reported the same way at all three "
                f"bureaus — it shows as a negative item here but not elsewhere. "
                f"Under 15 U.S.C. \u00a71681e(b), every bureau must maintain "
                f"maximum possible accuracy. An item that cannot be reported "
                f"consistently at all bureaus raises serious accuracy concerns "
                f"and needs to be fully verified."
            )
        else:
            reason = (
                f"The reporting on this account is inconsistent across bureaus. "
                f"It appears as a derogatory item in some places and not others, "
                f"which suggests the furnisher is either selectively reporting "
                f"or reporting information that cannot be verified. I am asking "
                f"that this account be fully investigated and removed if the "
                f"reporting cannot be verified as accurate at all bureaus."
            )

    elif attack_type == "late_payment_history_dispute":
        late_codes   = item.get("late_payment_codes", [])
        actual_lates = [c for c in late_codes if not c.startswith("CO:")]
        worst = "30"
        for code in actual_lates:
            val = code.split(":")[0]
            if val in ("90","120") and worst in ("30","60"):
                worst = val
            elif val == "60" and worst == "30":
                worst = val
        late_str  = ", ".join(actual_lates) if actual_lates else "in the payment history"
        status    = item.get("status","").lower()
        is_closed = any(k in status for k in ("closed","paid","refinanced"))
        v = variation_idx % 3
        if is_closed:
            if v == 0:
                reason = (
                    f"This account is closed and shows a zero balance, but the payment "
                    f"history contains a {worst}-day late mark ({late_str}). On a closed "
                    f"account, late marks must be accurate and the Date of First Delinquency "
                    f"must be reported correctly so the 7-year clock under 15 U.S.C. "
                    f"\u00a71681c(a)(4) can be verified. I am requesting that {furnisher} "
                    f"provide the original payment records confirming the date and amount "
                    f"of the missed payment and the correct DOFD."
                )
            elif v == 1:
                reason = (
                    f"I am disputing a late payment mark on this closed account ({late_str}). "
                    f"Because the account is paid, the reporting window is controlled by the "
                    f"Date of First Delinquency under 15 U.S.C. \u00a71681c(c). I am "
                    f"requesting that {furnisher} provide the original payment records "
                    f"and the correct DOFD. If this cannot be verified, the late mark "
                    f"must be removed."
                )
            else:
                reason = (
                    f"This closed account has a {worst}-day late payment in its history "
                    f"({late_str}). Late marks on closed accounts still affect my credit "
                    f"and must be accurate under 15 U.S.C. \u00a71681e(b). I am asking "
                    f"{furnisher} to verify this with original payment records — due date, "
                    f"receipt date — and to confirm the DOFD is correct at all bureaus."
                )
        else:
            if v == 0:
                reason = (
                    f"This account shows a {worst}-day late payment in its history "
                    f"({late_str}). Under 15 U.S.C. \u00a71681e(b), I am requesting "
                    f"documentation from {furnisher} — original payment records showing "
                    f"when payment was due and when it was received. If they cannot "
                    f"produce that, the late mark must be removed."
                )
            elif v == 1:
                reason = (
                    f"I am disputing the {worst}-day late payment on this account "
                    f"({late_str}). I am asking {furnisher} to provide the original "
                    f"billing statement and payment records for that month, confirming "
                    f"the exact due date and receipt date. Under 15 U.S.C. "
                    f"\u00a71681s-2(a)(1), this cannot be reported without primary "
                    f"documentation to back it up."
                )
            else:
                reason = (
                    f"There is a {worst}-day late mark in this account's history "
                    f"({late_str}) that I am disputing. I am asking {furnisher} to "
                    f"produce the original records for that month to confirm the mark "
                    f"is accurate. Without that, the late payment cannot be verified "
                    f"under the FCRA."
                )

    elif attack_type == "cross_bureau_payment_history_date_conflict":
        late_codes   = item.get("late_payment_codes", [])
        actual_lates = [c for c in late_codes if not c.startswith("CO:")]
        late_str     = ", ".join(actual_lates) if actual_lates else "in the payment history"
        v = variation_idx % 2
        if v == 0:
            reason = (
                f"This account shows a late payment ({late_str}) but the month reported "
                f"is different depending on which bureau you look at. A payment can only "
                f"be late on one specific date — the same event cannot be reported in "
                f"different months at different bureaus. Under 15 U.S.C. \u00a71681e(b), "
                f"I am requesting that {furnisher} provide the original payment records "
                f"and correct the reporting to show the same accurate date at all three bureaus."
            )
        else:
            reason = (
                f"The late payment on this account is reported in different months "
                f"across bureaus ({late_str} at this bureau). An event cannot happen "
                f"on two different dates — this is an accuracy violation under "
                f"15 U.S.C. \u00a71681e(b). I am asking that {furnisher} review the "
                f"original payment records and correct all three bureaus to show the "
                f"same accurate month. If the correct date cannot be verified, the "
                f"late mark must be removed."
            )

    else:  # requires_basic_verification
        if variation_idx % 3 == 0:
            reason = (
                f"I am disputing the accuracy of this account. I am asking that "
                f"{furnisher} provide full documentation — original agreement, "
                f"complete payment history, account status, and the exact date "
                f"I first fell behind. {v_close}"
            )
        elif variation_idx % 3 == 1:
            reason = (
                f"{opener}I do not believe the information being reported here "
                f"is complete or accurate. I need {furnisher} to back this up "
                f"with the original contract, a full payment record, and the "
                f"correct date of first delinquency. {v_close}"
            )
        else:
            reason = (
                f"I question the accuracy of this account as it is being reported. "
                f"I am asking that {furnisher} provide all underlying records — "
                f"original agreement, payment history, and the date I first "
                f"missed a payment. {v_close}"
            )

    return reason + " DELETE OFF MY CREDIT REPORT."


def build_dispute_letter_engine(
    letter_input_engine: dict[str, dict[str, list[dict[str, Any]]]],
    consumer_name: str = "[CLIENT NAME]",
    report_date: str = "",
    personal_info: dict[str, Any] | None = None,
    personal_info_issues: list[dict[str, Any]] | None = None,
) -> dict[str, dict[str, dict[str, str]]]:
    """
    Generate dispute letters — ONE LETTER PER ACCOUNT-TYPE GROUP PER ROUND PER BUREAU.

    Collections cannot be in the same letter as late payments or repossessions.
    Each group disputes accounts under distinct legal grounds and must be
    presented separately so the bureau processes them independently.

    Return structure:
        {
            "transunion": {
                "collections_chargeoffs": {"round_1": "...", "round_2": "..."},
                "late_payments":          {"round_1": "...", "round_2": "..."},
                "other_derogatory":       {"round_1": "...", "round_2": "..."},
            },
            ...
        }

    Groups:
        collections_chargeoffs — FDCPA + FCRA, chain-of-title, DOFD attacks
        late_payments          — payment history accuracy, §1681e(b)
        other_derogatory       — repossession (UCC Art.9), child support (§1681s-1),
                                 bankruptcy (§1681c), charge-off deficiency, paid collection
    """
    result: dict[str, dict[str, dict[str, str]]] = {}
    formatted_date = _format_date_long(report_date)

    bureau_seed_map = {"transunion": 1, "experian": 2, "equifax": 3}

    # Opening template assignment — unique per bureau+group+round combination.
    # We use a deterministic hash so the same client always gets the same letter,
    # and we spread evenly across the available templates.
    def _tpl_idx(bureau: str, group: str, round_key: str, n_templates: int) -> int:
        # Sequential assignment within each bureau: 3 groups × 2 rounds = 6 slots.
        # With 6 R1 templates and 3 R2 templates this guarantees zero intra-bureau dups.
        group_pos   = {"collections_chargeoffs": 0, "late_payments": 1, "other_derogatory": 2}
        round_pos   = 0 if round_key == "round_1" else 1
        g           = group_pos.get(group, 0)
        slot        = g * 2 + round_pos          # 0-5 for R1, 0-2 for R2
        return slot % n_templates

    group_order = ["collections_chargeoffs", "late_payments", "other_derogatory"]

    for bureau, groups in letter_input_engine.items():
        bureau_info    = BUREAU_ADDRESSES.get(bureau, {})
        bureau_name    = bureau_info.get("name", bureau.title())
        bureau_address = bureau_info.get("address", "")
        seed           = bureau_seed_map.get(bureau, 1)

        result[bureau] = {}

        for group_key in group_order:
            items_in_group = groups.get(group_key, [])
            if not items_in_group:
                continue

            # Split by round
            round_1_items = [i for i in items_in_group if i.get("recommended_round","round_1") == "round_1"]
            round_2_items = [i for i in items_in_group if i.get("recommended_round","round_1") == "round_2"]

            group_letters: dict[str, str] = {}

            for round_key, items in [("round_1", round_1_items), ("round_2", round_2_items)]:
                if not items:
                    continue

                is_r2     = round_key == "round_2"
                templates = _OPENING_TEMPLATES_R2 if is_r2 else _OPENING_TEMPLATES_R1
                tpl_idx   = _tpl_idx(bureau, group_key, round_key, len(templates))
                tpl       = templates[tpl_idx]
                # Substitute placeholders
                n            = len(items)
                these_items  = "these accounts" if n != 1 else "this account"
                they_verb    = "are" if n != 1 else "is"
                count_str    = f"{n} account{'s' if n != 1 else ''}"
                opening      = tpl.format(
                    count=count_str,
                    verb=they_verb,
                    they_verb=they_verb,
                    these_items=these_items,
                )

                header = (
                    f"{consumer_name}\n"
                    f"[Address]\n"
                    f"[City, State ZIP]\n"
                    f"\n"
                    f"{bureau_name}\n"
                    f"{bureau_address}\n"
                    f"\n"
                    f"{formatted_date}"
                )

                # Personal information section (Round 1 only, first group only)
                pi_section = ""
                if not is_r2 and group_key == "collections_chargeoffs" and personal_info and personal_info_issues:
                    pi_section = build_personal_info_section(
                        personal_info, personal_info_issues, bureau
                    )

                # Account list
                account_lines = []
                used_reasons: set[str] = set()
                for idx, item in enumerate(items, 1):
                    fname  = item.get("furnisher_name", "")
                    facct  = item.get("account_number", "")
                    at     = item.get("attack_type", "")
                    base_vi   = idx - 1
                    acct_hash = abs(hash(facct + fname + at)) % 89
                    variation_idx = base_vi + acct_hash
                    reason = ""
                    for attempt in range(20):
                        reason = _account_reason(item, variation_idx=variation_idx + attempt)
                        if reason not in used_reasons:
                            break
                    if reason in used_reasons:
                        reason = reason + f" (Account #{facct}.)"
                    used_reasons.add(reason)
                    account_lines.append(
                        f"{idx}. {fname} \u2014 Account #: {facct}\n"
                        f"Reason: {reason} DELETE OFF MY CREDIT REPORT."
                    )

                accounts_block = "\n\n".join(account_lines)

                body_parts = []
                if pi_section:
                    body_parts.append(pi_section)
                body_parts.append(
                    "The following accounts must be deleted immediately:\n\n" + accounts_block
                )

                full = (
                    header + "\n\n"
                    + opening + "\n\n\n"
                    + "\n\n\n".join(body_parts) + "\n\n\n"
                    + consumer_name
                )
                group_letters[round_key] = full

            if group_letters:
                result[bureau][group_key] = group_letters

    return result


def _group_context(group_key: str) -> str:
    """Plain-language context string for group — used in opening templates."""
    return {
        "collections_chargeoffs": "collection and charged-off accounts",
        "late_payments":          "late payment accounts",
        "other_derogatory":       "derogatory accounts",
    }.get(group_key, "accounts")
# =========================
# STUDENT LOAN COMPLEX ENGINE
# =========================
#
# Detects student loan reporting errors beyond the basic
# "student_loan_multiple_servicer" and "student_loan_status_inaccurate"
# attacks already in the system.
#
# Attack types added here:
#   student_loan_duplicate_tradeline      — same loan reported twice (balance doubled)
#   student_loan_transferred_still_active — old servicer still reporting after transfer
#   student_loan_deferment_late_payment   — late payment during deferment/forbearance
#   student_loan_paid_still_reporting     — paid/discharged loan showing balance
#   student_loan_discharged_still_active  — PSLF/disability discharge not reflected
#   student_loan_default_inaccurate       — default status after rehabilitation
#   student_loan_balance_inflated         — balance > original loan amount
#
# Context: As of early 2026, DOE has acknowledged ~1.4M duplicate loan records
# from servicer transfers (Navient→MOHELA, Great Lakes→Nelnet, etc.).
# A Senate investigation put the number at ~2M. Two federal class actions
# (Walsh v. DOE, SDNY Feb 2026) are active. Individual FCRA disputes are
# the fastest path to resolution and are independent of class litigation.
#
# Law:
#   §1681e(b)    — maximum possible accuracy
#   §1681s-2(a)(1) — furnisher must report accurate information
#   §1681s-2(b)  — furnisher must investigate disputes
#   §1681i(a)    — bureau reinvestigation within 30 days

# Known federal student loan servicers (current and legacy)
_STUDENT_LOAN_SERVICERS: frozenset[str] = frozenset({
    "MOHELA", "NELNET", "AIDVANTAGE", "NAVIENT", "GREAT LAKES",
    "FEDLOAN", "PHEAA", "EDFINANCIAL", "OSLA", "GRANITE STATE",
    "SALLIE MAE", "SALLIEMAE", "AES", "ECSI", "HEARTLAND ECSI",
    "MAXIMUS", "AMERICAN EDUCATION SERVICES", "DEPT OF EDUCATION",
    "DEPT EDUCATION", "DEPT ED", "DOE", "U.S. DEPT OF EDUCATION",
    "US DEPT OF EDUCATION", "NAVIENT SOLUTIONS", "PIONEER CREDIT",
    "COLLEGIATE FUNDING", "ACCESS GROUP", "FIRSTMARK",
})

# Keywords indicating deferment/forbearance in raw text
_DEFERMENT_KEYWORDS: tuple[str, ...] = (
    "deferment", "forbearance", "in school", "grace period",
    "military deferment", "economic hardship", "administrative forbearance",
    "covid", "cares act", "payment pause", "pandemic",
)

# Keywords indicating discharge/forgiveness
_DISCHARGE_KEYWORDS: tuple[str, ...] = (
    "discharged", "forgiven", "forgiveness", "cancelled", "canceled",
    "pslf", "public service loan forgiveness", "tpd", "total and permanent",
    "disability discharge", "closed school", "borrower defense",
    "false certification",
)

# Keywords indicating rehabilitation/consolidation (cures default)
_REHAB_KEYWORDS: tuple[str, ...] = (
    "rehabilitated", "rehabilitation", "consolidated", "consolidation",
    "fresh start", "good standing",
)


def _is_student_loan_servicer(name: str) -> bool:
    """Check if furnisher name matches a known student loan servicer."""
    n_up = name.upper().strip()
    # Exact match
    if n_up in _STUDENT_LOAN_SERVICERS:
        return True
    # Partial match — covers "NELNET BANK", "MOHELA/DOE", etc.
    return any(s in n_up for s in _STUDENT_LOAN_SERVICERS)


def detect_student_loan_complex_attacks(
    bureau: str,
    accounts: list[dict[str, Any]],
    all_bureaus_inventory: dict[str, list[dict[str, Any]]] | None = None,
    report_date: str = "",
) -> list[dict[str, Any]]:
    """
    Detect complex student loan reporting errors from credit report data alone.

    Parameters
    ----------
    bureau               : bureau being analyzed
    accounts             : negative accounts at this bureau
    all_bureaus_inventory: full inventory (needed for cross-bureau duplicate detection)
    report_date          : report date string MM/DD/YYYY
    """
    attacks: list[dict[str, Any]] = []
    all_inv  = all_bureaus_inventory or {bureau: accounts}

    # Build a list of all student loan accounts across all bureaus
    all_sl_accounts: list[dict[str, Any]] = []
    for b, b_accs in all_inv.items():
        for acc in b_accs:
            if _is_student_loan_servicer(acc.get("name", "")):
                all_sl_accounts.append({**acc, "_bureau": b})

    # Filter to just this bureau's student loan accounts
    bureau_sl = [a for a in accounts if _is_student_loan_servicer(a.get("name", ""))]

    if not bureau_sl:
        return attacks

    for acc in bureau_sl:
        name     = acc.get("name", "")
        acct_num = acc.get("account_number", "")
        status   = acc.get("status", "").lower()
        payment  = acc.get("payment_status", "").lower()
        balance  = _parse_dollar(acc.get("balance", ""))
        high_cr  = _parse_dollar(acc.get("high_credit", ""))
        comments = acc.get("comments", "").lower()
        raw      = " ".join(acc.get("raw_lines", [])).lower()
        d_opened = parse_date_field(acc.get("date_opened", ""))
        d_report = parse_date_field(report_date)
        all_text = comments + " " + raw

        # ── ATTACK 1: Duplicate tradeline (same loan, same bureau) ────────
        # Same servicer + same balance + same date_opened on this bureau
        duplicates_same_bureau = [
            a for a in bureau_sl
            if a is not acc
            and _normalize_name(a.get("name","")) == _normalize_name(name)
            and abs(_parse_dollar(a.get("balance","")) - balance) < 50
            and a.get("date_opened","") == acc.get("date_opened","")
        ]
        if duplicates_same_bureau:
            attacks.append(build_attack_record(
                attack_type="student_loan_duplicate_tradeline",
                bureau=bureau,
                accounts=[acc],
                strategy_tags=["FCRA_1681e_b", "FCRA_1681s_2_a_1"],
                reason=(
                    f"{name} (Account #{acct_num}) appears to be a duplicate "
                    f"tradeline — the same loan is being reported more than once "
                    f"at {bureau.title()} with the same balance and opening date. "
                    f"This is consistent with the widespread servicer transfer "
                    f"errors the Department of Education has acknowledged, where "
                    f"loan data is reported by both the old and new servicer "
                    f"simultaneously. Reporting the same loan twice inflates my "
                    f"total debt and is a direct violation of 15 U.S.C. §1681e(b). "
                    f"I am requesting that the duplicate entry be removed, leaving "
                    f"only the account with the current servicer."
                ),
            ))

        # ── ATTACK 2: Transferred loan still active from old servicer ─────
        # Old servicer (Navient, Great Lakes, FedLoan) reporting same loan
        # that also appears under a new servicer with similar balance
        legacy_servicers = {"NAVIENT", "GREAT LAKES", "FEDLOAN", "PHEAA"}
        name_up = name.upper()
        is_legacy = any(s in name_up for s in legacy_servicers)
        if is_legacy and balance > 0:
            # Check if same loan appears under a current servicer too
            current_servicers = {"MOHELA", "NELNET", "AIDVANTAGE", "EDFINANCIAL"}
            same_loan_elsewhere = [
                a for a in all_sl_accounts
                if a is not acc
                and any(s in a.get("name","").upper() for s in current_servicers)
                and abs(_parse_dollar(a.get("balance","")) - balance) < 200
            ]
            if same_loan_elsewhere:
                attacks.append(build_attack_record(
                    attack_type="student_loan_transferred_still_active",
                    bureau=bureau,
                    accounts=[acc],
                    strategy_tags=["FCRA_1681e_b", "FCRA_1681s_2_a_1"],
                    reason=(
                        f"{name} (Account #{acct_num}) is a former student loan "
                        f"servicer that appears to still be reporting this loan "
                        f"as active after the loan was transferred to a new servicer. "
                        f"When a loan is transferred, the previous servicer is required "
                        f"to update their tradeline to show a zero balance with a "
                        f"'transferred' or 'closed' status. Continuing to report an "
                        f"active balance after transfer creates a duplicate reporting "
                        f"of the same debt and violates 15 U.S.C. §1681s-2(a)(1). "
                        f"I am requesting that this tradeline be updated to reflect "
                        f"the transfer with a zero balance."
                    ),
                ))

        # ── ATTACK 3: Late payment during deferment/forbearance ───────────
        is_late    = "late" in payment and "collection" not in payment
        in_deferment = any(k in all_text for k in _DEFERMENT_KEYWORDS)
        if is_late and in_deferment:
            attacks.append(build_attack_record(
                attack_type="student_loan_deferment_late_payment",
                bureau=bureau,
                accounts=[acc],
                strategy_tags=["FCRA_1681e_b", "FCRA_1681s_2_a_1"],
                reason=(
                    f"{name} (Account #{acct_num}) is reporting a late payment "
                    f"status of '{acc.get('payment_status','')}' during what appears "
                    f"to be a deferment or forbearance period. When a student loan "
                    f"is in an authorized deferment or forbearance, no payment is "
                    f"legally due — therefore no payment can be reported as late. "
                    f"This error is consistent with the widespread servicer billing "
                    f"mistakes documented by the CFPB following the end of the "
                    f"COVID-19 payment pause. I am disputing this late payment "
                    f"notation as inaccurate under 15 U.S.C. §1681e(b) and "
                    f"requesting its immediate removal."
                ),
            ))

        # ── ATTACK 4: Paid/discharged loan still showing balance ──────────
        is_paid_status = "paid" in status or "closed" in status
        if is_paid_status and balance > 0 and "collection" not in payment:
            attacks.append(build_attack_record(
                attack_type="student_loan_paid_still_reporting",
                bureau=bureau,
                accounts=[acc],
                strategy_tags=["FCRA_1681e_b", "FCRA_1681s_2_a_1"],
                reason=(
                    f"{name} (Account #{acct_num}) shows a status of "
                    f"'{acc.get('status','')}' but continues to report a balance "
                    f"of {acc.get('balance','')}. A paid or closed student loan "
                    f"should reflect a zero balance. This discrepancy indicates "
                    f"that the loan payoff was not properly recorded and reported "
                    f"to the credit bureaus by the servicer, violating the "
                    f"accuracy requirements of 15 U.S.C. §1681e(b). I am "
                    f"requesting that the balance be corrected to zero."
                ),
            ))

        # ── ATTACK 5: Discharged/forgiven loan still showing as active ────
        has_discharge_indicator = any(k in all_text for k in _DISCHARGE_KEYWORDS)
        if has_discharge_indicator and balance > 0 and "derogatory" in status:
            attacks.append(build_attack_record(
                attack_type="student_loan_discharged_still_active",
                bureau=bureau,
                accounts=[acc],
                strategy_tags=["FCRA_1681e_b", "FCRA_1681s_2_a_1"],
                reason=(
                    f"{name} (Account #{acct_num}) appears to have language "
                    f"indicating a discharge or forgiveness event, but continues "
                    f"to show an active balance of {acc.get('balance','')} with "
                    f"a derogatory status. If this loan has been discharged through "
                    f"PSLF, total and permanent disability, borrower defense, or "
                    f"another forgiveness program, the servicer is required to "
                    f"report the correct discharged status and a zero balance "
                    f"under 15 U.S.C. §1681s-2(a)(1). I am requesting verification "
                    f"of the current loan status and correction of the reporting."
                ),
            ))

        # ── ATTACK 6: Default status after rehabilitation ─────────────────
        is_default = (
            "default" in payment or "default" in status
            or "collection/chargeoff" in payment and "student" in acc.get("account_type_detail","").lower()
        )
        has_rehab  = any(k in all_text for k in _REHAB_KEYWORDS)
        if is_default and has_rehab:
            attacks.append(build_attack_record(
                attack_type="student_loan_default_inaccurate",
                bureau=bureau,
                accounts=[acc],
                strategy_tags=["FCRA_1681e_b", "FCRA_1681s_2_a_1"],
                reason=(
                    f"{name} (Account #{acct_num}) is reporting a default status "
                    f"despite what appears to be a rehabilitation or consolidation "
                    f"event. When a federal student loan is successfully rehabilitated "
                    f"or consolidated out of default, the servicer is required to "
                    f"remove the default notation from the credit report. The "
                    f"Department of Education restored good standing for approximately "
                    f"3 million borrowers in 2023-2024. I am disputing this default "
                    f"status as inaccurate and requesting that it be corrected to "
                    f"reflect the current standing of this loan."
                ),
            ))

        # ── ATTACK 7: Balance greater than original loan amount ───────────
        # More than 25% above original — fees/interest cannot reasonably explain this
        if high_cr > 0 and balance > high_cr * 1.25:
            overage = balance - high_cr
            attacks.append(build_attack_record(
                attack_type="student_loan_balance_inflated",
                bureau=bureau,
                accounts=[acc],
                strategy_tags=["FCRA_1681e_b", "FCRA_1681s_2_a_1"],
                reason=(
                    f"{name} (Account #{acct_num}) shows a current balance of "
                    f"{acc.get('balance','')} that exceeds the original loan "
                    f"amount of {acc.get('high_credit','')} by approximately "
                    f"${overage:,.2f}. While interest and fees can increase the "
                    f"balance on student loans, an overage of this magnitude "
                    f"suggests a reporting error — particularly given the "
                    f"documented servicer billing errors that have caused inflated "
                    f"balances across millions of borrower accounts. I am "
                    f"requesting a complete itemized accounting of all interest, "
                    f"fees, and charges that account for this balance, and "
                    f"correction if any charges were applied in error."
                ),
            ))

    return attacks


# =========================
# MEDICAL DEBT ENGINE
# =========================
#
# Legal basis hierarchy (April 2026):
#
#   Tier 1 — Bureau voluntary policy (strongest argument)
#     • Under $500:      prohibited since April 11, 2023 (all 3 bureaus)
#     • Paid medical:    removed since July 2022 (all 3 bureaus)
#     • Under 12 months: not reportable since 2022 (all 3 bureaus)
#
#   Tier 2 — State law (applies where client lives)
#     • 15+ states have enacted medical debt credit reporting bans (2023-2025)
#     • CA, CO, IL, CT, NJ, NY, MD, VA, WA, OR, MN, ME, VT, RI, DE
#
#   Tier 3 — FCRA §1681e(b) accuracy (universal, always applies)
#     • Medical billing has documented systemic inaccuracy problem
#     • Insurance disputes, No Surprises Act violations, billing errors
#
#   CFPB rule (Jan 2025) — VACATED July 11, 2025. Not citable.
#   Bureau voluntary policies — still in effect. Primary dispute vehicle.

# States with active medical debt credit reporting restrictions (as of April 2026)
MEDICAL_DEBT_PROTECTED_STATES: set[str] = {
    "CA", "CALIFORNIA",
    "CO", "COLORADO",
    "IL", "ILLINOIS",
    "CT", "CONNECTICUT",
    "NJ", "NEW JERSEY",
    "NY", "NEW YORK",
    "MD", "MARYLAND",
    "VA", "VIRGINIA",
    "WA", "WASHINGTON",
    "OR", "OREGON",
    "MN", "MINNESOTA",
    "ME", "MAINE",
    "VT", "VERMONT",
    "RI", "RHODE ISLAND",
    "DE", "DELAWARE",
}

# Known medical provider name patterns — used to identify medical collections
_MEDICAL_KEYWORDS: tuple[str, ...] = (
    "hospital", "medical", "health", "clinic", "surgery", "physician",
    "doctor", "dental", "dentist", "orthopedic", "radiology", "laboratory",
    "lab ", "urgent care", "emergency", "er ", "ambulance", "pharmacy",
    "rx ", "rehab", "therapy", "optometry", "optician", "vision care",
    "behavioral health", "mental health", "psychiatry", "chiropractic",
    "dermatology", "oncology", "cardiology", "neurology", "pediatric",
    "obgyn", "ob/gyn", "urology", "anesthesia", "pathology", "imaging",
    "mri ", "ct scan", "xray", "x-ray", "dialysis", "infusion",
    "nursing home", "skilled nursing", "home health", "hospice",
    # Common medical billing company patterns
    "medstar", "medline", "carecredit", "synchrony health", "alphaeon",
    "premier health", "community health", "regional medical", "county hospital",
    "memorial hospital", "st. ", "saint ", "sacred heart", "mercy ", "providence",
    "adventist", "banner health", "dignity health", "hca ", "tenet health",
    "ascension", "commonspirit", "kaiser", "intermountain", "geisinger",
    "ssm health", "baycare", "piedmont", "spectrum health",
)

_MEDICAL_BUSINESS_TYPES: tuple[str, ...] = (
    "medical", "health", "hospital", "dental", "physician", "doctor",
    "clinic", "pharmacy", "laboratory", "therapy", "mental health",
)


def _is_medical_account(acc: dict[str, Any]) -> bool:
    """
    Determine if an account is a medical collection based on:
    - Furnisher name containing medical keywords
    - Account type / business type matching medical categories
    - Comments mentioning medical context
    """
    name    = acc.get("name", "").lower()
    acct_det= acc.get("account_type_detail", "").lower()
    biz_type= acc.get("business_type", "").lower()
    comments= acc.get("comments", "").lower()
    raw     = " ".join(acc.get("raw_lines", [])).lower()

    # Check name against medical keywords
    if any(k in name for k in _MEDICAL_KEYWORDS):
        return True
    # Check account type
    if any(k in acct_det for k in _MEDICAL_BUSINESS_TYPES):
        return True
    if any(k in biz_type for k in _MEDICAL_BUSINESS_TYPES):
        return True
    # Check comments/raw for medical context
    if any(k in comments for k in ("medical", "health", "hospital", "physician")):
        return True
    if "medical collection" in raw or "healthcare" in raw:
        return True

    return False


def detect_medical_debt_attacks(
    bureau: str,
    accounts: list[dict[str, Any]],
    report_date: str = "",
    client_state: str = "",
) -> list[dict[str, Any]]:
    """
    Detect medical debt accounts that should not be on credit reports.

    Four attack types in priority order:
      1. medical_debt_under_500      — balance < $500, per bureau voluntary policy
      2. paid_medical_collection     — paid/settled, per bureau voluntary policy
      3. medical_debt_premature      — reported within 12 months of service date
      4. medical_debt_state_law      — in a state with medical debt reporting ban
      5. medical_debt_accuracy       — general accuracy attack on any medical debt
    """
    from original_parser import parse_date_field
    import datetime as _dt

    attacks: list[dict[str, Any]] = []

    report_dt = parse_date_field(report_date)

    for acc in accounts:
        if not _is_medical_account(acc):
            continue

        name     = acc.get("name", "")
        acct_num = acc.get("account_number", "")
        balance  = _parse_dollar(acc.get("balance", ""))
        status   = acc.get("status", "").lower()
        payment  = acc.get("payment_status", "").lower()
        opened   = acc.get("date_opened", "")
        d_opened = parse_date_field(opened)

        is_paid = (
            "paid" in status
            or ("collection/chargeoff" in payment and balance == 0)
            or "paid" in payment
        )
        is_collection = (
            "collection" in acc.get("account_type_detail", "").lower()
            or "collection" in payment
            or "chargeoff" in payment
        )

        # ── ATTACK 1: Under $500 ───────────────────────────────────────
        if balance > 0 and balance < 500 and is_collection:
            attacks.append(build_attack_record(
                attack_type="medical_debt_under_500",
                bureau=bureau,
                accounts=[acc],
                strategy_tags=["bureau_policy_2023", "FCRA_1681e_b"],
                reason=(
                    f"{name} (Account #{acct_num}) is a medical collection "
                    f"with a balance of ${balance:,.2f} — under the $500 "
                    f"threshold. In April 2023, all three credit bureaus "
                    f"publicly committed to removing all medical debt under "
                    f"$500 from consumer credit reports. This account should "
                    f"not appear on my report under that policy. I am requesting "
                    f"its immediate removal."
                ),
            ))

        # ── ATTACK 2: Paid medical collection ─────────────────────────
        elif is_paid and is_collection:
            attacks.append(build_attack_record(
                attack_type="paid_medical_collection",
                bureau=bureau,
                accounts=[acc],
                strategy_tags=["bureau_policy_2022", "FCRA_1681e_b"],
                reason=(
                    f"{name} (Account #{acct_num}) is a paid or settled "
                    f"medical collection. As of July 2022, all three major "
                    f"credit bureaus committed to removing paid medical "
                    f"collection accounts from consumer credit reports. "
                    f"This account shows as paid or zero-balance yet continues "
                    f"to appear. I am requesting its removal per the bureaus' "
                    f"own stated policy."
                ),
            ))

        # ── ATTACK 3: Under 12 months ─────────────────────────────────
        elif (d_opened and report_dt and is_collection
              and (report_dt - d_opened).days < 365):
            months_old = (report_dt - d_opened).days // 30
            attacks.append(build_attack_record(
                attack_type="medical_debt_premature",
                bureau=bureau,
                accounts=[acc],
                strategy_tags=["bureau_policy_2022", "FCRA_1681e_b"],
                reason=(
                    f"{name} (Account #{acct_num}) is a medical collection "
                    f"that was opened approximately {months_old} months ago "
                    f"({opened}). In 2022, all three credit bureaus committed "
                    f"to a 12-month waiting period before medical debt may "
                    f"appear on credit reports. This account was reported "
                    f"before that window has elapsed. I am asking that it be "
                    f"removed until the 12-month period has passed."
                ),
            ))

        # ── ATTACK 4: State law ────────────────────────────────────────
        elif client_state and client_state.upper() in MEDICAL_DEBT_PROTECTED_STATES:
            state_name = client_state.upper()
            attacks.append(build_attack_record(
                attack_type="medical_debt_state_law",
                bureau=bureau,
                accounts=[acc],
                strategy_tags=["state_law", "FCRA_1681e_b"],
                reason=(
                    f"{name} (Account #{acct_num}) is a medical collection "
                    f"being reported for a consumer in {state_name}. That state "
                    f"has enacted a law prohibiting medical debt from appearing "
                    f"on consumer credit reports. Reporting this account violates "
                    f"applicable state consumer protection law. I am requesting "
                    f"its removal from my credit file."
                ),
            ))

        # ── ATTACK 5: General accuracy (all other medical debt) ────────
        else:
            attacks.append(build_attack_record(
                attack_type="medical_debt_accuracy",
                bureau=bureau,
                accounts=[acc],
                strategy_tags=["FCRA_1681e_b", "FCRA_1681i_a"],
                reason=(
                    f"{name} (Account #{acct_num}) is a medical collection. "
                    f"Medical debt is uniquely prone to inaccuracy — billing "
                    f"errors, incorrect insurance applications, disputed charges, "
                    f"and balance inflation are documented systemic problems in "
                    f"healthcare billing. Under 15 U.S.C. §1681e(b), this bureau "
                    f"must maintain maximum possible accuracy. I am disputing the "
                    f"accuracy of this medical collection and requesting full "
                    f"verification: itemized bill, proof of insurance applied, "
                    f"original creditor name, and the exact amount owed."
                ),
            ))

    return attacks


# =========================
# MULTI-SOURCE ENGINE
# =========================
#
# Architecture:
#   Level 1 — detect_source()        → identifies which service generated the PDF
#   Level 2 — source adapters        → convert each format to canonical inventory
#   Level 3 — detection engine       → unchanged, operates on canonical structure
#
# Canonical account keys (same regardless of source):
#   bureau, name, account_number, status, payment_status, balance, past_due,
#   comments, date_opened, date_last_active, date_of_last_payment, last_reported,
#   account_type_detail, high_credit, credit_limit, monthly_payment, raw_lines
#
# Sources supported:
#   "identityiq"              — 3-bureau side-by-side PDF (already implemented)
#   "bureau_direct_tu"        — TransUnion direct PDF (one bureau)
#   "bureau_direct_exp"       — Experian direct PDF (one bureau)
#   "bureau_direct_eq"        — Equifax direct PDF (one bureau)
#   "myfico"                  — myFICO 3-bureau PDF
#   "smartcredit"             — SmartCredit 3-bureau PDF
#   "unknown"                 — fallback to IdentityIQ parser


SOURCE_IDENTITYIQ       = "identityiq"
SOURCE_BUREAU_DIRECT_TU = "bureau_direct_tu"
SOURCE_BUREAU_DIRECT_EXP= "bureau_direct_exp"
SOURCE_BUREAU_DIRECT_EQ = "bureau_direct_eq"
SOURCE_MYFICO           = "myfico"
SOURCE_SMARTCREDIT      = "smartcredit"
SOURCE_UNKNOWN          = "unknown"


def detect_source(text: str) -> str:
    """
    Identify which service generated this credit report PDF.
    Returns one of the SOURCE_* constants.
    """
    t = text[:3000].lower()

    if "identityiq" in t or "identityiq.com" in t:
        return SOURCE_IDENTITYIQ
    if "myscoreiq" in t:
        return SOURCE_IDENTITYIQ  # same format

    if "myfico" in t or "myfico.com" in t:
        return SOURCE_MYFICO

    if "smartcredit" in t or "smart credit" in t:
        return SOURCE_SMARTCREDIT

    # Bureau-direct signatures
    if "transunion" in t and "experian" not in t and "equifax" not in t:
        if any(k in t for k in ["transunion credit report", "your transunion", "tu credit"]):
            return SOURCE_BUREAU_DIRECT_TU

    if "experian" in t and "transunion" not in t and "equifax" not in t:
        if any(k in t for k in ["experian credit report", "your experian"]):
            return SOURCE_BUREAU_DIRECT_EXP

    if "equifax" in t and "transunion" not in t and "experian" not in t:
        if any(k in t for k in ["equifax credit report", "your equifax"]):
            return SOURCE_BUREAU_DIRECT_EQ

    # Single-bureau heuristic: if only one bureau is prominent
    has_tu  = "transunion" in t
    has_exp = "experian" in t
    has_eq  = "equifax" in t

    if has_tu and not has_exp and not has_eq:
        return SOURCE_BUREAU_DIRECT_TU
    if has_exp and not has_tu and not has_eq:
        return SOURCE_BUREAU_DIRECT_EXP
    if has_eq and not has_tu and not has_exp:
        return SOURCE_BUREAU_DIRECT_EQ

    return SOURCE_UNKNOWN


def parse_bureau_direct(
    lines: list[str],
    bureau: str,
    source: str,
) -> list[dict[str, Any]]:
    """
    Parse a single-bureau credit report PDF (AnnualCreditReport.com, bureau-direct).

    Bureau-direct reports have one bureau's data per PDF. Each account block
    has labeled rows WITHOUT the 3-column side-by-side format of IdentityIQ.
    Field labels use the same names but values are single (not triplicated).

    Field label variations across bureaus:
      TransUnion:  "Account Status:" / "Payment Status:" / "Balance:"
      Experian:    same labels, sometimes "Account Condition:" for status
      Equifax:     same labels, sometimes "Remarks:" for comments

    Returns canonical account list — same structure as IdentityIQ inventory.
    """
    import re as _re
    accounts: list[dict[str, Any]] = []

    # Account block markers — bureau-direct reports usually show account name
    # on its own line followed by a line of dashes or account details
    current: dict[str, Any] | None = None

    def _val(line: str, label: str) -> str:
        """Extract value after 'Label:' removing noise."""
        if label.lower() in line.lower():
            idx = line.lower().index(label.lower()) + len(label)
            return line[idx:].strip().lstrip(":").strip()
        return ""

    def _save(acc: dict | None) -> None:
        if acc and acc.get("name") and acc.get("account_number"):
            acc.setdefault("bureau",              bureau)
            acc.setdefault("status",              "")
            acc.setdefault("payment_status",      "")
            acc.setdefault("balance",             "$0.00")
            acc.setdefault("past_due",            "$0.00")
            acc.setdefault("high_credit",         "$0.00")
            acc.setdefault("credit_limit",        "$0.00")
            acc.setdefault("monthly_payment",     "$0.00")
            acc.setdefault("comments",            "")
            acc.setdefault("date_opened",         "")
            acc.setdefault("date_last_active",    "")
            acc.setdefault("date_of_last_payment","")
            acc.setdefault("last_reported",       "")
            acc.setdefault("account_type",        "")
            acc.setdefault("account_type_detail", "")
            acc.setdefault("possible_duplicate_group", "")
            acc.setdefault("block_id",            safe_lower(acc["name"])[:12].replace(" ",""))
            acc.setdefault("raw_lines",           [])
            accounts.append(acc)

    # Field label map — handles label variants across bureaus
    LABEL_MAP = {
        "account number":       "account_number",
        "account #":            "account_number",
        "account status":       "status",
        "account condition":    "status",   # Experian variant
        "account standing":     "status",   # SmartCredit variant
        "payment status":       "payment_status",
        "payment history":      "payment_status",
        "payment rating":       "payment_status",   # Equifax variant
        "payment condition":    "payment_status",
        "balance":              "balance",
        "balance owed":         "balance",
        "amount owed":          "balance",
        "past due":             "past_due",
        "amount past due":      "past_due",
        "high balance":         "high_credit",
        "high credit":          "high_credit",
        "original amount":      "high_credit",
        "credit limit":         "credit_limit",
        "scheduled payment":    "monthly_payment",
        "monthly payment":      "monthly_payment",
        "payment amount":       "monthly_payment",
        "date opened":          "date_opened",
        "opened":               "date_opened",
        "date of last activity":"date_last_active",
        "last activity":        "date_last_active",
        "date last active":     "date_last_active",
        "date of last payment": "date_of_last_payment",
        "last payment":         "date_of_last_payment",
        "last reported":        "last_reported",
        "date reported":        "last_reported",
        "reported":             "last_reported",
        "account type":         "account_type",
        "type":                 "account_type",
        "comments":             "comments",
        "remarks":              "comments",
    }

    date_re   = _re.compile(r'\b\d{1,2}/\d{1,2}/\d{4}\b|\b\d{4}\b')
    dollar_re = _re.compile(r'\$[\d,]+\.?\d*')

    i = 0
    while i < len(lines):
        line = lines[i].strip()
        lo   = line.lower()

        # Detect account block start — line that looks like a creditor name
        # followed by account number or type info on the next lines
        # Heuristic: all-caps or title-case word not in skip list, followed by
        # account-specific labels within 3 lines
        is_account_header = (
            len(line) > 3
            and not any(skip in lo for skip in [
                "personal information", "credit score", "inquiries",
                "public records", "account history", "summary",
                "payment history", "http", "credit report",
                "date of birth", "address", "employer",
                "page", "4/", "annual credit",
                "account information", "credit accounts",
                "transunion credit", "experian credit", "equifax credit",
                "report date", "report as of", "consumer:", "accounts",
                "negative items", "positive accounts", "open accounts",
            ])
            and _re.match(r'^[A-Z0-9][A-Z0-9\s\-\/\.,&\']+$', line)
            and len(line) < 70
            and i + 1 < len(lines)
            and any(
                label in lines[i+1].lower()
                for label in ["account", "balance", "opened", "status", "payment", "type", "reported"]
            )
        )

        if is_account_header:
            _save(current)
            current = {"name": line, "raw_lines": [line]}
            i += 1
            continue

        if current is not None:
            current["raw_lines"].append(line)

            # Try to match a field label
            matched = False
            for label, field in LABEL_MAP.items():
                if lo.startswith(label + ":") or lo.startswith(label + " :"):
                    val = line[len(label)+1:].strip().lstrip(":").strip()
                    if val and not current.get(field):
                        current[field] = val
                    matched = True
                    break

            # Stop block on blank line or next header signal
            if not line and current.get("account_number"):
                _save(current)
                current = None

        i += 1

    _save(current)
    return accounts


def build_report_single_bureau(
    pdf_path: str,
    bureau: str,
) -> dict[str, Any]:
    """
    Process a single-bureau PDF (AnnualCreditReport.com style).
    Returns same structure as build_report() but with data only for one bureau.
    Use build_report_multi() to combine three single-bureau reports.
    """
    raw_text   = extract_text_from_pdf(pdf_path)
    clean_text = normalize_text(raw_text)
    lines      = split_lines(clean_text)

    source = detect_source(raw_text[:3000])

    if source == SOURCE_IDENTITYIQ:
        # IdentityIQ — use normal pipeline, just return one bureau's slice
        full = build_report(pdf_path)
        sliced_inv = {bureau: full["inventory_by_bureau"].get(bureau, [])}
        return {"source": source, "bureau": bureau, "inventory": sliced_inv}

    # Bureau-direct path
    accounts = parse_bureau_direct(lines, bureau=bureau, source=source)

    import re as _re
    rd_match = _re.search(r'(?:Report Date|Generated|As of)[:\s]*([\d/]+)', raw_text[:2000])
    report_date_str = rd_match.group(1) if rd_match else ""

    personal_info, personal_info_issues = parse_and_detect_personal_info(lines)

    return {
        "source":              source,
        "bureau":              bureau,
        "report_date":         report_date_str,
        "raw_accounts":        len(accounts),
        "accounts":            accounts,
        "personal_info":       personal_info,
        "personal_info_issues":personal_info_issues,
    }


def build_report_multi(pdf_paths: dict[str, str]) -> dict[str, Any]:
    """
    Process multiple PDFs — one per bureau — and combine into a single result.
    Use this when the client uploads AnnualCreditReport.com bureau-by-bureau reports.

    pdf_paths: {"transunion": "/path/tu.pdf", "experian": "/path/exp.pdf", "equifax": "/path/eq.pdf"}

    Returns the same structure as build_report() so the rest of the pipeline
    (detection engine, letter engine, etc.) works without changes.
    """
    import re as _re

    all_inventory: dict[str, list] = {b: [] for b in BUREAUS}
    report_date_str = ""
    all_pi: dict[str, Any] = {}
    all_pi_issues: list = []
    all_lines: list[str] = []
    all_inquiries: list[dict] = []

    for bureau, pdf_path in pdf_paths.items():
        if not pdf_path:
            continue
        raw_text   = extract_text_from_pdf(pdf_path)
        clean_text = normalize_text(raw_text)
        lines      = split_lines(clean_text)
        all_lines.extend(lines)

        source = detect_source(raw_text[:3000])

        if source == SOURCE_IDENTITYIQ:
            # IdentityIQ — extract just this bureau's data
            full = build_report(pdf_path)
            all_inventory[bureau] = full["inventory_by_bureau"].get(bureau, [])
            if not report_date_str:
                report_date_str = full.get("report_date", "")
            if not all_pi:
                all_pi = full.get("personal_info", {})
                all_pi_issues = full.get("personal_info_issues", [])
            all_inquiries.extend(full.get("inquiries", []))
        else:
            # Bureau-direct
            accounts = parse_bureau_direct(lines, bureau=bureau, source=source)
            all_inventory[bureau] = accounts

            if not report_date_str:
                rd = _re.search(r'(?:Report Date|Generated|As of)[:\s]*([\d/]+)', raw_text[:2000])
                if rd:
                    report_date_str = rd.group(1)

            pi, pi_issues = parse_and_detect_personal_info(lines)
            if not all_pi:
                all_pi = pi
            all_pi_issues.extend(pi_issues)
            all_inquiries.extend(parse_inquiries(lines))

    # Now run the full detection pipeline on the combined inventory
    # Normalize through same pipeline as single-report
    negatives_by_bureau = build_negative_inventory_by_bureau(all_inventory)
    negatives_by_bureau = build_dofd_engine(negatives_by_bureau, report_date_str)

    # Build a fake base_tradeline_engine from the multi-bureau inventory
    # (needed for cross-bureau attack detection)
    base_tradeline_engine: dict[str, list] = {}
    for bureau, accs in all_inventory.items():
        for acc in accs:
            block_id = acc.get("block_id", "")
            if block_id:
                base_tradeline_engine.setdefault(block_id, []).append(acc)

    inquiry_attacks = detect_inquiry_attacks(all_inquiries)
    inquiry_letters = build_inquiry_letters(
        all_inquiries, consumer_name="[CLIENT NAME]", report_date=report_date_str
    )

    legal_detection_engine  = build_legal_detection_engine(negatives_by_bureau, base_tradeline_engine)
    legal_detection_summary = build_legal_detection_summary(negatives_by_bureau, legal_detection_engine)
    attack_scoring_engine   = build_attack_scoring_engine(legal_detection_engine)
    strategy_engine         = build_strategy_engine(attack_scoring_engine)
    letter_input_engine     = build_letter_input_engine(strategy_engine, negatives_by_bureau)
    dispute_letters         = build_dispute_letter_engine(
        letter_input_engine,
        consumer_name="[CLIENT NAME]",
        report_date=report_date_str,
        personal_info=all_pi,
        personal_info_issues=all_pi_issues,
    )
    furnisher_letters = build_furnisher_letter_engine(
        letter_input_engine, consumer_name="[CLIENT NAME]", report_date=report_date_str
    )

    return {
        "source":               "multi_bureau",
        "report_date":          report_date_str,
        "personal_info":        all_pi,
        "personal_info_issues": all_pi_issues,
        "inventory_by_bureau":  all_inventory,
        "negatives_by_bureau":  negatives_by_bureau,
        "inquiries":            all_inquiries,
        "inquiry_attacks":      inquiry_attacks,
        "inquiry_letters":      inquiry_letters,
        "legal_detection_engine":  legal_detection_engine,
        "legal_detection_summary": legal_detection_summary,
        "attack_scoring_engine":   attack_scoring_engine,
        "strategy_engine":         strategy_engine,
        "letter_input_engine":     letter_input_engine,
        "dispute_letters":         dispute_letters,
        "furnisher_letters":       furnisher_letters,
    }


def parse_inquiries(lines: list[str]) -> list[dict[str, Any]]:
    """
    Parse the Inquiries section of an IdentityIQ 3-bureau PDF.

    Each inquiry line format:
        CREDITOR_NAME   TYPE_OF_BUSINESS   MM/DD/YYYY   Bureau

    Returns list of dicts with keys:
        creditor_name, business_type, date, bureau
    """
    import re as _re
    inquiries: list[dict[str, Any]] = []

    in_section = False
    date_pattern = _re.compile(r'\b\d{2}/\d{2}/\d{4}\b')
    bureau_names = {"transunion", "experian", "equifax"}

    for i, line in enumerate(lines):
        # Detect section start
        if "inquiries" in line.lower() and "back to top" in line.lower():
            in_section = True
            continue
        # Detect section end
        if in_section and ("public information" in line.lower() or
                           "creditor contacts" in line.lower()):
            break
        if not in_section:
            continue
        # Skip header lines and noise
        if any(skip in line.lower() for skip in [
            "below are the names", "can remain on your credit",
            "creditor name", "type of business", "http", "4/", "credit report -"
        ]):
            continue

        # Parse: look for a date in the line
        m = date_pattern.search(line)
        if not m:
            continue

        date_str   = m.group(0)
        before     = line[:m.start()].strip()
        after      = line[m.end():].strip()

        # Determine bureau — last token after date
        bureau = ""
        for b in bureau_names:
            if b in after.lower() or after.lower() == b[:len(after)].lower():
                bureau = b
                break
        if not bureau:
            # Try matching by capitalized bureau name
            after_lower = after.strip().lower()
            if "transunion" in after_lower:
                bureau = "transunion"
            elif "experian" in after_lower:
                bureau = "experian"
            elif "equifax" in after_lower:
                bureau = "equifax"
            else:
                continue  # can't determine bureau, skip

        # Split 'before' into name and business type
        # Business types tend to end the 'before' portion
        business_types = [
            "Auto Financing", "Auto Dealers, Used", "Auto Dealers, New",
            "Personal Loan Companies", "Bank Credit Cards", "Miscellaneous Finance",
            "Jewelers", "Mortgage Companies", "Finance Companies",
        ]
        name = before
        btype = "-"
        for bt in business_types:
            if bt.lower() in before.lower():
                idx = before.lower().index(bt.lower())
                name  = before[:idx].strip()
                btype = bt
                break

        if not name:
            name = before

        inquiries.append({
            "creditor_name":  name.strip(),
            "business_type":  btype,
            "date":           date_str,
            "bureau":         bureau,
        })

    return inquiries


def detect_inquiry_attacks(inquiries: list[dict[str, Any]],
                           accounts_opened: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    """
    Detect inquiry-level §1681b permissible purpose violations.

    Four attack types:

    TYPE A — duplicate_inquiry_same_creditor
        Same creditor + same bureau + same date, pulled 2+ times.
        No single application generates two pulls — each requires
        an independent permissible purpose.

    TYPE B — repeat_inquiry_no_account
        Same creditor + same bureau on 2+ different dates with no
        resulting account opened near those dates.
        Multiple pulls separated by weeks/months suggests the
        creditor kept pulling without a new application each time.

    TYPE C — inquiry_cluster_same_day
        5+ different creditors on the same day at the same bureau.
        While rate-shopping for auto or mortgage allows a 14-day
        window under §1681b(c)(3), dealer pulls and financing pulls
        are counted separately. Beyond the rate-shopping window,
        each requires its own permissible purpose.

    TYPE D — creditor_pulled_multiple_bureaus
        Same creditor on the same date across 2+ bureaus.
        Each bureau pull is a separate permissible purpose event.
        Unless the consumer applied at multiple places, this may
        indicate unauthorized sharing of the initial pull.
    """
    from collections import defaultdict
    attacks: list[dict[str, Any]] = []

    if not inquiries:
        return attacks

    # Index by key combinations
    by_exact:        dict[tuple, list] = defaultdict(list)  # (name, bureau, date)
    by_name_bureau:  dict[tuple, list] = defaultdict(list)  # (name, bureau)
    by_date_bureau:  dict[tuple, list] = defaultdict(list)  # (date, bureau)
    by_name_date:    dict[tuple, set]  = defaultdict(set)   # (name, date) → bureaus

    for inq in inquiries:
        n = inq["creditor_name"]
        b = inq["bureau"]
        d = inq["date"]
        by_exact[(n, b, d)].append(inq)
        by_name_bureau[(n, b)].append(inq)
        by_date_bureau[(d, b)].append(inq)
        by_name_date[(n, d)].add(b)

    seen: set = set()

    # ── TYPE A: Exact duplicate ──────────────────────────────────────────
    for (name, bureau, date), inqs in by_exact.items():
        if len(inqs) > 1:
            key = ("dup", name, bureau, date)
            if key not in seen:
                seen.add(key)
                attacks.append({
                    "attack_type":   "duplicate_inquiry_same_creditor",
                    "bureau":        bureau,
                    "creditor_name": name,
                    "date":          date,
                    "count":         len(inqs),
                    "laws":          ["15 USC 1681b", "15 USC 1681n"],
                    "severity":      "high",
                    "reason": (
                        f"{name} pulled my {bureau.title()} credit report "
                        f"{len(inqs)} times on {date}. Each credit inquiry "
                        f"requires a separate permissible purpose under "
                        f"15 U.S.C. \u00a71681b. A single application "
                        f"generates one pull — not two. The duplicate inquiry "
                        f"has no valid permissible purpose and must be removed."
                    ),
                })

    # ── TYPE B: Same creditor, multiple dates, no resulting account ───────
    for (name, bureau), inqs in by_name_bureau.items():
        dates = sorted(set(i["date"] for i in inqs))
        if len(dates) > 1:
            key = ("repeat", name, bureau)
            if key not in seen:
                seen.add(key)
                attacks.append({
                    "attack_type":   "repeat_inquiry_no_account",
                    "bureau":        bureau,
                    "creditor_name": name,
                    "dates":         dates,
                    "count":         len(dates),
                    "laws":          ["15 USC 1681b"],
                    "severity":      "medium",
                    "reason": (
                        f"{name} pulled my {bureau.title()} credit report "
                        f"{len(dates)} times on different dates "
                        f"({', '.join(dates)}). Each pull requires a new "
                        f"permissible purpose — a separate application or "
                        f"authorization. Without evidence of separate "
                        f"applications, these repeat pulls may not have had "
                        f"a valid permissible purpose under 15 U.S.C. \u00a71681b."
                    ),
                })

    # ── TYPE C: Cluster — 5+ creditors same day same bureau ──────────────
    for (date, bureau), inqs in by_date_bureau.items():
        unique_creditors = list(dict.fromkeys(i["creditor_name"] for i in inqs))
        if len(unique_creditors) >= 5:
            key = ("cluster", date, bureau)
            if key not in seen:
                seen.add(key)
                # Check if they're all auto-related (rate-shopping exception)
                auto_terms = {"auto", "dealer", "financial", "credit", "motor", "car", "vehicle"}
                biz_types  = [i.get("business_type","").lower() for i in inqs]
                is_auto    = sum(1 for bt in biz_types if any(t in bt for t in auto_terms))
                pct_auto   = is_auto / len(inqs) if inqs else 0

                context = (
                    "These appear to be auto-related inquiries. While §1681b(c)(3) "
                    "allows a 14-day rate-shopping window for auto loans, dealer "
                    "pulls and financing company pulls are counted separately, and "
                    "each requires that the consumer initiated an application. "
                    if pct_auto > 0.6 else
                    ""
                )

                attacks.append({
                    "attack_type":   "inquiry_cluster_same_day",
                    "bureau":        bureau,
                    "date":          date,
                    "count":         len(unique_creditors),
                    "creditors":     unique_creditors,
                    "laws":          ["15 USC 1681b"],
                    "severity":      "medium",
                    "reason": (
                        f"My {bureau.title()} credit report was pulled by "
                        f"{len(unique_creditors)} different creditors on {date}. "
                        f"{context}"
                        f"Each inquiry requires an independent permissible purpose "
                        f"under 15 U.S.C. \u00a71681b. I am requesting that each "
                        f"creditor confirm the permissible purpose for their inquiry, "
                        f"and that any pull I did not authorize be removed."
                    ),
                })

    # ── TYPE D: Same creditor across multiple bureaus same day ────────────
    for (name, date), bureaus in by_name_date.items():
        if len(bureaus) > 1:
            key = ("multi_bureau", name, date)
            if key not in seen:
                seen.add(key)
                attacks.append({
                    "attack_type":   "inquiry_multi_bureau_same_day",
                    "bureaus":       sorted(bureaus),
                    "creditor_name": name,
                    "date":          date,
                    "laws":          ["15 USC 1681b"],
                    "severity":      "medium",
                    "reason": (
                        f"{name} pulled my credit report at "
                        f"{' and '.join(b.title() for b in sorted(bureaus))} "
                        f"on the same date ({date}). Pulling multiple bureaus "
                        f"without separate consumer authorizations may indicate "
                        f"that the initial inquiry was shared or resold, which "
                        f"requires a permissible purpose for each separate pull "
                        f"under 15 U.S.C. \u00a71681b."
                    ),
                })

    return attacks


def build_inquiry_dispute_letter(
    bureau: str,
    attacks: list[dict[str, Any]],
    consumer_name: str,
    report_date: str = "",
) -> str:
    """
    Generate an inquiry dispute letter for one bureau.
    Inquiries are separate from account disputes — they go in their own letter.
    """
    bureau_info    = BUREAU_ADDRESSES.get(bureau, {})
    bureau_name    = bureau_info.get("name", bureau.title())
    bureau_address = bureau_info.get("address", "")
    date_str       = _format_date_long(report_date)

    header = (
        f"{consumer_name}\n[Address]\n[City, State ZIP]\n\n"
        f"{bureau_name}\n{bureau_address}\n\n"
        f"{date_str}"
    )

    opening = (
        f"Hi,\n\n"
        f"I am writing to dispute unauthorized or improperly obtained "
        f"credit inquiries on my {bureau_name} credit report. Under "
        f"15 U.S.C. \u00a71681b, a credit inquiry is only permissible "
        f"when the consumer has applied for credit or otherwise authorized "
        f"the pull. Each inquiry listed below either lacks a valid "
        f"permissible purpose, is a duplicate, or was obtained without "
        f"proper authorization. I am requesting that each be investigated "
        f"and removed."
    )

    inquiry_lines = []
    for idx, atk in enumerate(attacks, 1):
        inquiry_lines.append(
            f"{idx}. {atk.get('creditor_name', atk.get('creditors', ['?'])[0])}\n"
            f"Reason: {atk['reason']} DELETE OFF MY CREDIT REPORT."
        )

    legal_note = (
        f"Unauthorized inquiries damage my credit score and constitute "
        f"a violation of 15 U.S.C. \u00a71681b. Willful violations are "
        f"subject to statutory damages of $100 to $1,000 per violation "
        f"plus punitive damages and attorney fees under 15 U.S.C. \u00a71681n. "
        f"Please complete your investigation within 30 days as required "
        f"by 15 U.S.C. \u00a71681i(a)(1) and provide written results."
    )

    return (
        header + "\n\n"
        + opening + "\n\n\n"
        + "The following inquiries must be removed:\n\n"
        + "\n\n".join(inquiry_lines) + "\n\n\n"
        + legal_note + "\n\n"
        + consumer_name
    )


def build_inquiry_letters(
    inquiries: list[dict[str, Any]],
    consumer_name: str,
    report_date: str = "",
) -> dict[str, str]:
    """
    Build one inquiry dispute letter per bureau that has attacks.
    Returns {bureau: letter_text}.
    """
    attacks_by_bureau: dict[str, list] = {}
    all_attacks = detect_inquiry_attacks(inquiries)

    for atk in all_attacks:
        bureau = atk.get("bureau", "")
        if not bureau:
            # Multi-bureau attacks — add to each bureau
            for b in atk.get("bureaus", []):
                attacks_by_bureau.setdefault(b, []).append(atk)
        else:
            attacks_by_bureau.setdefault(bureau, []).append(atk)

    letters = {}
    for bureau, bureau_attacks in attacks_by_bureau.items():
        if bureau_attacks:
            letters[bureau] = build_inquiry_dispute_letter(
                bureau=bureau,
                attacks=bureau_attacks,
                consumer_name=consumer_name,
                report_date=report_date,
            )
    return letters


def extract_scores(lines: list[str]) -> dict[str, int]:
    """
    Extract credit scores from IdentityIQ PDF.
    Looks for the Credit Score section which has:
        Credit Score:   646   628   645
    Returns {"transunion": 646, "experian": 628, "equifax": 645}
    """
    import re as _re
    scores = {"transunion": 0, "experian": 0, "equifax": 0}

    for i, line in enumerate(lines):
        if line.strip().lower().startswith("credit score:"):
            # Extract digits from this line
            vals = _re.findall(r'\b(\d{3})\b', line)
            if len(vals) >= 3:
                scores["transunion"] = int(vals[0])
                scores["experian"]   = int(vals[1])
                scores["equifax"]    = int(vals[2])
                break
            # Sometimes split across next lines — scan next 3 lines
            combined = line
            for j in range(1, 4):
                if i + j < len(lines):
                    combined += " " + lines[i + j]
            vals = _re.findall(r'\b(\d{3})\b', combined)
            if len(vals) >= 3:
                scores["transunion"] = int(vals[0])
                scores["experian"]   = int(vals[1])
                scores["equifax"]    = int(vals[2])
            break

    return scores


def build_report(pdf_path: str) -> dict[str, Any]:
    raw_text = extract_text_from_pdf(pdf_path)
    clean_text = normalize_text(raw_text)
    lines = split_lines(clean_text)

    raw_accounts = parse_raw_account_blocks(lines)
    base_tradeline_engine = build_base_tradeline_engine(raw_accounts)
    same_block_cross_bureau_summary = build_same_block_cross_bureau_summary(base_tradeline_engine)

    # Personal information — extracted early, before account pipeline
    personal_info, personal_info_issues = parse_and_detect_personal_info(lines)

    # Credit scores
    scores = extract_scores(lines)

    inventory = build_inventory_by_bureau(raw_accounts)
    inventory = normalize_inventory_final(inventory)

    negatives_by_bureau = build_negative_inventory_by_bureau(inventory)

    # Enrich negatives with DOFD calculations (§1681c)
    import re as _re
    rd_match = _re.search(r"Report Date:\s*([\d/]+)", raw_text[:2000])
    report_date_str = rd_match.group(1) if rd_match else ""
    negatives_by_bureau = build_dofd_engine(negatives_by_bureau, report_date_str)

    # Inquiries — analyzed after report_date is known
    inquiries       = parse_inquiries(lines)
    inquiry_attacks = detect_inquiry_attacks(inquiries)
    inquiry_letters = build_inquiry_letters(
        inquiries, consumer_name="[CLIENT NAME]", report_date=report_date_str
    )

    legal_detection_engine = build_legal_detection_engine(
        negatives_by_bureau, base_tradeline_engine,
        report_date=report_date_str,
        client_state="",          # populated when called via API with client profile
    )
    legal_detection_summary = build_legal_detection_summary(
        negatives_by_bureau,
        legal_detection_engine
    )
    attack_scoring_engine = build_attack_scoring_engine(legal_detection_engine)
    strategy_engine = build_strategy_engine(attack_scoring_engine)
    letter_input_engine  = build_letter_input_engine(strategy_engine, negatives_by_bureau)
    dispute_letters      = build_dispute_letter_engine(
        letter_input_engine,
        consumer_name="[CLIENT NAME]",
        report_date=report_date_str,
        personal_info=personal_info,
        personal_info_issues=personal_info_issues,
    )
    furnisher_letters    = build_furnisher_letter_engine(letter_input_engine, consumer_name="[CLIENT NAME]", report_date=report_date_str)

    expanded_accounts_found = sum(len(v) for v in inventory.values())

    return {
        "raw_accounts": len(raw_accounts),
        "expanded_accounts_found": expanded_accounts_found,
        "report_date": report_date_str,
        "scores": scores,
        "personal_info": personal_info,
        "personal_info_issues": personal_info_issues,
        "inquiries": inquiries,
        "inquiry_attacks": inquiry_attacks,
        "inquiry_letters": inquiry_letters,
        "base_tradeline_engine": base_tradeline_engine,
        "same_block_cross_bureau_summary": same_block_cross_bureau_summary,
        "inventory_by_bureau": inventory,
        "negatives_by_bureau": negatives_by_bureau,
        "legal_detection_engine": legal_detection_engine,
        "legal_detection_summary": legal_detection_summary,
        "attack_scoring_engine": attack_scoring_engine,
        "strategy_engine": strategy_engine,
        "letter_input_engine": letter_input_engine,
        "dispute_letters": dispute_letters,
        "furnisher_letters": furnisher_letters,
    }

_COLLECTOR_ATTACK_TYPES = {
    # Chain of title / authority attacks — definitionally collector-only
    "collector_original_creditor_self_declared",
    "collector_original_creditor_pattern",
    # Duplicate reporting — applies to collectors
    "same_account_number_same_balance",
    "duplicate_account_number",
    "multi_furnisher_same_balance",
    # DOFD / re-aging — applies to collectors manipulating dates
    "potential_re_aging",
    "dofd_unknown_verification_required",
    # Account type attacks — only when furnisher is confirmed collector
    "paid_collection_still_derogatory",
    "child_support_derogatory",
    # Note: repossession/charge_off attacks on original creditors are NOT here
    # because original creditors get bureau-channel letters only (not FDCPA)
}


def _normalize_collector_name(name: str) -> str:
    """
    Normalize collector names so that abbreviated and full-name versions
    of the same collector are treated as one entity for deduplication.
    e.g. NCA == NATIONAL CREDIT ADJUST, CREDENCE RM == CREDENCE RESOURCE MANA
    """
    n = name.upper().strip()
    # Remove original creditor suffix for normalization key only
    if "(ORIGINAL CREDITOR:" in n:
        n = n[:n.index("(ORIGINAL CREDITOR:")].strip()
    # Known normalizations
    aliases = {
        "NCA":             "NATIONAL CREDIT ADJUST",
        "CREDENCE RM":     "CREDENCE RESOURCE MANA",
        "CRCORPSOL":       "CREDIT CORP SOLUTIONS",
        "CREDIT COR":      "CREDIT CORP SOLUTIONS",
        "DNF ASSOC":       "DNF ASSOCIATES",
        "PORTFOLIO RC":    "PORTFOLIO RECOVERY",
        "PORTFOLIO":       "PORTFOLIO RECOVERY",
        "LVNV FUNDING LLC": "LVNV FUNDING",
        "CAVALRY PORT":    "CAVALRY PORTFOLIO",
        "CAVALRY SPV":     "CAVALRY PORTFOLIO",
    }
    for short, full in aliases.items():
        if n == short or n.startswith(short + " "):
            return full
    return n


def _is_collector_account(item: dict[str, Any]) -> bool:
    """
    Determine if an account should get a direct furnisher letter.
    Criteria: attack type is collector-specific AND §1681s-2 appears in laws.
    """
    attack = item.get("attack_type", "")
    laws   = item.get("laws", [])
    has_furnisher_law = any("1681s-2" in l for l in laws)
    return attack in _COLLECTOR_ATTACK_TYPES and has_furnisher_law


def _collector_letter_address(furnisher_name: str) -> str:
    """
    Known collector addresses. Falls back to generic placeholder.
    In production this would be a database lookup.
    """
    known = {
        "NATIONAL CREDIT ADJUST": "National Credit Adjusters LLC\nP.O. Box 550\nHutchinson, KS 67504",
        "NCA":                    "National Credit Adjusters LLC\nP.O. Box 550\nHutchinson, KS 67504",
        "CREDENCE RESOURCE MANA": "Credence Resource Management\n4222 Trinity Mills Suite 260\nDallas, TX 75287",
        "CREDENCE RM":            "Credence Resource Management\n4222 Trinity Mills Suite 260\nDallas, TX 75287",
        "CREDIT CORP SOLUTIONS":  "Credit Corp Solutions Inc\n121 W Election Road Suite 200\nDraper, UT 84020",
        "CRCORPSOL":              "Credit Corp Solutions Inc\n121 W Election Road Suite 200\nDraper, UT 84020",
        "CREDIT COR":             "Credit Corp Solutions Inc\n121 W Election Road Suite 200\nDraper, UT 84020",
        "DNF ASSOC":              "DNF Associates LLC\n2351 N Forest Road Suite 110\nGetzville, NY 14068",
        "DNF ASSOCIATES":         "DNF Associates LLC\n2351 N Forest Road Suite 110\nGetzville, NY 14068",
        "FIRST CREDIT FINANCE":   "First Credit Finance\n16005 Sherman Way Suite 20\nVan Nuys, CA 91406",
        "LVNV FUNDING LLC": "LVNV Funding LLC\nP.O. Box 10587\nGreenville, SC 29603",
        "PORTFOLIO":       "Portfolio Recovery Associates LLC\nP.O. Box 41067\nNorfolk, VA 23541",
        "PORTFOLIO RC":    "Portfolio Recovery Associates LLC\nP.O. Box 41067\nNorfolk, VA 23541",
        "CAVALRY PORT":    "Cavalry Portfolio Services LLC\nP.O. Box 27288\nTempe, AZ 85285",
        "CAVALRY SPV":     "Cavalry Portfolio Services LLC\nP.O. Box 27288\nTempe, AZ 85285",
        "MIDLAND":         "Midland Credit Management\nP.O. Box 2011\nWarren, MI 48090",
        "MIDLAND CREDIT":  "Midland Credit Management\nP.O. Box 2011\nWarren, MI 48090",
        "JEFFERSON":       "Jefferson Capital Systems LLC\nP.O. Box 7999\nSaint Cloud, MN 56302",
        "JEFFCAPSYS":      "Jefferson Capital Systems LLC\nP.O. Box 7999\nSaint Cloud, MN 56302",
        "ALDOUS":          "Aldous & Associates\n6322 S 3000 E Suite 200\nSalt Lake City, UT 84121",
        "CELTIC":          "Celtic Bank Corporation\n268 S State St Suite 300\nSalt Lake City, UT 84111",
    }
    fname_upper = furnisher_name.upper()
    for key, addr in known.items():
        if key in fname_upper:
            return addr
    # Generic fallback
    return f"{furnisher_name}\n[Collector Address]\n[City, State ZIP]"


def _furnisher_account_demand(item: dict[str, Any]) -> str:
    """
    Specific demand paragraph for one account in the furnisher letter.
    Different from bureau letter — this goes directly to the collector.
    Tone: firm, legally precise, but still written in first person.
    """
    furnisher   = item.get("furnisher_name", "")
    acct        = item.get("account_number", "")
    attack_type = item.get("attack_type", "")
    dofd        = item.get("dofd_estimated")
    fcra_exp    = item.get("fcra_expiration")
    dla_refresh = item.get("dla_suspected_refresh", False)
    balance     = item.get("balance", "")

    if attack_type == "potential_re_aging":
        return (
            f"Account #: {acct}\n"
            f"I am disputing this account because the date you are showing as when "
            f"this account started does not match when I actually first fell behind "
            f"on the original obligation, which was around {dofd}. Under the Fair "
            f"Credit Reporting Act (15 U.S.C. \u00a71681c(c)), the seven-year "
            f"reporting period runs from when I first missed a payment — not from "
            f"when your company acquired the debt. I am requesting that you provide "
            f"documentation of the original date of first delinquency from the "
            f"original creditor, and correct the reporting period accordingly. "
            f"This account should expire around {fcra_exp}. If you cannot provide "
            f"this documentation, you must cease reporting this account."
        )

    elif attack_type == "dofd_unknown_verification_required":
        if dla_refresh:
            return (
                f"Account #: {acct}\n"
                f"I am disputing this account. The date last active you are reporting "
                f"matches almost exactly the date this account was last reported to "
                f"the credit bureaus, which suggests you may be refreshing that date "
                f"rather than accurately disclosing when I first fell behind. Under "
                f"15 U.S.C. \u00a71681c(c), the reporting period must be calculated "
                f"from the original date of first delinquency. I am requesting that "
                f"you provide that date with supporting documentation. If you cannot, "
                f"you must cease reporting this account to any credit bureau."
            )
        else:
            return (
                f"Account #: {acct}\n"
                f"I am disputing this account. The information you are reporting does "
                f"not clearly disclose the date I first fell behind on the original "
                f"obligation. That date is required under 15 U.S.C. \u00a71681c(c) "
                f"to determine whether this account is within its legal reporting "
                f"window. I am requesting full documentation including the original "
                f"date of first delinquency. If you cannot provide it, you must "
                f"cease reporting this account."
            )

    elif attack_type in {
        "collector_original_creditor_self_declared",
        "collector_original_creditor_pattern",
    }:
        return (
            f"Account #: {acct}\n"
            f"I am disputing your authority to report this account on my credit file. "
            f"You are reporting this as a collection account, which means you must "
            f"be able to demonstrate that you are the legal holder of this debt. "
            f"I am requesting the following: (1) the original signed agreement "
            f"between me and the original creditor, (2) every assignment, sale, "
            f"or transfer document from the original creditor to your company, "
            f"(3) if this debt was securitized, the Pooling and Servicing Agreement "
            f"and the schedule of assets identifying this specific account, and "
            f"(4) proof that you hold the current legal right to collect and report "
            f"this debt under the Uniform Commercial Code and 15 U.S.C. \u00a71681s-2. "
            f"If you cannot produce all of this, you do not have the legal standing "
            f"to report this account and you must request its deletion."
        )

    elif attack_type in {
        "same_account_number_same_balance",
        "duplicate_account_number",
        "multi_furnisher_same_balance",
    }:
        bal_note = f" with a balance of {balance}" if balance and balance not in {"$0.00","0"} else ""
        return (
            f"Account #: {acct}\n"
            f"This account{bal_note} is appearing under more than one company name "
            f"on my credit report. Only one entity can legally hold and report a "
            f"single debt at any given time. If another company has also reported "
            f"this account, one of those reports is unauthorized. I am requesting "
            f"that you provide proof that you are the current legal holder of this "
            f"specific obligation — including the full chain of assignment from the "
            f"original creditor to your company — and that you correct or retract "
            f"any reporting that cannot be substantiated."
        )

    else:
        return (
            f"Account #: {acct}\n"
            f"I am disputing the accuracy and completeness of this account as you "
            f"are reporting it. I am requesting complete documentation including "
            f"the original agreement, full payment history, itemized balance, "
            f"the exact date of first delinquency, and proof of your legal authority "
            f"to report this account."
        )


def build_furnisher_letter_engine(
    letter_input_engine: dict[str, dict[str, list[dict[str, Any]]]],
    consumer_name: str = "[CLIENT NAME]",
    report_date: str = "",
) -> dict[str, dict[str, str]]:
    """
    Generate direct-to-collector letters (FDCPA §1692g + FCRA §1681s-2).

    Structure returned:
        {
          "collector_name": {
              "round_1": "full letter text",
              "round_2": "full letter text",   # only if warranted
          }
        }

    Rules:
    - One letter per unique collector (deduplicated across bureaus).
    - Every collector letter is sent in Round 1 — parallel to bureau letters.
      The bureau's round classification does NOT delay the furnisher letter.
    - Round 2 furnisher letter is generated for collectors whose accounts
      survived Round 1 (used when uploading next report cycle).
    - Covers all accounts from that collector across all bureaus in one letter.
    - Dual legal basis: FDCPA §1692g (cease until validated) +
                        FCRA §1681s-2(a) (accuracy duty) +
                        FCRA §1681s-2(b) (investigation duty after bureau notice)
    """
    formatted_date = _format_date_long(report_date)

    # ── Collect all collector accounts, deduplicated by furnisher name ──────
    # Key: normalized furnisher name
    # Value: list of unique accounts across all bureaus
    collector_accounts: dict[str, list[dict[str, Any]]] = {}

    seen_accts: dict[str, set[str]] = {}  # furnisher → set of account numbers seen

    for bureau, groups in letter_input_engine.items():
        for items in groups.values():
            for item in items:
                if not _is_collector_account(item):
                    continue

                fname     = item.get("furnisher_name", "").strip()
                fname_key = _normalize_collector_name(fname)   # dedup key
                acct      = item.get("account_number", "").strip()

                if fname_key not in collector_accounts:
                    # Store under display name of first occurrence
                    collector_accounts[fname_key] = {"display": fname, "items": []}
                    seen_accts[fname_key] = set()

                if acct not in seen_accts[fname_key]:
                    seen_accts[fname_key].add(acct)
                    collector_accounts[fname_key]["items"].append(item)

    result: dict[str, dict[str, str]] = {}

    for fname_key, data in collector_accounts.items():
        display_name = data["display"]
        items        = data["items"]
        collector_addr = _collector_letter_address(display_name)
        letters: dict[str, str] = {}

        for round_key in ["round_1", "round_2"]:
            is_r2 = round_key == "round_2"

            # Header
            header = (
                f"{consumer_name}\n"
                f"[Address]\n"
                f"[City, State ZIP]\n"
                f"\n"
                f"{display_name}\n"
                f"{collector_addr}\n"
                f"\n"
                f"{formatted_date}\n"
                f"\n"
                f"RE: Debt Validation Request and Formal Dispute — "
                f"15 U.S.C. \u00a71692g / 15 U.S.C. \u00a71681s-2"
            )

            # Opening
            if not is_r2:
                opening = (
                    f"To Whom It May Concern,\n\n"
                    f"I am writing to formally dispute the account(s) listed below "
                    f"and to request validation of the alleged debt(s) pursuant to "
                    f"the Fair Debt Collection Practices Act, 15 U.S.C. \u00a71692g(b). "
                    f"I also dispute the accuracy and completeness of the information "
                    f"you are reporting to the credit bureaus under the Fair Credit "
                    f"Reporting Act, 15 U.S.C. \u00a71681s-2.\n\n"
                    f"Until you provide complete validation of each account listed "
                    f"below, you are required under 15 U.S.C. \u00a71692g(b) to cease "
                    f"all collection activity, including reporting or updating this "
                    f"account at any credit reporting agency. Continued reporting "
                    f"without validating constitutes a violation of both the FDCPA "
                    f"and the FCRA, and may result in statutory damages, actual "
                    f"damages, and attorney fees."
                )
            else:
                opening = (
                    f"To Whom It May Concern,\n\n"
                    f"This is a follow-up to my previous dispute and validation "
                    f"request regarding the account(s) below. I have not received "
                    f"a complete and adequate response, and these accounts remain "
                    f"on my credit report without proper validation.\n\n"
                    f"Under 15 U.S.C. \u00a71692g(b), you were required to cease "
                    f"all collection activity — including credit bureau reporting — "
                    f"until you provided full validation. If you have continued "
                    f"reporting or have reported updates without completing this "
                    f"validation, you are in violation of the FDCPA. You are also "
                    f"in violation of 15 U.S.C. \u00a71681s-2(b), which requires "
                    f"you to investigate a consumer dispute thoroughly and correct "
                    f"or delete information you cannot verify. Continued willful "
                    f"noncompliance may expose your company to liability under "
                    f"15 U.S.C. \u00a71681n, including statutory damages of $100 "
                    f"to $1,000 per violation plus punitive damages and attorney fees."
                )

            # Documentation demand (same for both rounds, more specific in R2)
            if not is_r2:
                demand_intro = (
                    f"To validate each account, you must provide ALL of the following:"
                )
                demand_list = (
                    f"1. The original signed agreement or contract between me and the "
                    f"original creditor establishing the obligation.\n\n"
                    f"2. The complete chain of assignment — every sale, transfer, or "
                    f"assignment document from the original creditor to your company, "
                    f"dated and signed.\n\n"
                    f"3. Proof that your company is the current legal holder of this "
                    f"debt and has the right to collect and report it (UCC Articles "
                    f"3 and 9 apply where applicable).\n\n"
                    f"4. If this debt has been securitized, the Pooling and Servicing "
                    f"Agreement and the schedule of assets identifying this specific "
                    f"account.\n\n"
                    f"5. A complete itemization of the debt: original principal, all "
                    f"interest, all fees and charges, and all payments made, per "
                    f"12 CFR \u00a71006.34(c)(2).\n\n"
                    f"6. The exact Date of First Delinquency (the date I first missed "
                    f"a payment to the original creditor), which controls the FCRA "
                    f"7-year reporting period under 15 U.S.C. \u00a71681c(c).\n\n"
                    f"7. All prior communications sent to me regarding this account."
                )
            else:
                demand_intro = (
                    f"I am repeating my demand for full validation. As a reminder, "
                    f"the documentation required includes:"
                )
                demand_list = (
                    f"1. Original signed agreement with the original creditor.\n\n"
                    f"2. Complete chain of assignment to your company.\n\n"
                    f"3. Proof of legal right to collect and report (UCC Art. 3 & 9).\n\n"
                    f"4. PSA and asset schedule if securitized.\n\n"
                    f"5. Full debt itemization per 12 CFR \u00a71006.34(c)(2).\n\n"
                    f"6. Original Date of First Delinquency with documentation.\n\n"
                    f"7. All prior communications regarding this account."
                )

            # Account-specific sections
            account_sections = []
            for item in items:
                demand = _furnisher_account_demand(item)
                account_sections.append(demand)

            accounts_block = "\n\n".join(account_sections)

            # Legal notice
            legal_notice = (
                f"LEGAL NOTICE\n\n"
                f"Any attempt to continue collection activity, report, update, or "
                f"\"verify\" this account to any credit bureau without first "
                f"providing complete validation constitutes a violation of:\n\n"
                f"\u2022 15 U.S.C. \u00a71692g(b) — Continuing collection without validating.\n"
                f"\u2022 15 U.S.C. \u00a71681s-2(b) — Reporting without proper investigation.\n"
                f"\u2022 15 U.S.C. \u00a71692e — False representation of authority or ownership.\n"
                f"\u2022 UCC Articles 3 and 9 — Collection without legal right.\n\n"
                f"I am retaining copies of all correspondence. You have 30 days "
                f"from receipt of this letter to provide complete validation. "
                f"Failure to do so will be treated as an inability to validate "
                f"and an abandonment of any right to collect or report this debt."
            )

            # Signature
            closing = (
                f"Sincerely,\n\n"
                f"{consumer_name}"
            )

            full = (
                header + "\n\n"
                + opening + "\n\n"
                + "DISPUTED ACCOUNT(S):\n\n"
                + accounts_block + "\n\n"
                + demand_intro + "\n\n"
                + demand_list + "\n\n"
                + legal_notice + "\n\n"
                + closing
            )
            letters[round_key] = full

        result[display_name] = letters

    return result


# =========================
# PERSONAL INFORMATION ENGINE
# =========================

def _split_three_bureau_names(words: list[str]) -> list[str]:
    """
    Split a flat word list into exactly 3 bureau names.
    Input:  ['JEREMY', 'A', 'STEIN', 'JEREMY', 'STEIN', 'JEREMY', 'A', 'STEIN']
    Output: ['JEREMY A STEIN', 'JEREMY STEIN', 'JEREMY A STEIN']

    Strategy: detect where a name repetition starts by finding the first word
    that also appears earlier in the sequence (likely a first name repeat).
    Falls back to equal splits if the heuristic fails.
    """
    n = len(words)
    if n == 0:
        return ["", "", ""]

    # Find split points: look for position where words[i] == words[0]
    # (first name repeating = start of next bureau's name)
    splits = [0]
    for i in range(1, n):
        if words[i] == words[0] and i > 1:
            splits.append(i)
        if len(splits) == 3:
            break

    # If we found exactly 2 split points (start of bureau 2 and 3)
    if len(splits) == 3:
        names = [
            " ".join(words[splits[0]:splits[1]]),
            " ".join(words[splits[1]:splits[2]]),
            " ".join(words[splits[2]:]),
        ]
        return names

    # Fallback: try to detect by checking if the last word of a group
    # matches a known surname pattern (same word appearing multiple times)
    # Split into 3 as evenly as possible
    chunk = max(1, n // 3)
    names = [
        " ".join(words[:chunk]),
        " ".join(words[chunk:chunk*2]),
        " ".join(words[chunk*2:]),
    ]
    return names


def parse_personal_information(lines: list[str]) -> dict[str, Any]:
    """Extract personal information block from PDF lines per bureau."""
    import re as _re

    result: dict[str, Any] = {
        "name_by_bureau": {},
        "dob_by_bureau": {},
        "aka_by_bureau": {},
        "former_name_by_bureau": {},
        "current_addresses": [],
        "previous_addresses": [],
        "raw_block": [],
    }

    pi_start = None
    for i, line in enumerate(lines):
        if "personal information" in line.lower() and i < 150:
            pi_start = i
            break

    if pi_start is None:
        return result

    pi_lines = []
    for i in range(pi_start, min(pi_start + 70, len(lines))):
        line = lines[i].strip()
        if i > pi_start + 5 and any(
            m in line.lower()
            for m in ["credit score", "credit summary", "account #:", "account name"]
        ):
            break
        pi_lines.append(line)
        result["raw_block"].append(line)

    # Name: TU EXP EQ — line contains 3 names concatenated: "JEREMY A STEIN JEREMY STEIN JEREMY A STEIN"
    for line in pi_lines:
        if line.lower().startswith("name:"):
            names_raw = line.split(":", 1)[1].strip()
            words     = names_raw.split()
            # Split word list into exactly 3 names.
            # Strategy: find the first word of bureau 2 by looking for repetition of
            # a known first-name-like word after a complete name sequence.
            # Fallback: split as evenly as possible into 3 chunks.
            names3 = _split_three_bureau_names(words)
            for idx, bureau in enumerate(BUREAUS):
                result["name_by_bureau"][bureau] = names3[idx] if idx < len(names3) else ""

    # Date of Birth: "9/14/1992 1992 9/14/1992" — one token per bureau
    for line in pi_lines:
        if line.lower().startswith("date of birth:"):
            dob_raw = line.split(":", 1)[1].strip()
            tokens  = dob_raw.split()
            for idx, bureau in enumerate(BUREAUS):
                result["dob_by_bureau"][bureau] = tokens[idx] if idx < len(tokens) else ""

    # Also Known As: "- JEREMY AARON -" — dash=no AKA, multi-word name=AKA
    for line in pi_lines:
        if line.lower().startswith("also known as:"):
            raw    = line.split(":", 1)[1].strip()
            tokens = raw.split()
            bureau_idx = 0
            i          = 0
            per_bureau: dict[str, str] = {}
            while i < len(tokens) and bureau_idx < 3:
                bureau = BUREAUS[bureau_idx]
                if tokens[i] == "-":
                    per_bureau[bureau] = ""
                    bureau_idx += 1
                    i += 1
                else:
                    aka_parts = []
                    while i < len(tokens) and tokens[i] != "-":
                        aka_parts.append(tokens[i])
                        i += 1
                    per_bureau[bureau] = " ".join(aka_parts)
                    bureau_idx += 1
            # Fill any missing bureaus
            for bureau in BUREAUS:
                result["aka_by_bureau"][bureau] = per_bureau.get(bureau, "")

    # Former name
    for line in pi_lines:
        if line.lower().startswith("former:"):
            raw    = line.split(":", 1)[1].strip()
            tokens = [t for t in raw.split() if t not in ("-", "")]
            if tokens:
                full = " ".join(tokens)
                for bureau in BUREAUS:
                    result["former_name_by_bureau"][bureau] = full

    # Current / Previous addresses (raw collection)
    state = None
    buf: list[str] = []
    for line in pi_lines:
        lo = line.lower()
        if lo.startswith("current address"):
            state = "current"
            rest = line.split(":", 1)[1].strip() if ":" in line else ""
            if rest:
                buf.append(rest)
        elif lo.startswith("previous address"):
            if buf:
                result["current_addresses"].append(" ".join(buf))
            buf   = []
            state = "previous"
        elif lo.startswith("employer"):
            if buf and state == "previous":
                result["previous_addresses"].append(" ".join(buf))
            buf   = []
            state = None
        elif state and "http" not in line and len(line) > 3 and not line[:3].isdigit():
            buf.append(line)
    if buf:
        if state == "previous":
            result["previous_addresses"].append(" ".join(buf))
        elif state == "current":
            result["current_addresses"].append(" ".join(buf))

    return result


def detect_personal_info_issues(personal_info: dict[str, Any]) -> list[dict[str, Any]]:
    """Detect discrepancies and return list of issue dicts."""
    issues: list[dict[str, Any]] = []

    # DOB inconsistency
    dob = personal_info.get("dob_by_bureau", {})
    dob_vals = [v for v in dob.values() if v]
    if len(set(dob_vals)) > 1:
        issues.append({
            "type": "dob_inconsistency",
            "severity": "high",
            "bureaus": dob,
            "description": (
                "Date of birth is reported differently across bureaus: "
                + ", ".join(f"{b}={v}" for b, v in dob.items() if v)
                + ". Under 15 U.S.C. \u00a71681e(b), each bureau must maintain "
                "maximum possible accuracy of all identifying information."
            ),
        })

    # Former / unrecognized name
    former = personal_info.get("former_name_by_bureau", {})
    former_vals = list({v for v in former.values() if v and v not in ("-", "")})
    if former_vals:
        issues.append({
            "type": "unknown_former_name",
            "severity": "high",
            "value": former_vals[0],
            "description": (
                f"A former name of '{former_vals[0]}' appears on this credit file. "
                "If this name does not belong to the consumer, it may indicate a mixed "
                "file — another person's information has been merged into this file. "
                "This affects the accuracy of every account under this file."
            ),
        })

    # AKA names
    aka = personal_info.get("aka_by_bureau", {})
    aka_vals = list({v for v in aka.values() if v and v not in ("-", "")})
    if aka_vals:
        issues.append({
            "type": "unrecognized_aka",
            "severity": "medium",
            "value": ", ".join(aka_vals),
            "description": (
                f"The file shows 'Also Known As' name(s): {', '.join(aka_vals)}. "
                "If the consumer does not recognize these names, they should not be "
                "associated with this file and must be removed under \u00a71681e(b)."
            ),
        })

    # Name inconsistency
    names    = personal_info.get("name_by_bureau", {})
    name_set = {v for v in names.values() if v}
    if len(name_set) > 1:
        issues.append({
            "type": "name_inconsistency",
            "severity": "medium",
            "bureaus": names,
            "description": (
                "The consumer's name appears differently across bureaus: "
                + ", ".join(f"{b}='{v}'" for b, v in names.items() if v)
                + "."
            ),
        })

    return issues


def build_personal_info_section(
    personal_info: dict[str, Any],
    issues: list[dict[str, Any]],
    bureau: str,
) -> str:
    """
    Build personal information dispute paragraph for ONE bureau.
    Returns empty string if no issues apply to this bureau.
    Goes at the TOP of the dispute letter, before the account list.
    """
    if not issues:
        return ""

    bureau_display = BUREAU_ADDRESSES.get(bureau, {}).get("name", bureau.title())
    relevant: list[dict[str, Any]] = []

    for issue in issues:
        itype = issue["type"]
        if itype == "dob_inconsistency":
            bureau_dob = issue.get("bureaus", {}).get(bureau, "")
            all_dobs   = list(issue.get("bureaus", {}).values())
            if bureau_dob and len(set(all_dobs)) > 1:
                relevant.append(issue)
        elif itype in {"unknown_former_name", "unrecognized_aka"}:
            relevant.append(issue)
        elif itype == "name_inconsistency":
            all_name_vals = list(issue.get("bureaus", {}).values())
            if len(set(all_name_vals)) > 1:
                relevant.append(issue)

    if not relevant:
        return ""

    parts = [
        f"Before addressing the specific accounts below, I also want to dispute "
        f"inaccurate personal information in my file at {bureau_display}. "
        f"Under 15 U.S.C. \u00a71681e(b), you are required to maintain maximum "
        f"possible accuracy of all information — including name, date of birth, "
        f"and address. Inaccurate identifying information in a file undermines "
        f"the reliability of every account reported under that file."
    ]

    for issue in relevant:
        itype = issue["type"]

        if itype == "dob_inconsistency":
            bureau_dob = issue.get("bureaus", {}).get(bureau, "")
            all_dobs   = issue.get("bureaus", {})
            # Find the most complete DOB (contains /) from other bureaus
            other_complete = next(
                (v for b, v in all_dobs.items() if b != bureau and "/" in str(v)),
                None,
            )
            if bureau_dob and "/" not in bureau_dob and other_complete:
                # This bureau has the incomplete/wrong DOB
                parts.append(
                    f"My date of birth appears as '{bureau_dob}' in your file — "
                    f"just the year, without the full date. Other bureaus correctly "
                    f"show my complete date of birth. I am requesting that you "
                    f"update my date of birth to the accurate full date."
                )
            elif bureau_dob and "/" in bureau_dob and other_complete:
                # This bureau has it right, skip DOB for this bureau
                pass

        elif itype == "unknown_former_name":
            name_val = issue.get("value", "")
            parts.append(
                f"My file includes a former name of '{name_val}'. "
                f"I do not recognize this name. If it belongs to another person, "
                f"its presence suggests information from another consumer's file "
                f"has been mixed into mine — which would call into question the "
                f"accuracy of every account listed here. I am requesting that this "
                f"name be verified and, if it does not belong to me, removed immediately."
            )

        elif itype == "unrecognized_aka":
            name_val = issue.get("value", "")
            parts.append(
                f"My file shows an 'Also Known As' of '{name_val}'. "
                f"I do not use this name. I am requesting it be removed."
            )

        elif itype == "name_inconsistency":
            bureau_name = issue.get("bureaus", {}).get(bureau, "")
            if bureau_name:
                parts.append(
                    f"My name appears as '{bureau_name}' at {bureau_display}. "
                    f"I am requesting that my correct legal name be reflected accurately."
                )

    parts.append(
        f"I am requesting that {bureau_display} correct all identifying "
        f"information discrepancies above and provide me with confirmation "
        f"that my file reflects accurate personal information, per "
        f"15 U.S.C. \u00a71681g(a) and \u00a71681i(a)."
    )

    return "\n\n".join(parts)


def parse_and_detect_personal_info(lines: list[str]) -> tuple:
    """Convenience wrapper — parse then detect."""
    personal_info = parse_personal_information(lines)
    issues        = detect_personal_info_issues(personal_info)
    return personal_info, issues





# =========================
# VERIFIED RESPONSE ENGINE
# =========================
#
# When a bureau responds "verified", the consumer has the right under
# §1681i(a)(6)(B)(iii) to demand:
#   - The exact PROCEDURE used to reinvestigate
#   - The NAME, ADDRESS, and PHONE of every furnisher contacted
#   - The DOCUMENTATION reviewed
#
# An ACDV-only ping (automated inquiry, no document review) does NOT
# meet the "reasonable reinvestigation" standard. This letter forces the
# bureau to either prove they did a real investigation or delete the item.
#
# This engine is called ON DEMAND — not during the initial pipeline.
# API: POST /verified-response  {bureau, page_id, response_date, accounts[]}

_VERIFIED_RESPONSE_OPENINGS = [
    (
        "Hi,\n\n"
        "I am writing in response to your reinvestigation results dated "
        "{response_date}, in which you indicated that the account(s) listed "
        "below were verified. I am not satisfied with this result because I "
        "do not believe a genuine reinvestigation took place. I am now "
        "exercising my right under 15 U.S.C. \u00a71681i(a)(6)(B)(iii) to "
        "request a full description of the procedure you used to verify each "
        "item, including who you contacted and what documentation you reviewed."
    ),
    (
        "Hi,\n\n"
        "I received your response stating that the accounts below were verified "
        "following my dispute. I am writing back because a response that simply "
        "states an account was verified does not satisfy the law. Under "
        "15 U.S.C. \u00a71681i(a)(6)(B)(iii), I have the right to know exactly "
        "how you conducted this investigation \u2014 the specific procedure used, "
        "every company you contacted, and the documentation you relied on to "
        "conclude the information is accurate. I am requesting all of that now."
    ),
    (
        "Hi,\n\n"
        "Thank you for your reinvestigation response. However, I am disputing "
        "your conclusion that the accounts below were verified. A verification "
        "under the Fair Credit Reporting Act requires more than confirming with "
        "the reporting company that their own data is correct \u2014 it requires "
        "a reasonable reinvestigation with actual documentation. Under "
        "15 U.S.C. \u00a71681i(a)(6)(B)(iii), I am requesting a written "
        "description of your reinvestigation procedure for each account listed."
    ),
]

_VERIFIED_BUREAU_INDEX = {
    "transunion": 0,
    "experian":   1,
    "equifax":    2,
}


def _what_needed_to_verify(attack_type: str, furnisher: str) -> str:
    """Return plain-language description of what docs were needed for this attack."""
    if attack_type in {
        "late_payment_history_dispute",
        "cross_bureau_payment_history_date_conflict",
    }:
        return 72
    if attack_type in {
        "collector_original_creditor_self_declared",
        "collector_original_creditor_pattern",
        "same_account_number_same_balance",
        "duplicate_account_number",
        "multi_furnisher_same_balance",
    }:
        return (
            f"the original signed agreement, the complete chain of assignment "
            f"from the original creditor to {furnisher}, and proof of their "
            f"legal authority to report this specific account"
        )
    elif attack_type in {"potential_re_aging", "dofd_unknown_verification_required"}:
        return (
            "the original Date of First Delinquency with documentation from "
            "the original creditor, confirming the correct start of the "
            "7-year reporting period under 15 U.S.C. \u00a71681c(c)"
        )
    elif attack_type in {
        "cross_bureau_balance_conflict",
        "cross_bureau_payment_status_conflict",
        "cross_bureau_account_status_conflict",
    }:
        return (
            "the accurate balance, payment status, and account status with "
            "primary documentation \u2014 not simply a confirmation from the "
            "reporting company that their own records are correct"
        )
    elif attack_type == "obsolete_account_7yr_limit":
        return (
            "documentation of the original Date of First Delinquency to confirm "
            "this account is within the FCRA 7-year reporting window"
        )
    else:
        return (
            "the complete accuracy of every field reported \u2014 balance, payment "
            "history, account status, and date of first delinquency \u2014 with "
            "primary source documentation from the original creditor"
        )


def build_verified_response_letter(
    bureau: str,
    accounts: list[dict[str, Any]],
    consumer_name: str,
    response_date: str = "",
    report_date: str = "",
) -> str:
    """
    Generate a post-verified-response letter demanding ACDV procedure
    description and documentation under §1681i(a)(6)(B)(iii).

    Parameters:
        bureau:        "transunion" | "experian" | "equifax"
        accounts:      items from letter_input_engine that were marked verified
        consumer_name: real client name
        response_date: date on bureau's verification letter (MM/DD/YYYY or string)
        report_date:   original report date for letter header
    """
    bureau_info    = BUREAU_ADDRESSES.get(bureau, {})
    bureau_name    = bureau_info.get("name", bureau.title())
    bureau_address = bureau_info.get("address", "")
    letter_date    = _format_date_long(report_date or response_date)
    resp_label     = response_date if response_date else "your recent reinvestigation response"

    # Opening template — one per bureau, guaranteed unique
    tpl_idx = _VERIFIED_BUREAU_INDEX.get(bureau, 0) % len(_VERIFIED_RESPONSE_OPENINGS)
    opening = _VERIFIED_RESPONSE_OPENINGS[tpl_idx].format(response_date=resp_label)

    header = (
        f"{consumer_name}\n"
        f"[Address]\n"
        f"[City, State ZIP]\n"
        f"\n"
        f"{bureau_name}\n"
        f"{bureau_address}\n"
        f"\n"
        f"{letter_date}"
    )

    # Per-account demand paragraphs
    account_sections: list[str] = []
    for i, acc in enumerate(accounts, 1):
        furnisher   = acc.get("furnisher_name", "")
        acct        = acc.get("account_number", "")
        attack_type = acc.get("attack_type", "")
        needed_docs = _what_needed_to_verify(attack_type, furnisher)

        section = (
            f"{i}. {furnisher} \u2014 Account #: {acct}\n"
            f"You indicated this account was verified. Under "
            f"15 U.S.C. \u00a71681i(a)(6)(B)(iii), I am requesting:\n\n"
            f"(a) A complete description of the reinvestigation procedure you "
            f"used for this account, including whether it was conducted via an "
            f"automated system or through direct contact and document review.\n\n"
            f"(b) The name, address, and telephone number of every person or "
            f"company you contacted in connection with this reinvestigation.\n\n"
            f"(c) A description of the documentation you received and reviewed. "
            f"A genuine verification of this account would have required "
            f"{needed_docs}.\n\n"
            f"If your reinvestigation consisted only of sending an automated "
            f"inquiry to {furnisher} and accepting their response without "
            f"independently reviewing documentation, that does not constitute "
            f"a reasonable reinvestigation under 15 U.S.C. \u00a71681i(a) and "
            f"this account remains unverifiable and must be deleted."
        )
        account_sections.append(section)

    accounts_block = "\n\n".join(account_sections)

    legal_warning = (
        "I want to be direct: if the verification of any of these accounts was "
        "based solely on an automated response from the reporting company \u2014 "
        "without independent review of primary documentation \u2014 that does not "
        "meet the reasonable reinvestigation standard. Courts have consistently "
        "held that simply accepting a furnisher's own confirmation is insufficient.\n\n"
        "Continuing to report information that has not been genuinely verified "
        "after a properly submitted dispute may constitute willful noncompliance "
        "under 15 U.S.C. \u00a71681n, which provides for statutory damages of "
        "$100 to $1,000 per violation, punitive damages, and attorney fees. "
        "I am retaining copies of all correspondence.\n\n"
        "Please provide the procedure description within 15 days. If any account "
        "cannot be verified with actual documentation, I am requesting its deletion "
        "and written confirmation per 15 U.S.C. \u00a71681i(a)(5) and \u00a71681i(a)(6)."
    )

    closing = f"Thank you,\n\n{consumer_name}"

    return (
        header + "\n\n"
        + opening + "\n\n\n"
        + "Accounts requiring procedure description:\n\n"
        + accounts_block + "\n\n\n"
        + legal_warning + "\n\n"
        + closing
    )


def build_verified_response_letters(
    bureau: str,
    verified_accounts: list[dict[str, Any]],
    consumer_name: str,
    response_date: str = "",
    report_date: str = "",
) -> dict[str, Any]:
    """
    Convenience wrapper for the API endpoint /verified-response.

    verified_accounts: items from letter_input_engine that the bureau
                       marked as verified (selected by the user in the UI).
    """
    letter = build_verified_response_letter(
        bureau=bureau,
        accounts=verified_accounts,
        consumer_name=consumer_name,
        response_date=response_date,
        report_date=report_date,
    )
    return {
        "letter": letter,
        "bureau": bureau,
        "account_count": len(verified_accounts),
        "response_date": response_date,
    }



# =========================
# BUREAU RESPONSE ENGINE
# =========================
#
# Complete catalog of bureau responses and their attacks:
#
#   VERIFIED / MEETS REQUIREMENTS  — §1681i(a)(6)(B)(iii) procedure demand (already built)
#   UPDATED                        — What changed? Why not deleted? Is it enough?
#   DELETED                        — Victory, but monitor for reinsertion
#   FRIVOLOUS / IRRELEVANT         — §1681i(a)(3): must give reason + info needed
#   UNABLE TO PROCESS              — Resubmit with better identification
#   NO RESPONSE IN 30 DAYS         — §1681i(a)(1): mandatory deletion demand
#   REINSERTION                    — §1681i(a)(5)(B): willful violation if no notice
#
# Each response type generates a specific letter that matches exactly
# what the law requires as the next step.
#
# API: POST /bureau-response
#      {page_id, bureau, response_type, response_date, report_date, accounts[]}

# ── Response type constants ───────────────────────────────────────────────────

BUREAU_RESPONSE_VERIFIED       = "verified"
BUREAU_RESPONSE_UPDATED        = "updated"
BUREAU_RESPONSE_DELETED        = "deleted"
BUREAU_RESPONSE_FRIVOLOUS      = "frivolous"
BUREAU_RESPONSE_UNABLE         = "unable_to_process"
BUREAU_RESPONSE_NO_RESPONSE    = "no_response_30_days"
BUREAU_RESPONSE_REINSERTION    = "reinsertion"

# ── Shared helpers ────────────────────────────────────────────────────────────

def _bureau_header(
    consumer_name: str,
    bureau: str,
    letter_date: str,
) -> str:
    info    = BUREAU_ADDRESSES.get(bureau, {})
    name    = info.get("name", bureau.title())
    address = info.get("address", "")
    return (
        f"{consumer_name}\n[Address]\n[City, State ZIP]\n\n"
        f"{name}\n{address}\n\n"
        f"{letter_date}"
    )


# ── UPDATED response ──────────────────────────────────────────────────────────

def build_updated_response_letter(
    bureau: str,
    accounts: list[dict],
    consumer_name: str,
    response_date: str = "",
    report_date: str = "",
) -> str:
    """
    When the bureau says an item was 'updated' but not deleted.
    Attack: demand what specifically changed, confirm the original dispute
    is still not resolved, and request deletion if still unverifiable.
    """
    info        = BUREAU_ADDRESSES.get(bureau, {})
    bureau_name = info.get("name", bureau.title())
    date_str    = _format_date_long(report_date or response_date)
    resp_label  = response_date or "your recent reinvestigation response"

    header  = _bureau_header(consumer_name, bureau, date_str)
    opening = (
        f"Hi,\n\n"
        f"I received your reinvestigation results dated {resp_label}, which "
        f"indicate that the account(s) below were 'updated.' I am writing "
        f"because an update is not the same as a resolution. Simply changing "
        f"a number or a date does not necessarily correct the underlying "
        f"inaccuracy I disputed, and it does not satisfy the bureau's "
        f"obligation under 15 U.S.C. \u00a71681i(a)(5) to delete or "
        f"correct information that cannot be verified."
    )

    sections = []
    for i, acc in enumerate(accounts, 1):
        furnisher   = acc.get("furnisher_name", "")
        acct        = acc.get("account_number", "")
        attack_type = acc.get("attack_type", "")

        section = (
            f"{i}. {furnisher} \u2014 Account #: {acct}\n"
            f"You have indicated this account was updated. I have the following "
            f"questions and demands regarding this update:\n\n"
            f"(a) What specific information was changed? Please provide a "
            f"description of every field that was modified and what the "
            f"corrected value is.\n\n"
            f"(b) Was the underlying issue I disputed actually resolved? "
            f"My original dispute raised concerns about {_short_attack_description(attack_type, furnisher)}. "
            f"An update to a peripheral field does not resolve this.\n\n"
            f"(c) Was this account fully verified with primary documentation, "
            f"or was the update based solely on information provided by "
            f"{furnisher} without independent review?\n\n"
            f"If the update did not fully resolve my original dispute — or if "
            f"the account still cannot be verified with primary documentation — "
            f"it must be deleted under 15 U.S.C. \u00a71681i(a)(5)."
        )
        sections.append(section)

    closing = (
        f"I am requesting written confirmation of exactly what was changed "
        f"and confirmation that my original dispute has been fully resolved. "
        f"If any aspect of my dispute remains unresolved, I am requesting "
        f"deletion of the item(s) as required by 15 U.S.C. \u00a71681i(a)(5).\n\n"
        f"Thank you,\n\n{consumer_name}"
    )

    return (
        header + "\n\n"
        + opening + "\n\n\n"
        + "Accounts marked as 'updated':\n\n"
        + "\n\n".join(sections) + "\n\n\n"
        + closing
    )


# ── DELETED — reinsertion monitoring letter ───────────────────────────────────

def build_deletion_confirmed_letter(
    bureau: str,
    accounts: list[dict],
    consumer_name: str,
    response_date: str = "",
    report_date: str = "",
) -> str:
    """
    Sent AFTER a deletion — not immediately, but when the consumer uploads
    a new report and the deleted item has REAPPEARED. Attacks §1681i(a)(5)(B).
    If no reinsertion: this function returns a confirmation/monitoring notice.
    """
    info        = BUREAU_ADDRESSES.get(bureau, {})
    bureau_name = info.get("name", bureau.title())
    date_str    = _format_date_long(report_date or response_date)
    resp_label  = response_date or "your recent reinvestigation response"

    header  = _bureau_header(consumer_name, bureau, date_str)

    # Check if any accounts are flagged as reinserted
    reinserted = [a for a in accounts if a.get("reinserted", False)]

    if not reinserted:
        # Confirmation letter — no reinsertion yet
        opening = (
            f"Hi,\n\n"
            f"Thank you for your reinvestigation response dated {resp_label}, "
            f"which indicates that the account(s) below have been deleted from "
            f"my credit file. I am acknowledging this deletion and requesting "
            f"written confirmation that these items have been permanently removed.\n\n"
            f"I am also putting you on notice that if any of these deleted items "
            f"reappear on my credit report in the future without proper procedure, "
            f"that will constitute a violation of 15 U.S.C. \u00a71681i(a)(5)(B), "
            f"which requires written notice to the consumer within five business "
            f"days of any reinsertion and requires the furnisher to certify "
            f"the accuracy of the reinserted information."
        )
        sections = []
        for i, acc in enumerate(accounts, 1):
            sections.append(
                f"{i}. {acc.get('furnisher_name', '')} \u2014 "
                f"Account #: {acc.get('account_number', '')}\n"
                f"Deletion confirmed. I am requesting written confirmation "
                f"of this deletion and will monitor my credit file to ensure "
                f"this item does not reappear."
            )
        closing = (
            f"Please send written confirmation of these deletions. "
            f"I am retaining this response as part of my records.\n\n"
            f"Thank you,\n\n{consumer_name}"
        )
    else:
        # Reinsertion violation letter
        opening = (
            f"Hi,\n\n"
            f"I am writing regarding a serious violation of the Fair Credit "
            f"Reporting Act. The account(s) listed below were previously deleted "
            f"from my credit file following my dispute. They have now reappeared "
            f"on my credit report without the proper procedure required by law.\n\n"
            f"Under 15 U.S.C. \u00a71681i(a)(5)(B), if deleted information is "
            f"reinserted, the bureau must: (1) ensure the furnisher certifies "
            f"in writing that the information is complete and accurate, and "
            f"(2) notify the consumer in writing within five business days "
            f"of the reinsertion. I did not receive such notice. "
            f"This is a willful violation of the FCRA."
        )
        sections = []
        for i, acc in enumerate(reinserted, 1):
            sections.append(
                f"{i}. {acc.get('furnisher_name', '')} \u2014 "
                f"Account #: {acc.get('account_number', '')}\n"
                f"This account was previously deleted and has reappeared. "
                f"I did not receive the required written notice of reinsertion "
                f"under 15 U.S.C. \u00a71681i(a)(5)(B)(ii). I am demanding "
                f"immediate re-deletion of this item and a written explanation "
                f"of: (a) who authorized the reinsertion, (b) what certification "
                f"the furnisher provided, and (c) why I was not notified."
            )
        closing = (
            f"I am demanding immediate deletion of all reinserted items listed "
            f"above. Be advised that reinserting previously deleted information "
            f"without proper procedure and without notifying the consumer is a "
            f"willful violation of 15 U.S.C. \u00a71681n, exposing your "
            f"organization to statutory damages of $100 to $1,000 per violation, "
            f"punitive damages, and attorney fees. I am retaining all records.\n\n"
            f"Thank you,\n\n{consumer_name}"
        )

    return (
        header + "\n\n"
        + opening + "\n\n\n"
        + ("Deleted accounts:\n\n" if not reinserted else "Reinserted accounts \u2014 immediate deletion required:\n\n")
        + "\n\n".join(sections) + "\n\n\n"
        + closing
    )


# ── FRIVOLOUS response ────────────────────────────────────────────────────────

def build_frivolous_response_letter(
    bureau: str,
    accounts: list[dict],
    consumer_name: str,
    response_date: str = "",
    report_date: str = "",
    frivolous_reason: str = "",
) -> str:
    """
    When the bureau labels the dispute frivolous or irrelevant.
    Attack: challenge the frivolous designation, provide additional specificity,
    demand the exact information they claim is missing, and warn that blanket
    frivolous labels are themselves FCRA violations.
    """
    info        = BUREAU_ADDRESSES.get(bureau, {})
    bureau_name = info.get("name", bureau.title())
    date_str    = _format_date_long(report_date or response_date)
    resp_label  = response_date or "your recent response"

    header  = _bureau_header(consumer_name, bureau, date_str)
    opening = (
        f"Hi,\n\n"
        f"I received your response dated {resp_label} indicating that my "
        f"dispute was deemed frivolous or irrelevant. I am writing to "
        f"challenge this determination and to resubmit my dispute with "
        f"additional specificity.\n\n"
        f"Under 15 U.S.C. \u00a71681i(a)(3), a bureau may only decline to "
        f"investigate a dispute if it reasonably determines the dispute is "
        f"frivolous or irrelevant. When it does so, it must notify the "
        f"consumer within five business days with the specific reasons and "
        f"the information needed to investigate. A dispute is not frivolous "
        f"simply because it is inconvenient to investigate, because it was "
        f"submitted multiple times, or because the bureau believes the "
        f"information is accurate. The FCRA does not permit a bureau to "
        f"label a dispute frivolous based on suspicion that it originated "
        f"from a credit repair organization."
    )

    # Add reason-specific challenge if we have it
    if frivolous_reason:
        reason_para = (
            f"\n\nYou stated the reason for this determination was: "
            f"'{frivolous_reason}'. I am disputing this characterization. "
            f"My dispute identifies specific accounts, specific inaccuracies, "
            f"and specific legal grounds. This is not a frivolous dispute."
        )
        opening += reason_para

    sections = []
    for i, acc in enumerate(accounts, 1):
        furnisher   = acc.get("furnisher_name", "")
        acct        = acc.get("account_number", "")
        attack_type = acc.get("attack_type", "")

        section = (
            f"{i}. {furnisher} \u2014 Account #: {acct}\n"
            f"I am resubmitting this dispute with additional specificity. "
            f"The specific inaccuracy I am disputing is: "
            f"{_short_attack_description(attack_type, furnisher)}. "
            f"This is a concrete, factual dispute about a specific field "
            f"in my credit report. The bureau is required to investigate "
            f"this under 15 U.S.C. \u00a71681i(a). I am also requesting "
            f"that you specify, in writing, exactly what additional "
            f"information you require to conduct this investigation, "
            f"as mandated by \u00a71681i(a)(3)(C)."
        )
        sections.append(section)

    closing = (
        f"If you continue to decline investigation of these items without "
        f"a valid legal basis, I will file a complaint with the Consumer "
        f"Financial Protection Bureau (CFPB) and consider legal action "
        f"under 15 U.S.C. \u00a71681n for willful noncompliance.\n\n"
        f"Thank you,\n\n{consumer_name}"
    )

    return (
        header + "\n\n"
        + opening + "\n\n\n"
        + "Disputes resubmitted with additional specificity:\n\n"
        + "\n\n".join(sections) + "\n\n\n"
        + closing
    )


# ── UNABLE TO PROCESS ─────────────────────────────────────────────────────────

def build_unable_to_process_letter(
    bureau: str,
    accounts: list[dict],
    consumer_name: str,
    response_date: str = "",
    report_date: str = "",
    consumer_ssn_last4: str = "",
    consumer_dob: str = "",
) -> str:
    """
    When the bureau says it could not process the dispute.
    Attack: resubmit with more identifying information and assert the
    bureau's obligation to investigate cannot be waived by a processing failure.
    """
    info        = BUREAU_ADDRESSES.get(bureau, {})
    bureau_name = info.get("name", bureau.title())
    date_str    = _format_date_long(report_date or response_date)
    resp_label  = response_date or "your recent response"

    header  = _bureau_header(consumer_name, bureau, date_str)
    opening = (
        f"Hi,\n\n"
        f"I received your response dated {resp_label} indicating that you "
        f"were unable to process my dispute. I am resubmitting this dispute "
        f"with additional identifying information to assist in locating my file.\n\n"
        f"The bureau's obligation to investigate a consumer dispute under "
        f"15 U.S.C. \u00a71681i(a) cannot be waived by a processing difficulty. "
        f"If additional identification is needed to locate my file, I am "
        f"providing it below. If the issue is something other than identification, "
        f"I am requesting a specific explanation of what prevented processing "
        f"and what is required to resolve it."
    )

    # Consumer ID block
    id_block = (
        f"Consumer identification:\n"
        f"  Full name: {consumer_name}\n"
        f"  Date of birth: {consumer_dob or '[DATE OF BIRTH]'}\n"
        f"  SSN last 4: {consumer_ssn_last4 or '[LAST 4 SSN]'}\n"
        f"  Current address: [ADDRESS]\n"
        f"  Enclosures: Copy of government-issued ID, Proof of address"
    )

    sections = []
    for i, acc in enumerate(accounts, 1):
        furnisher = acc.get("furnisher_name", "")
        acct      = acc.get("account_number", "")
        sections.append(
            f"{i}. {furnisher} \u2014 Account #: {acct}\n"
            f"I am resubmitting my dispute for this account. "
            f"If there is a specific issue that prevented processing "
            f"of this item, please advise in writing."
        )

    closing = (
        f"I am enclosing copies of my identification documents to assist "
        f"in verifying my identity and locating my file. Please process "
        f"this dispute and provide a response within the 30-day period "
        f"required by 15 U.S.C. \u00a71681i(a)(1).\n\n"
        f"Thank you,\n\n{consumer_name}\n\n"
        f"Enclosures: Government-issued ID, Proof of Address"
    )

    return (
        header + "\n\n"
        + opening + "\n\n"
        + id_block + "\n\n\n"
        + "Disputes resubmitted:\n\n"
        + "\n\n".join(sections) + "\n\n\n"
        + closing
    )


# ── NO RESPONSE IN 30 DAYS ────────────────────────────────────────────────────

def build_no_response_letter(
    bureau: str,
    accounts: list[dict],
    consumer_name: str,
    dispute_date: str = "",
    report_date: str = "",
) -> str:
    """
    When the bureau has not responded within 30 days of the dispute.
    Under §1681i(a)(1) the disputed items MUST be deleted.
    This is not discretionary — it is mandatory.
    """
    info        = BUREAU_ADDRESSES.get(bureau, {})
    bureau_name = info.get("name", bureau.title())
    date_str    = _format_date_long(report_date)
    dispute_label = dispute_date or "[original dispute date]"

    header  = _bureau_header(consumer_name, bureau, date_str)
    opening = (
        f"Hi,\n\n"
        f"I submitted a formal credit report dispute on or around {dispute_label}. "
        f"As of today, more than 30 days have passed and I have not received "
        f"a response to my dispute.\n\n"
        f"Under 15 U.S.C. \u00a71681i(a)(1), the bureau must complete its "
        f"reinvestigation within 30 days of receiving the dispute. If the "
        f"bureau fails to complete the reinvestigation within this period, "
        f"the disputed items must be deleted from my credit file. "
        f"This obligation is not discretionary — it is a statutory requirement. "
        f"The 30-day period has now elapsed. I am demanding immediate deletion "
        f"of the disputed items listed below."
    )

    sections = []
    for i, acc in enumerate(accounts, 1):
        furnisher = acc.get("furnisher_name", "")
        acct      = acc.get("account_number", "")
        sections.append(
            f"{i}. {furnisher} \u2014 Account #: {acct}\n"
            f"This item was disputed on {dispute_label}. The 30-day "
            f"reinvestigation period has expired without a response. "
            f"Under 15 U.S.C. \u00a71681i(a)(1), this item must be "
            f"deleted immediately."
        )

    closing = (
        f"I am demanding written confirmation of the deletion of all items "
        f"listed above within five business days. Continued reporting of "
        f"these items after the expiration of the 30-day investigation "
        f"period constitutes a violation of 15 U.S.C. \u00a71681n and "
        f"exposes this bureau to statutory damages of $100 to $1,000 per "
        f"violation, punitive damages, and attorney fees. I am retaining "
        f"certified mail records of my original dispute as proof of the "
        f"submission date.\n\n"
        f"Thank you,\n\n{consumer_name}"
    )

    return (
        header + "\n\n"
        + opening + "\n\n\n"
        + "Items past 30-day deadline \u2014 mandatory deletion required:\n\n"
        + "\n\n".join(sections) + "\n\n\n"
        + closing
    )


# ── Shared helper ─────────────────────────────────────────────────────────────

def _short_attack_description(attack_type: str, furnisher: str) -> str:
    """One-sentence plain-language description of the dispute ground."""
    descs = {
        "collector_original_creditor_self_declared": (
            f"the lack of documented legal authority for {furnisher} to report "
            f"this collection account, including missing chain of assignment"
        ),
        "collector_original_creditor_pattern": (
            f"the absence of proof that {furnisher} holds the legal right "
            f"to report this account, given the collector/original creditor pattern"
        ),
        "same_account_number_same_balance": (
            "the same account number appearing under multiple furnisher names, "
            "which constitutes duplicate reporting of a single obligation"
        ),
        "duplicate_account_number": (
            "the same account number appearing in multiple separate tradelines"
        ),
        "potential_re_aging": (
            f"the incorrect reporting period, where {furnisher} appears to be "
            f"using its own acquisition date rather than the original date of "
            f"first delinquency, artificially extending the 7-year clock"
        ),
        "dofd_unknown_verification_required": (
            "the missing or unverifiable Date of First Delinquency, which makes "
            "it impossible to confirm this account is within its legal reporting window"
        ),
        "cross_bureau_balance_conflict": (
            "the balance being reported differently at different bureaus"
        ),
        "cross_bureau_payment_status_conflict": (
            "the payment status being reported inconsistently across bureaus"
        ),
        "absent_bureau_reporting_inconsistency": (
            "the inconsistent reporting — this account appears negative at some "
            "bureaus but not others"
        ),
        "obsolete_account_7yr_limit": (
            "the account being reported past its FCRA 7-year maximum reporting period"
        ),
    }
    return descs.get(attack_type, "the inaccuracy and unverifiability of the reported information")


# ── Master dispatch function ──────────────────────────────────────────────────

def build_bureau_response_letter(
    response_type: str,
    bureau: str,
    accounts: list[dict],
    consumer_name: str,
    response_date: str = "",
    report_date: str = "",
    dispute_date: str = "",
    frivolous_reason: str = "",
    consumer_ssn_last4: str = "",
    consumer_dob: str = "",
    reinserted_accounts: list[dict] | None = None,
) -> dict:
    """
    Master dispatch for all bureau response types.

    Returns:
        {
            "letter": str,
            "response_type": str,
            "bureau": str,
            "account_count": int,
            "next_steps": str,   # what to do after sending this letter
        }
    """
    if response_type == BUREAU_RESPONSE_VERIFIED:
        letter = build_verified_response_letter(
            bureau=bureau,
            accounts=accounts,
            consumer_name=consumer_name,
            response_date=response_date,
            report_date=report_date,
        )
        next_steps = (
            "Send certified mail. Bureau has 15 days to provide procedure description. "
            "If they cannot — or if the procedure was ACDV-only — file CFPB complaint "
            "and consult FCRA attorney for §1681n claim."
        )

    elif response_type == BUREAU_RESPONSE_UPDATED:
        letter = build_updated_response_letter(
            bureau=bureau,
            accounts=accounts,
            consumer_name=consumer_name,
            response_date=response_date,
            report_date=report_date,
        )
        next_steps = (
            "If the update did not fully resolve the dispute, send this letter and "
            "pull a fresh report to confirm what changed. If still inaccurate, "
            "escalate to CFPB complaint."
        )

    elif response_type == BUREAU_RESPONSE_DELETED:
        accts_with_flag = [
            dict(a, reinserted=a.get("account_number", "") in
                 {r.get("account_number", "") for r in (reinserted_accounts or [])})
            for a in accounts
        ]
        letter = build_deletion_confirmed_letter(
            bureau=bureau,
            accounts=accts_with_flag,
            consumer_name=consumer_name,
            response_date=response_date,
            report_date=report_date,
        )
        next_steps = (
            "Monitor the next credit report. If the item reappears, immediately "
            "send this letter again with reinserted=True flagged on those accounts. "
            "Reinsertion without notice is a §1681i(a)(5)(B) violation."
        )

    elif response_type == BUREAU_RESPONSE_FRIVOLOUS:
        letter = build_frivolous_response_letter(
            bureau=bureau,
            accounts=accounts,
            consumer_name=consumer_name,
            response_date=response_date,
            report_date=report_date,
            frivolous_reason=frivolous_reason,
        )
        next_steps = (
            "File a CFPB complaint simultaneously. If the bureau ignores this "
            "resubmission, consult an FCRA attorney — improper frivolous designations "
            "are themselves §1681n violations."
        )

    elif response_type == BUREAU_RESPONSE_UNABLE:
        letter = build_unable_to_process_letter(
            bureau=bureau,
            accounts=accounts,
            consumer_name=consumer_name,
            response_date=response_date,
            report_date=report_date,
            consumer_ssn_last4=consumer_ssn_last4,
            consumer_dob=consumer_dob,
        )
        next_steps = (
            "Send certified mail with ID documents enclosed. Track the 30-day "
            "clock from the date of this resubmission. If no response in 30 days, "
            "send the no_response_30_days letter."
        )

    elif response_type == BUREAU_RESPONSE_NO_RESPONSE:
        letter = build_no_response_letter(
            bureau=bureau,
            accounts=accounts,
            consumer_name=consumer_name,
            dispute_date=dispute_date,
            report_date=report_date,
        )
        # Append CFPB notice for no-response (strongest escalation trigger)
        cfpb_para = build_cfpb_complaint_language(BUREAU_RESPONSE_NO_RESPONSE)
        letter = letter.replace(consumer_name + "\n", cfpb_para + "\n\n" + consumer_name + "\n", 1)
        next_steps = (
            "Send certified mail. File CFPB complaint simultaneously at "
            "consumerfinance.gov/complaint — this creates regulatory exposure. "
            "If items are not deleted within 5 business days of bureau receiving "
            "this letter, consult an FCRA attorney — §1681i(a)(1) violation "
            "with strong §1681n exposure."
        )

    elif response_type == BUREAU_RESPONSE_REINSERTION:
        accts_marked = [dict(a, reinserted=True) for a in accounts]
        letter = build_deletion_confirmed_letter(
            bureau=bureau,
            accounts=accts_marked,
            consumer_name=consumer_name,
            response_date=response_date,
            report_date=report_date,
        )
        # Append CFPB notice for reinsertion (willful violation — mandatory escalation)
        cfpb_para = build_cfpb_complaint_language(BUREAU_RESPONSE_REINSERTION)
        letter = letter.replace(consumer_name + "\n", cfpb_para + "\n\n" + consumer_name + "\n", 1)
        next_steps = (
            "Send certified mail. File CFPB complaint immediately at "
            "consumerfinance.gov/complaint. "
            "Reinsertion without notice is one of the strongest §1681n claims — "
            "consult an FCRA attorney."
        )

    else:
        return {
            "error": f"Unknown response_type: {response_type}",
            "valid_types": [
                BUREAU_RESPONSE_VERIFIED, BUREAU_RESPONSE_UPDATED,
                BUREAU_RESPONSE_DELETED, BUREAU_RESPONSE_FRIVOLOUS,
                BUREAU_RESPONSE_UNABLE, BUREAU_RESPONSE_NO_RESPONSE,
                BUREAU_RESPONSE_REINSERTION,
            ],
        }

    return {
        "letter":        letter,
        "response_type": response_type,
        "bureau":        bureau,
        "account_count": len(accounts),
        "next_steps":    next_steps,
    }

# =========================
# COMPARISON ENGINE
# =========================
#
# Compares Round N vs Round N+1 credit reports for the same client.
# Determines what was removed, what remained, what worsened, and
# what was reinserted — then generates targeted R2 strategy.
#
# Flow:
#   1. build_round_snapshot(result, round_num)  → compact snapshot dict
#   2. compare_rounds(snapshot_r1, snapshot_r2) → comparison_result
#   3. build_comparison_report(comparison_result) → human-readable text
#   4. filter_remaining_for_r2(comparison_result, result_r2) → filtered
#      letter_input_engine containing only accounts that need R2 letters
#
# Account matching uses a normalized fingerprint:
#   fingerprint = NORMALIZED_NAME + "::" + DIGITS_ONLY(acct_number)
#   Handles masking changes across reports (1234**** == 1234XXXX)
#
# Outcome codes per account per bureau:
#   REMOVED    — was in R1 negatives, gone from R2 → success
#   REMAINED   — still negative in R2 → needs R2 letter
#   IMPROVED   — still present but status/balance improved
#   WORSENED   — still present, balance increased or status deteriorated
#   NEW        — appeared in R2 but was not in R1 negatives
#   REINSERTED — was REMOVED in a prior round, now back → §1681i(a)(5)(B)

OUTCOME_REMOVED    = "removed"
OUTCOME_REMAINED   = "remained"
OUTCOME_IMPROVED   = "improved"
OUTCOME_WORSENED   = "worsened"
OUTCOME_NEW        = "new"
OUTCOME_REINSERTED = "reinserted"


def _normalize_name(name: str) -> str:
    """Normalize furnisher name for cross-report matching."""
    import re as _re
    n = name.upper().strip()
    # Remove original creditor suffix
    if "(ORIGINAL CREDITOR:" in n:
        n = n[:n.index("(ORIGINAL CREDITOR:")].strip()
    # Remove common legal suffixes
    for suffix in (" LLC", " INC", " CORP", " LTD", " NA", " N A", " N.A.", " FSB"):
        if n.endswith(suffix):
            n = n[:-len(suffix)].strip()
    # Strip punctuation except spaces
    n = _re.sub(r"[^A-Z0-9 ]", "", n)
    # Collapse spaces
    n = _re.sub(r"\s+", " ", n).strip()
    return n


def _digits_only(s: str) -> str:
    """Extract only digit characters — strips masking (*, X, x, -)."""
    import re as _re
    return _re.sub(r"[^0-9]", "", s)


def _account_fingerprint(name: str, account_number: str) -> str:
    """
    Stable identity key for one account across reports.
    Uses normalized name + extracted digits from account number.
    Handles masking format changes between reports AND collector
    name abbreviation differences (NCA == NATIONAL CREDIT ADJUST).
    """
    # Apply collector normalization first (NCA → NATIONAL CREDIT ADJUST)
    try:
        norm = _normalize_collector_name(name)
    except Exception:
        norm = name
    name_key  = _normalize_name(norm)
    acct_key  = _digits_only(account_number)
    if not acct_key and account_number:
        acct_key = account_number.strip()[-6:]
    return f"{name_key}::{acct_key}"


def build_round_snapshot(
    result: dict[str, Any],
    round_num: int,
    consumer_name: str = "",
) -> dict[str, Any]:
    """
    Create a compact snapshot of one round's negative accounts.
    Store this JSON after processing each round to enable comparison later.

    Returns a dict suitable for JSON serialization and storage.
    """
    negatives = result.get("negatives_by_bureau", {})
    ld        = result.get("legal_detection_engine", {})
    letters   = result.get("dispute_letters", {})
    fl        = result.get("furnisher_letters", {})
    inq       = result.get("inquiry_letters", {})

    # Build per-bureau account snapshots
    accounts_by_bureau: dict[str, list[dict[str, Any]]] = {}
    for bureau, accs in negatives.items():
        bureau_list = []
        for acc in accs:
            name   = acc.get("name", "")
            acct   = acc.get("account_number", "")
            bureau_list.append({
                "fingerprint":    _account_fingerprint(name, acct),
                "name":           name,
                "account_number": acct,
                "bureau":         bureau,
                "negative_type":  acc.get("negative_type", ""),
                "status":         acc.get("status", ""),
                "payment_status": acc.get("payment_status", ""),
                "balance":        acc.get("balance", ""),
                "past_due":       acc.get("past_due", ""),
                "date_opened":    acc.get("date_opened", ""),
                "dofd_estimated": acc.get("dofd_estimated", ""),
                "is_obsolete":    acc.get("is_obsolete", False),
            })
        accounts_by_bureau[bureau] = bureau_list

    # Attack summary
    attack_counts: dict[str, int] = {}
    for bureau, attacks in ld.items():
        for a in attacks:
            at = a.get("attack_type", "")
            attack_counts[at] = attack_counts.get(at, 0) + 1

    total_bureau_letters = sum(
        len(rounds)
        for groups in letters.values()
        for rounds in groups.values()
    )

    return {
        "round":           round_num,
        "report_date":     result.get("report_date", ""),
        "consumer_name":   consumer_name,
        "accounts_by_bureau": accounts_by_bureau,
        "negative_counts": {
            bureau: len(accs)
            for bureau, accs in accounts_by_bureau.items()
        },
        "total_negatives": sum(
            len(accs) for accs in accounts_by_bureau.values()
        ),
        "attack_counts":   attack_counts,
        "total_attacks":   sum(attack_counts.values()),
        "letter_counts": {
            "bureau":    total_bureau_letters,
            "furnisher": len(fl),
            "inquiry":   len(inq),
        },
    }


def compare_rounds(
    snapshot_r1: dict[str, Any],
    snapshot_r2: dict[str, Any],
    prior_removed_fingerprints: set[str] | None = None,
) -> dict[str, Any]:
    """
    Compare two round snapshots. Returns a comprehensive comparison result.

    prior_removed_fingerprints: fingerprints of accounts removed in any prior
    round — used to detect reinsertion (account removed then reappeared).

    Returns:
    {
        "by_bureau": {
            "transunion": {
                "removed":    [...accounts...],
                "remained":   [...accounts with outcome details...],
                "improved":   [...],
                "worsened":   [...],
                "new":        [...],
                "reinserted": [...],
            }
        },
        "summary": {
            "total_r1_negatives": int,
            "total_r2_negatives": int,
            "removed_count": int,
            "remained_count": int,
            "improved_count": int,
            "worsened_count": int,
            "new_count": int,
            "reinserted_count": int,
            "removal_rate": float,   # % of R1 negatives removed
            "net_change": int,       # R2 negatives - R1 negatives
        },
        "reinserted_fingerprints": set[str],  # for passing to round 3+
        "escalation_required": bool,
        "reinsertion_alerts": [...],
    }
    """
    prior_removed = prior_removed_fingerprints or set()

    by_bureau: dict[str, dict[str, list]] = {}
    all_reinserted: list[dict] = []

    bureaus = set(list(snapshot_r1["accounts_by_bureau"].keys()) +
                  list(snapshot_r2["accounts_by_bureau"].keys()))

    total_r1 = total_r2 = 0
    total_removed = total_remained = total_improved = 0
    total_worsened = total_new = total_reinserted = 0

    def _parse_bal(s: str) -> float:
        try: return float(str(s).replace("$","").replace(",",""))
        except: return 0.0

    def _status_score(status: str, payment: str) -> int:
        """Higher = worse. Used to detect improvement/worsening."""
        s = (status + " " + payment).lower()
        if "derogatory" in s:      return 4
        if "chargeoff" in s:       return 3
        if "collection" in s:      return 3
        if "late" in s:            return 2
        if "closed" in s:          return 1
        return 0

    for bureau in bureaus:
        r1_accs = snapshot_r1["accounts_by_bureau"].get(bureau, [])
        r2_accs = snapshot_r2["accounts_by_bureau"].get(bureau, [])

        r1_by_fp: dict[str, dict] = {a["fingerprint"]: a for a in r1_accs}
        r2_by_fp: dict[str, dict] = {a["fingerprint"]: a for a in r2_accs}

        removed    = []
        remained   = []
        improved   = []
        worsened   = []
        new_items  = []
        reinserted = []

        total_r1 += len(r1_accs)
        total_r2 += len(r2_accs)

        # Accounts in R1 — where did they go?
        for fp, r1_acc in r1_by_fp.items():
            if fp not in r2_by_fp:
                removed.append({**r1_acc, "outcome": OUTCOME_REMOVED})
                total_removed += 1
            else:
                r2_acc = r2_by_fp[fp]
                r1_bal = _parse_bal(r1_acc.get("balance",""))
                r2_bal = _parse_bal(r2_acc.get("balance",""))
                r1_score = _status_score(r1_acc.get("status",""), r1_acc.get("payment_status",""))
                r2_score = _status_score(r2_acc.get("status",""), r2_acc.get("payment_status",""))

                delta_bal   = r2_bal - r1_bal
                delta_score = r2_score - r1_score

                if delta_score < 0 or (delta_bal < -10 and delta_score <= 0):
                    outcome = OUTCOME_IMPROVED
                    total_improved += 1
                    improved.append({**r2_acc, "outcome": outcome,
                                     "r1_balance": r1_acc.get("balance"),
                                     "balance_change": f"{delta_bal:+.2f}",
                                     "r1_status": r1_acc.get("status"),
                                     "r1_payment": r1_acc.get("payment_status")})
                elif delta_score > 0 or delta_bal > 50:
                    outcome = OUTCOME_WORSENED
                    total_worsened += 1
                    worsened.append({**r2_acc, "outcome": outcome,
                                     "r1_balance": r1_acc.get("balance"),
                                     "balance_change": f"{delta_bal:+.2f}",
                                     "r1_status": r1_acc.get("status"),
                                     "r1_payment": r1_acc.get("payment_status")})
                else:
                    outcome = OUTCOME_REMAINED
                    total_remained += 1
                    remained.append({**r2_acc, "outcome": outcome,
                                     "r1_balance": r1_acc.get("balance"),
                                     "balance_change": f"{delta_bal:+.2f}"})

        # Accounts in R2 not in R1
        for fp, r2_acc in r2_by_fp.items():
            if fp not in r1_by_fp:
                if fp in prior_removed:
                    outcome = OUTCOME_REINSERTED
                    total_reinserted += 1
                    reinserted.append({**r2_acc, "outcome": outcome,
                                       "law": "15 USC §1681i(a)(5)(B)",
                                       "note": "Previously removed — reinsertion without proper notice"})
                    all_reinserted.append({**r2_acc, "bureau": bureau})
                else:
                    outcome = OUTCOME_NEW
                    total_new += 1
                    new_items.append({**r2_acc, "outcome": outcome})

        by_bureau[bureau] = {
            "removed":    removed,
            "remained":   remained,
            "improved":   improved,
            "worsened":   worsened,
            "new":        new_items,
            "reinserted": reinserted,
        }

    removal_rate = (total_removed / total_r1 * 100) if total_r1 > 0 else 0.0

    # Collect all removed fingerprints for future rounds
    all_removed_fps: set[str] = set()
    for bureau_data in by_bureau.values():
        for acc in bureau_data["removed"]:
            all_removed_fps.add(acc["fingerprint"])

    return {
        "round_from":   snapshot_r1.get("round", 1),
        "round_to":     snapshot_r2.get("round", 2),
        "report_date_r1": snapshot_r1.get("report_date", ""),
        "report_date_r2": snapshot_r2.get("report_date", ""),
        "by_bureau":    by_bureau,
        "summary": {
            "total_r1_negatives": total_r1,
            "total_r2_negatives": total_r2,
            "removed_count":   total_removed,
            "remained_count":  total_remained,
            "improved_count":  total_improved,
            "worsened_count":  total_worsened,
            "new_count":       total_new,
            "reinserted_count":total_reinserted,
            "removal_rate":    round(removal_rate, 1),
            "net_change":      total_r2 - total_r1,
        },
        "removed_fingerprints": all_removed_fps,
        "escalation_required":  total_remained > 0 or total_worsened > 0,
        "reinsertion_alerts":   all_reinserted,
    }


def build_comparison_report(
    comparison: dict[str, Any],
    consumer_name: str = "[CLIENT NAME]",
) -> str:
    """
    Human-readable comparison report. Sent to client and filed to Drive.
    Summarizes wins, what needs escalation, and any reinsertion violations.
    """
    s       = comparison["summary"]
    r_from  = comparison.get("round_from", 1)
    r_to    = comparison.get("round_to", 2)
    d1      = comparison.get("report_date_r1", "")
    d2      = comparison.get("report_date_r2", "")

    removal_rate = s["removal_rate"]
    grade = (
        "Excellent" if removal_rate >= 70 else
        "Good"      if removal_rate >= 40 else
        "Moderate"  if removal_rate >= 20 else
        "Low"
    )

    lines = [
        f"CREDIT DISPUTE COMPARISON REPORT",
        f"{'='*60}",
        f"Client:       {consumer_name}",
        f"Round {r_from} report:  {d1}",
        f"Round {r_to} report:  {d2}",
        f"{'='*60}",
        f"",
        f"OVERALL RESULTS",
        f"{'─'*40}",
        f"Negative accounts (Round {r_from}): {s['total_r1_negatives']}",
        f"Negative accounts (Round {r_to}): {s['total_r2_negatives']}",
        f"",
        f"  Removed (success):      {s['removed_count']}",
        f"  Improved (partial win): {s['improved_count']}",
        f"  Remained (escalate):    {s['remained_count']}",
        f"  Worsened (escalate):    {s['worsened_count']}",
        f"  New items:              {s['new_count']}",
        f"  Reinserted (⚠ ALERT):  {s['reinserted_count']}",
        f"",
        f"  Removal rate:  {removal_rate}% ({grade})",
        f"  Net change:    {s['net_change']:+d} accounts",
        f"",
    ]

    # Reinsertion alerts
    if comparison["reinsertion_alerts"]:
        lines += [
            f"⚠ REINSERTION ALERT — §1681i(a)(5)(B) VIOLATION",
            f"{'─'*40}",
            f"The following accounts were previously removed but have",
            f"reappeared without proper notice. This is a willful FCRA",
            f"violation. File CFPB complaint and escalate immediately.",
            f"",
        ]
        for acc in comparison["reinsertion_alerts"]:
            lines.append(f"  • {acc.get('name','')} — #{acc.get('account_number','')} [{acc.get('bureau','').title()}]")
        lines.append("")

    # Per-bureau breakdown
    for bureau, data in comparison["by_bureau"].items():
        total_in_bureau = sum(len(v) for v in data.values())
        if total_in_bureau == 0:
            continue

        lines += [
            f"{bureau.upper()}",
            f"{'─'*40}",
        ]

        if data["removed"]:
            lines.append(f"  ✓ REMOVED ({len(data['removed'])}):")
            for acc in data["removed"]:
                lines.append(f"    • {acc['name']} — #{acc['account_number']}")

        if data["reinserted"]:
            lines.append(f"  ⚠ REINSERTED ({len(data['reinserted'])}) — FILE CFPB COMPLAINT:")
            for acc in data["reinserted"]:
                lines.append(f"    • {acc['name']} — #{acc['account_number']}")

        if data["remained"]:
            lines.append(f"  → REMAINED ({len(data['remained'])}) — send Round {r_to} letter:")
            for acc in data["remained"]:
                bal_note = f" [balance: {acc.get('r1_balance','')} → {acc.get('balance','')}]" if acc.get("balance_change","").strip("+0.") else ""
                lines.append(f"    • {acc['name']} — #{acc['account_number']}{bal_note}")

        if data["improved"]:
            lines.append(f"  ↑ IMPROVED ({len(data['improved'])}) — monitor:")
            for acc in data["improved"]:
                lines.append(f"    • {acc['name']} — #{acc['account_number']} [was {acc.get('r1_status','')} → {acc.get('status','')}]")

        if data["worsened"]:
            lines.append(f"  ↓ WORSENED ({len(data['worsened'])}) — escalate:")
            for acc in data["worsened"]:
                lines.append(f"    • {acc['name']} — #{acc['account_number']} [balance change: {acc.get('balance_change','')}]")

        if data["new"]:
            lines.append(f"  + NEW ({len(data['new'])}) — dispute if unauthorized:")
            for acc in data["new"]:
                lines.append(f"    • {acc['name']} — #{acc['account_number']}")

        lines.append("")

    # Next steps
    lines += [
        f"RECOMMENDED NEXT STEPS",
        f"{'─'*40}",
    ]
    if s["reinserted_count"] > 0:
        lines.append(f"  1. FILE CFPB COMPLAINT immediately for reinserted items")
        lines.append(f"     URL: https://www.consumerfinance.gov/complaint/")
    if s["remained_count"] + s["worsened_count"] > 0:
        lines.append(f"  {'2' if s['reinserted_count'] > 0 else '1'}. Send Round {r_to} dispute letters for all 'Remained' and 'Worsened' accounts")
    if s["new_count"] > 0:
        lines.append(f"  • Review new items — dispute any that should not have appeared")
    if s["removed_count"] > 0:
        lines.append(f"  • Monitor removed items — if any reappear, file reinsertion claim")
    if s["remained_count"] + s["worsened_count"] == 0 and s["reinserted_count"] == 0:
        lines.append(f"  • All disputed items resolved. Continue monitoring.")

    return "\n".join(lines)


def filter_remaining_for_r2(
    comparison: dict[str, Any],
    result_r2: dict[str, Any],
) -> dict[str, Any]:
    """
    Filter the R2 result's letter_input_engine to only include accounts
    that REMAINED, WORSENED, IMPROVED (still present), or REINSERTED.

    Accounts that were REMOVED don't need R2 letters.
    New items get fresh R1 treatment.

    Returns a modified letter_input_engine ready for build_dispute_letter_engine().
    """
    # Build set of fingerprints that need escalation
    escalation_fps: set[str] = set()
    reinsertion_fps: set[str] = set()

    for bureau, data in comparison["by_bureau"].items():
        for outcome in ("remained", "improved", "worsened"):
            for acc in data[outcome]:
                escalation_fps.add(acc["fingerprint"])
        for acc in data["reinserted"]:
            reinsertion_fps.add(acc["fingerprint"])
            escalation_fps.add(acc["fingerprint"])

    # Deep-copy and filter letter_input_engine
    import copy
    lei_r2 = result_r2.get("letter_input_engine", {})
    filtered: dict[str, dict[str, list]] = {}

    for bureau, groups in lei_r2.items():
        filtered[bureau] = {}
        for group, items in groups.items():
            kept = []
            for item in items:
                name = item.get("furnisher_name", "")
                acct = item.get("account_number", "")
                fp   = _account_fingerprint(name, acct)

                if fp in reinsertion_fps:
                    # Upgrade attack type for reinsertion items
                    item_copy = dict(item)
                    item_copy["attack_type"] = "reinsertion_violation"
                    item_copy["laws"] = [
                        "15 USC 1681i(a)(5)(B)",
                        "15 USC 1681n",
                        "15 USC 1681e(b)",
                    ]
                    kept.append(item_copy)
                elif fp in escalation_fps:
                    kept.append(item)
                # else: REMOVED — skip

            if kept:
                filtered[bureau][group] = kept

    return filtered


# =========================
# IDENTITY THEFT BLOCK ENGINE
# =========================
#
# §1681c-2 (FCRA §605B) — Block of Information Resulting from Identity Theft
#
# This is NOT a dispute. It is a mandatory block.
# Key differences from §1681i dispute:
#
#   DISPUTE (§1681i):   Bureau has 30 days. May respond "verified." No guarantee of removal.
#   BLOCK (§1681c-2):   Bureau MUST block within 4 BUSINESS DAYS. No verification defense.
#                       Furnisher must not re-report the blocked information.
#
# Required documents the consumer must attach:
#   1. Government-issued ID (driver's license, passport)
#   2. FTC Identity Theft Report from IdentityTheft.gov (police report optional)
#   3. List of specific fraudulent accounts/items
#   4. Signed statement that items resulted from identity theft
#
# When to use block vs dispute:
#   BLOCK:   Account opened fraudulently, unauthorized inquiry, address added by thief
#   DISPUTE: Accurate account with inaccurate information (wrong balance, status, etc.)
#
# Companion tools:
#   §1681c-1 Fraud Alert — placed at one bureau, notified to all three. 1-year or 7-year.
#   Credit Freeze — stronger than fraud alert. Stops new credit from being opened.
#
# FTC report URL: https://www.identitytheft.gov/
# Police report:  Optional but recommended. Some creditors require both.


def detect_potential_identity_theft_indicators(
    accounts: list[dict[str, Any]],
    known_creditors: list[str] | None = None,
) -> list[dict[str, Any]]:
    """
    Flag accounts that may indicate identity theft based on credit report data.
    These are INDICATORS — not confirmations. Consumer must verify.

    Flags:
      - Account opened recently that consumer doesn't recognize
      - Unknown furnisher name (not in consumer's known creditors list)
      - Account type unusual for consumer's profile
      - Multiple new accounts same date (rapid opening pattern)
      - Address or personal info not matching consumer's records
    """
    indicators: list[dict[str, Any]] = []
    known = {n.upper().strip() for n in (known_creditors or [])}

    # Group by date_opened for rapid-opening detection
    from collections import defaultdict
    by_open_date: dict[str, list] = defaultdict(list)
    for acc in accounts:
        d = acc.get("date_opened", "")
        if d:
            by_open_date[d].append(acc)

    for acc in accounts:
        name    = acc.get("name", "")
        acct    = acc.get("account_number", "")
        opened  = acc.get("date_opened", "")
        flags   = []

        # Unknown creditor (consumer provided known list)
        if known and name.upper().strip() not in known:
            flags.append(f"Unrecognized creditor: {name}")

        # Multiple accounts opened same day (rapid-opening pattern)
        if opened and len(by_open_date.get(opened, [])) >= 3:
            flags.append(f"Rapid account opening: {len(by_open_date[opened])} accounts on {opened}")

        if flags:
            indicators.append({
                "name":           name,
                "account_number": acct,
                "date_opened":    opened,
                "flags":          flags,
                "bureau":         acc.get("bureau", ""),
                "negative_type":  acc.get("negative_type", ""),
            })

    return indicators


def build_identity_theft_block_letter(
    bureau: str,
    fraudulent_accounts: list[dict[str, Any]],
    consumer_name: str,
    consumer_address: str = "[Address]",
    consumer_city_state_zip: str = "[City, State ZIP]",
    consumer_dob: str = "",
    consumer_ssn_last4: str = "",
    ftc_report_number: str = "",
    police_report_number: str = "",
    police_department: str = "",
    report_date: str = "",
) -> str:
    """
    Generate a §1681c-2 identity theft block request letter.

    The consumer must attach to this letter:
      1. Copy of government-issued ID
      2. FTC Identity Theft Report (from IdentityTheft.gov)
      3. Police report (optional but recommended)

    The bureau MUST block the identified items within 4 business days.
    """
    bureau_info    = BUREAU_ADDRESSES.get(bureau, {})
    bureau_name    = bureau_info.get("name", bureau.title())
    bureau_address = bureau_info.get("address", "")
    date_str       = _format_date_long(report_date)

    # Build DOB/SSN identity line
    identity_line = ""
    if consumer_dob:
        identity_line += f"Date of Birth: {consumer_dob}\n"
    if consumer_ssn_last4:
        identity_line += f"Last 4 of SSN: {consumer_ssn_last4}\n"

    # Build FTC/police reference
    report_refs = []
    if ftc_report_number:
        report_refs.append(f"FTC Identity Theft Report Number: {ftc_report_number}")
    if police_report_number and police_department:
        report_refs.append(f"Police Report Number: {police_report_number} ({police_department})")
    elif police_report_number:
        report_refs.append(f"Police Report Number: {police_report_number}")
    report_ref_block = "\n".join(report_refs) if report_refs else "[FTC Report Number / Police Report Number]"

    # Account list
    account_lines = []
    for i, acc in enumerate(fraudulent_accounts, 1):
        name    = acc.get("name", acc.get("furnisher_name", "Unknown"))
        acct    = acc.get("account_number", "Unknown")
        opened  = acc.get("date_opened", "")
        balance = acc.get("balance", "")
        details = []
        if opened:  details.append(f"Opened: {opened}")
        if balance: details.append(f"Balance: {balance}")
        detail_str = f" ({', '.join(details)})" if details else ""
        account_lines.append(f"  {i}. {name} — Account #{acct}{detail_str}")

    accounts_block = "\n".join(account_lines)

    letter = f"""{consumer_name}
{consumer_address}
{consumer_city_state_zip}
{identity_line}
{bureau_name}
{bureau_address}

{date_str}

RE: IDENTITY THEFT BLOCK REQUEST UNDER 15 U.S.C. §1681c-2 (FCRA §605B)

To Whom It May Concern:

I am writing to formally request that {bureau_name} block the fraudulent accounts and information listed below from my credit file under 15 U.S.C. §1681c-2 (Fair Credit Reporting Act §605B).

I am a victim of identity theft. The accounts and items identified in this letter were opened or created without my knowledge or authorization. I am enclosing a copy of my FTC Identity Theft Report and proof of my identity as required by §1681c-2(a).

Under 15 U.S.C. §1681c-2(a), {bureau_name} is required to block the identified information from my credit file no later than 4 BUSINESS DAYS from the date of receipt of this letter and the required documentation. This block is mandatory under federal law — it is not subject to a 30-day reinvestigation period.

{report_ref_block}

The following accounts and items are the result of identity theft and must be blocked:

{accounts_block}

I certify that the information I have provided is accurate and that the items listed above resulted from identity theft as described in my enclosed Identity Theft Report.

Please also note the following legal requirements that apply to this block:

1. BLOCK WITHIN 4 BUSINESS DAYS: Under §1681c-2(a), you must block this information no later than 4 business days after receiving this letter and the required documentation.

2. NOTIFY THE FURNISHER: Under §1681c-2(a), once the block is placed, you must notify the furnisher of the blocked accounts that the information has been blocked. The furnisher is then prohibited from re-reporting the blocked information under §1681s-2.

3. CONSUMER NOTICE: If you decline to block any item, you must notify me promptly in the same manner required for reinsertion notice under §1681i(a)(5)(B).

4. NO VERIFICATION DEFENSE: The block procedure under §1681c-2 is distinct from the dispute reinvestigation procedure under §1681i. A finding that information is "accurate" does not override the mandatory block requirement — the standard is whether the information resulted from identity theft, not whether it is accurate.

Failure to comply with §1681c-2 may constitute a willful violation of the FCRA, subject to statutory damages of $100 to $1,000 per violation under §1681n, plus punitive damages and attorney fees.

Enclosed:
  [ ] Copy of government-issued photo ID
  [ ] FTC Identity Theft Report from IdentityTheft.gov
  [ ] Police report (if obtained)
  [ ] This signed block request letter

Please confirm in writing that the block has been placed and provide the date on which the block was applied.

Sincerely,

{consumer_name}
"""
    return letter


def build_fraud_alert_letter(
    alert_type: str = "initial",
    consumer_name: str = "[CLIENT NAME]",
    consumer_address: str = "[Address]",
    consumer_city_state_zip: str = "[City, State ZIP]",
    consumer_phone: str = "[Phone Number]",
    consumer_dob: str = "",
    consumer_ssn_last4: str = "",
    report_date: str = "",
) -> str:
    """
    Generate a fraud alert placement letter under §1681c-1.

    alert_type:
      "initial"  — 1 year, placed at one bureau (they notify the others)
      "extended" — 7 years, requires Identity Theft Report, entitles to 2 free reports/year

    The consumer only needs to contact ONE bureau — that bureau must notify the other two.
    Recommended: contact TransUnion first (fastest processing).
    """
    date_str = _format_date_long(report_date)

    if alert_type == "extended":
        duration    = "seven (7) years"
        law_cite    = "15 U.S.C. §1681c-1(b)"
        entitlement = (
            "As required by §1681c-1(b)(1)(B), please also provide me with "
            "two free copies of my credit report during the first twelve months "
            "following placement of this extended alert."
        )
        requires_report = True
    else:
        duration    = "one (1) year"
        law_cite    = "15 U.S.C. §1681c-1(a)"
        entitlement = (
            "As required by §1681c-1(a)(2), please also provide me with "
            "a free copy of my credit report."
        )
        requires_report = False

    identity_line = ""
    if consumer_dob:    identity_line += f"Date of Birth: {consumer_dob}\n"
    if consumer_ssn_last4: identity_line += f"Last 4 of SSN: {consumer_ssn_last4}\n"
    if consumer_phone:  identity_line += f"Phone Number: {consumer_phone}\n"

    # Only send to TransUnion — they notify the others
    bureau_info    = BUREAU_ADDRESSES.get("transunion", {})
    bureau_name    = bureau_info.get("name", "TransUnion")
    bureau_address = bureau_info.get("address", "")

    letter = f"""{consumer_name}
{consumer_address}
{consumer_city_state_zip}
{identity_line}
{bureau_name}
{bureau_address}

{date_str}

RE: FRAUD ALERT PLACEMENT REQUEST UNDER {law_cite}

To Whom It May Concern:

I am requesting that you place a {alert_type.upper()} fraud alert on my credit file pursuant to {law_cite}.

I am a victim of identity theft (or have reason to believe I may become a victim). A fraud alert requires that any user of my credit report take reasonable steps to verify my identity before extending credit in my name.

{identity_line}
Under {law_cite}, you are required to:
1. Place the fraud alert on my credit file for {duration}
2. Notify Experian and Equifax of the fraud alert so they place it on their files as well
3. Provide me with a free copy of my credit report

{entitlement}

{"I am enclosing a copy of my FTC Identity Theft Report as required for an extended fraud alert." if requires_report else ""}

Please confirm in writing that the fraud alert has been placed on my file at all three bureaus.

Sincerely,

{consumer_name}
"""
    return letter


def build_identity_theft_action_guide(
    consumer_name: str,
    fraudulent_accounts: list[dict[str, Any]] | None = None,
    consumer_state: str = "",
) -> str:
    """
    Complete step-by-step guide for identity theft victims.
    Generated alongside the block letters to give client a clear roadmap.
    """
    fraudulent_accounts = fraudulent_accounts or []
    num_accounts = len(fraudulent_accounts)
    account_summary = (
        f"\n".join(
            f"  • {acc.get('name', acc.get('furnisher_name','?'))} "
            f"— #{acc.get('account_number','?')}"
            for acc in fraudulent_accounts
        )
        if fraudulent_accounts else "  • (Accounts to be identified from your credit reports)"
    )

    state_note = ""
    if consumer_state.upper() in {"CA", "CALIFORNIA"}:
        state_note = (
            "\nCALIFORNIA NOTE: California law (CA Civ. Code §1785.16) requires "
            "creditors and debt collectors to accept an FTC Identity Theft Report "
            "alone — without a police report — as sufficient proof of identity theft."
        )

    guide = f"""IDENTITY THEFT RECOVERY ACTION GUIDE
{'='*60}
Client: {consumer_name}
Fraudulent accounts identified: {num_accounts}

{account_summary}
{'='*60}

UNDERSTAND YOUR RIGHTS
{'─'*40}
You have TWO powerful tools under federal law:

  1. FRAUD ALERT (§1681c-1)
     • Flags your file so lenders must verify your identity before opening new accounts
     • Place at ONE bureau — that bureau must notify the other two
     • Initial: 1 year | Extended (with ID theft report): 7 years
     • FREE to place

  2. IDENTITY THEFT BLOCK (§1681c-2)
     • Forces bureaus to REMOVE fraudulent information within 4 BUSINESS DAYS
     • Stronger than a dispute — bureau cannot say "verified" as a defense
     • Furnisher is prohibited from re-reporting the blocked item
     • Requires: (a) ID, (b) FTC Identity Theft Report, (c) list of fraudulent items

  3. CREDIT FREEZE (§1681c-1 / state law)
     • Prevents any new credit from being opened in your name
     • Must be placed at EACH bureau separately
     • FREE under federal law

STEP-BY-STEP PROCESS
{'─'*40}

STEP 1: FILE FTC IDENTITY THEFT REPORT (Do this FIRST)
  → Go to: https://www.identitytheft.gov/
  → Fill out the online form — takes ~15 minutes
  → Download and save your Identity Theft Report (PDF)
  → Your FTC report number will be on the report
  → This report is SUFFICIENT for §1681c-2 block requests (no police report required)
  → TIME: Same day

STEP 2: PLACE FRAUD ALERT
  → Contact TransUnion ONLY — they must notify Experian and Equifax
  → Use the enclosed Fraud Alert Letter or call: 1-800-680-7289
  → Request EXTENDED fraud alert (7 years) if you have your FTC report
  → TransUnion will send you free credit reports from all 3 bureaus
  → TIME: Same day or next business day

STEP 3: FILE POLICE REPORT (Recommended)
  → Take your FTC Identity Theft Report to your local police department
  → Ask them to attach the FTC report to the police file
  → Get a copy of the police report with report number
  → Note: FTC report alone is legally sufficient — police report strengthens the case
  → TIME: 1-3 days
  {state_note}

STEP 4: SEND IDENTITY THEFT BLOCK LETTERS
  → Use the enclosed block letters for each bureau
  → Each letter must include:
      ✓ Copy of government-issued photo ID
      ✓ FTC Identity Theft Report (PDF)
      ✓ Police report (if obtained)
      ✓ Signed block request letter
  → Send CERTIFIED MAIL to each bureau (keep tracking numbers)
  → Bureau addresses:
      TransUnion:  PO Box 2000, Chester, PA 19016
      Experian:    P.O. Box 4500, Allen, TX 75013
      Equifax:     P.O. Box 740256, Atlanta, GA 30374
  → TIME: Send within 24-48 hours of obtaining FTC report

STEP 5: TRACK THE 4-BUSINESS-DAY DEADLINE
  → Block must be placed within 4 BUSINESS DAYS of bureau receiving your package
  → Mark your calendar: certified mail delivery date + 4 business days
  → If block not confirmed by that deadline, send follow-up and file CFPB complaint
  → CFPB complaint: https://www.consumerfinance.gov/complaint/

STEP 6: CONTACT EACH FRAUDULENT CREDITOR DIRECTLY
  → Call each creditor listed on your account summary
  → Tell them the account was opened fraudulently — not your debt
  → They must give you details about the account if you ask
  → They must stop collecting and reporting once they receive block notification
  → Request written confirmation from each creditor

STEP 7: PLACE CREDIT FREEZE (Optional but recommended)
  → Contact each bureau separately to place a credit freeze
  → Experian:    experian.com/freeze or 1-888-397-3742
  → TransUnion:  transunion.com/credit-freeze or 1-888-909-8872
  → Equifax:     equifax.com/personal/credit-report-services or 1-800-685-1111
  → FREE under federal law (15 U.S.C. §1681c-1)
  → You can temporarily lift the freeze when you need to apply for credit

STEP 8: MONITOR AND FOLLOW UP
  → After 4 business days: call each bureau to confirm block is in place
  → Pull new credit reports (you're entitled to free reports after fraud alert)
  → If blocked items reappear: immediate reinsertion claim under §1681i(a)(5)(B)
  → Keep all documentation: tracking numbers, call logs, written confirmations

IMPORTANT WARNINGS
{'─'*40}
  ⚠ Do NOT pay any of the fraudulent debts — paying may be interpreted as acknowledging the debt
  ⚠ Do NOT close legitimate accounts — this can hurt your credit score
  ⚠ Keep COPIES of everything — every letter sent, every confirmation received
  ⚠ Watch for new fraudulent accounts — pull credit reports every 90 days for 1 year
  ⚠ If a furnisher keeps reporting after the block: FCRA §1681s-2 violation — consult FCRA attorney

TIMELINE SUMMARY
{'─'*40}
  Day 1:          File FTC report, place fraud alert
  Day 1-3:        File police report, send block letters via certified mail
  Day 4-7:        Bureaus receive letters
  Day 8-11:       4-business-day block deadline
  Day 12+:        Confirm blocks, contact creditors, pull updated reports
  Ongoing:        Monitor every 90 days for 1 year

LEGAL REMEDIES IF BUREAUS DON'T COMPLY
{'─'*40}
  • CFPB Complaint: consumerfinance.gov/complaint
  • Willful violation of §1681c-2: $100–$1,000 per violation + punitive damages (§1681n)
  • Negligent violation: actual damages + attorney fees (§1681o)
  • FCRA attorney: National Association of Consumer Advocates — naca.net/find-an-attorney
"""
    return guide


# =========================
# CFPB ENGINE
# =========================
#
# The CFPB is a force multiplier, not the primary legal tool.
# Our dispute letters use FCRA §1681i as the primary mechanism.
# The CFPB adds:
#
#   1. COMPLAINT LANGUAGE — citing a concurrent CFPB complaint in our
#      letters signals regulatory exposure to the bureau. Bureaus respond
#      faster and more seriously when CFPB complaints are filed.
#
#   2. COMPLAINT TEMPLATE — structured guide for client to file at
#      consumerfinance.gov. Covers every account disputed, with specific
#      legal violation cited. Client files this alongside sending letters.
#
#   3. DATA CITATIONS — CFPB research and supervisory findings used to
#      strengthen accuracy arguments. "The CFPB has documented that..."
#      adds weight that a plain §1681e(b) argument lacks.
#
#   4. ESCALATION TRIGGER — after bureau response, CFPB complaint is
#      the escalation path before litigation. The complaint endpoint
#      generates a filing guide for the specific response type.
#
# Current CFPB status (April 2026):
#   - Core FCRA enforcement authority intact
#   - Medical debt rule VACATED (July 2025) — do not cite
#   - Consumer complaint portal still active and effective
#   - FTC retains parallel enforcement authority


# CFPB data points usable in letters — cited from published CFPB reports
_CFPB_DATA_POINTS = {
    "reinvestigation_quality": (
        "The CFPB has documented through supervisory examinations that credit bureau "
        "reinvestigations are frequently automated rather than genuine — the bureau "
        "transmits dispute data electronically to the furnisher and accepts the "
        "furnisher's response without independent review."
    ),
    "medical_accuracy": (
        "The CFPB has published data showing that medical debt is a poor predictor "
        "of a consumer's creditworthiness and that medical bills frequently contain "
        "errors due to insurance billing disputes and coding inaccuracies."
    ),
    "furnisher_duty": (
        "The CFPB has found through supervision that many furnishers fail to update "
        "account records after disputes are resolved, violating their ongoing duty "
        "under 15 U.S.C. §1681s-2 to report accurate information."
    ),
    "dispute_rights": (
        "Under 12 U.S.C. §5511, the CFPB is empowered to enforce federal consumer "
        "financial laws including FCRA. Willful violations are subject to civil "
        "money penalties in addition to damages available to the consumer."
    ),
}

# CFPB complaint URL
CFPB_COMPLAINT_URL = "https://www.consumerfinance.gov/complaint/"

# CFPB protected states for medical debt (current as of April 2026)
# Used in complaint template to note state law violations
_CFPB_MEDICAL_STATES = MEDICAL_DEBT_PROTECTED_STATES  # reuse existing set


def build_cfpb_complaint_language(response_type: str = "") -> str:
    """
    Return a paragraph to append to bureau dispute letters signaling
    that a concurrent CFPB complaint is being filed.
    Tailored to response type — stronger for frivolous/no-response/reinsertion.
    """
    base = (
        "Please be advised that I am filing a concurrent complaint with the "
        "Consumer Financial Protection Bureau (CFPB) regarding this matter "
        f"at {CFPB_COMPLAINT_URL}. "
        "The CFPB has enforcement authority over consumer reporting under "
        "15 U.S.C. §1681s and 12 U.S.C. §5511. "
    )

    if response_type in (BUREAU_RESPONSE_NO_RESPONSE, "no_response_30_days"):
        return base + (
            "Failure to complete a reinvestigation within 30 days as required "
            "by 15 U.S.C. §1681i(a)(1) is a willful violation subject to "
            "statutory damages of $100 to $1,000 per account plus punitive "
            "damages under 15 U.S.C. §1681n. I am preserving my right to "
            "pursue all available remedies."
        )
    elif response_type in (BUREAU_RESPONSE_REINSERTION, "reinsertion"):
        return base + (
            "Reinsertion of a previously deleted item without following the "
            "notice procedure in 15 U.S.C. §1681i(a)(5)(B) is among the "
            "strongest willful violation claims under 15 U.S.C. §1681n. "
            "I am preserving all rights to pursue statutory damages, "
            "punitive damages, and attorney fees."
        )
    elif response_type in (BUREAU_RESPONSE_FRIVOLOUS, "frivolous"):
        return base + (
            "An improper frivolous designation under 15 U.S.C. §1681i(a)(3) "
            "is itself a violation of the FCRA. I am resubmitting this dispute "
            "with full specificity and expect a legitimate reinvestigation. "
            "Continued refusal to process a properly stated dispute will be "
            "included in my CFPB complaint and any subsequent legal action."
        )
    elif response_type in (BUREAU_RESPONSE_VERIFIED, "verified"):
        return base + (
            "I am specifically requesting, under 15 U.S.C. §1681i(a)(6)(B)(iii), "
            "a description of the procedure used in your reinvestigation, "
            "including the name and contact information of every person contacted. "
            "If the procedure consisted solely of an automated ACDV transmission "
            "without independent review of documentation, that does not meet the "
            "statutory standard for a reasonable reinvestigation."
        )
    else:
        return base + (
            "I expect all rights under the Fair Credit Reporting Act to be "
            "honored in full, including the right to accurate information, "
            "timely reinvestigation, and written results of that investigation."
        )


def build_cfpb_complaint_template(
    consumer_name: str,
    consumer_address: str = "[Address]",
    consumer_state: str = "",
    bureau: str = "",
    accounts: list[dict[str, Any]] | None = None,
    response_type: str = "",
    dispute_date: str = "",
    response_date: str = "",
) -> str:
    """
    Generate a structured CFPB complaint filing guide.
    The client uses this to file at consumerfinance.gov.

    The complaint is not a letter — it is a plain-language description
    of what happened and what was wrong, structured for the CFPB portal.
    """
    accounts = accounts or []
    bureau_name = {
        "transunion": "TransUnion",
        "experian":   "Experian",
        "equifax":    "Equifax Information Services",
    }.get(bureau.lower(), bureau.title())

    date_str = _format_date_long(dispute_date or response_date)

    # Complaint type based on response
    complaint_type_map = {
        BUREAU_RESPONSE_VERIFIED:    "Bureau verified incorrect information",
        BUREAU_RESPONSE_UPDATED:     "Bureau updated but did not fully correct",
        BUREAU_RESPONSE_FRIVOLOUS:   "Bureau improperly designated dispute as frivolous",
        BUREAU_RESPONSE_NO_RESPONSE: "Bureau did not respond within 30 days",
        BUREAU_RESPONSE_REINSERTION: "Bureau reinserted previously deleted item",
        BUREAU_RESPONSE_UNABLE:      "Bureau unable to process despite valid dispute",
        "initial":                   "Inaccurate information on credit report",
    }
    complaint_type = complaint_type_map.get(response_type, "Inaccurate information on credit report")

    # Build account list for complaint
    account_lines = []
    for i, acc in enumerate(accounts, 1):
        name    = acc.get("name", "Unknown Furnisher")
        acct    = acc.get("account_number", "Unknown")
        at      = acc.get("attack_type", "inaccurate_information")
        balance = acc.get("balance", "")
        bal_str = f" Balance: {balance}." if balance and balance not in ("$0.00", "0") else ""
        account_lines.append(
            f"{i}. {name} — Account #{acct}.{bal_str} "
            f"Issue: {at.replace('_', ' ').title()}."
        )

    accounts_block = "\n".join(account_lines) if account_lines else "See attached dispute letter."

    # State law note
    state_note = ""
    if consumer_state and consumer_state.upper() in _CFPB_MEDICAL_STATES:
        state_note = (
            f"\n\nNote: I reside in {consumer_state.upper()}, which has enacted a "
            f"law restricting medical debt credit reporting. Any medical collection "
            f"accounts included in this complaint may also violate state law."
        )

    # Build the complaint
    complaint = f"""CFPB CONSUMER COMPLAINT FILING GUIDE
{'='*60}

CONSUMER: {consumer_name}
ADDRESS:  {consumer_address}
DATE:     {date_str}

COMPANY COMPLAINED ABOUT:
{bureau_name}

TYPE OF COMPLAINT:
{complaint_type}

FILING URL:
{CFPB_COMPLAINT_URL}

{'='*60}
COMPLAINT NARRATIVE (copy and paste into the CFPB portal)
{'='*60}

I am filing this complaint regarding {bureau_name}'s handling of my credit report dispute.

I submitted a formal dispute under the Fair Credit Reporting Act (FCRA) on {dispute_date or "[DATE OF DISPUTE]"}, disputing the following accounts for the reasons stated:

{accounts_block}

{_cfpb_narrative_for_response(response_type, bureau_name, response_date)}

The following legal requirements apply:
• 15 U.S.C. §1681i(a) — Bureau must complete reinvestigation within 30 days
• 15 U.S.C. §1681e(b) — Bureau must maintain maximum possible accuracy
• 15 U.S.C. §1681s-2 — Furnisher must report accurate information and investigate disputes
• 15 U.S.C. §1681n — Willful violations subject to $100-$1,000 per account + punitive damages{state_note}

WHAT I WANT THE COMPANY TO DO:
Remove or correct all disputed accounts that cannot be verified with actual documentation. Provide written results of any reinvestigation including the name and contact information of every person or company contacted.

{'='*60}
IMPORTANT NOTES FOR FILING
{'='*60}
• Select company type: "Credit reporting company"
• Select product: "Credit reporting, credit repair services, or other personal consumer reports"
• Attach a copy of your dispute letter and any bureau response received
• Attach copies of your credit reports showing the disputed accounts
• Request a public response from the company (increases response pressure)
• Keep your complaint reference number for follow-up
"""
    return complaint


def _cfpb_narrative_for_response(response_type: str, bureau_name: str, response_date: str) -> str:
    """Generate the specific narrative paragraph based on what the bureau did."""
    date_ref = f" on {response_date}" if response_date else ""

    narratives = {
        BUREAU_RESPONSE_VERIFIED: (
            f"{bureau_name} responded{date_ref} stating that the disputed "
            f"information was 'verified.' However, the bureau did not provide "
            f"any description of the reinvestigation procedure used, the name "
            f"of the person or company contacted, or any documentation reviewed. "
            f"A purely automated ACDV process does not constitute a 'reasonable "
            f"reinvestigation' as required by 15 U.S.C. §1681i(a). The disputed "
            f"information remains on my report and continues to damage my credit."
        ),
        BUREAU_RESPONSE_FRIVOLOUS: (
            f"{bureau_name} responded{date_ref} by designating my dispute as "
            f"'frivolous' and refusing to investigate. My dispute was specific, "
            f"identified the accounts by name and account number, and explained "
            f"the exact reason each item is inaccurate. Under 15 U.S.C. "
            f"§1681i(a)(3), a bureau may only refuse to investigate if the "
            f"dispute is 'frivolous or irrelevant' — which requires notice to "
            f"the consumer and a specific explanation. An unjustified frivolous "
            f"designation is itself an FCRA violation."
        ),
        BUREAU_RESPONSE_NO_RESPONSE: (
            f"{bureau_name} has not responded to my dispute as of this filing. "
            f"Under 15 U.S.C. §1681i(a)(1), the bureau must complete its "
            f"reinvestigation and notify me of the results within 30 days of "
            f"receiving my dispute. That period has elapsed without any response. "
            f"Failure to reinvestigate within 30 days is a clear violation of "
            f"the FCRA and the disputed items must be deleted."
        ),
        BUREAU_RESPONSE_REINSERTION: (
            f"After {bureau_name} deleted the disputed items{date_ref}, those "
            f"same items were reinserted on my credit report without the "
            f"required notice under 15 U.S.C. §1681i(a)(5)(B). The law "
            f"requires that before reinserting a previously deleted item, the "
            f"bureau must notify the consumer within 5 business days and "
            f"certify that the furnisher has verified the information. "
            f"This procedure was not followed. Reinsertion without notice "
            f"is one of the most serious FCRA violations."
        ),
        BUREAU_RESPONSE_UPDATED: (
            f"{bureau_name} responded{date_ref} by updating some information "
            f"but did not fully resolve my dispute. The accounts continue to "
            f"reflect inaccurate information despite the bureau's acknowledgment "
            f"that updates were necessary. Under 15 U.S.C. §1681i(a)(5), "
            f"if information cannot be verified it must be deleted — a partial "
            f"update is not sufficient when the underlying accuracy issue remains."
        ),
    }
    return narratives.get(
        response_type,
        (
            f"I am disputing inaccurate information on my {bureau_name} credit "
            f"report and requesting that the bureau conduct a genuine "
            f"reinvestigation under 15 U.S.C. §1681i(a)."
        )
    )


if __name__ == "__main__":
    path = input("PDF path: ").strip()
    result = build_report(path)

    output = Path(path).with_suffix(".parsed.json")
    output.write_text(
        json.dumps(result, indent=2, ensure_ascii=False),
        encoding="utf-8"
    )

    print(f"OK -> {output}")
