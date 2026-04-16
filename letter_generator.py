"""
letter_generator.py
===================
Report Defence – PDF Letter Generator

This module does ONE thing:
  1. Calls the existing letter engines from report_parser.py to produce
     plain-text letters (first-person voice, narrative per account, FCRA compliant)
  2. Converts each text letter to a clean PDF using ReportLab

It does NOT rewrite letter logic, reformat disputes into tables,
add reason codes, or change the voice. The text from the original
engine is the letter. We just put it in a PDF.

Output: /mnt/user-data/outputs/letters/
  Bureau letters:    {client}_{bureau}_{group}_round{N}.pdf
  Furnisher letters: {client}_furnisher_{name}_round{N}.pdf
"""

from __future__ import annotations

import os
import sys
import re
from typing import Any

_HERE = os.path.dirname(os.path.abspath(__file__))
for _p in [_HERE, "/mnt/user-data/outputs", "/home/claude"]:
    if _p not in sys.path:
        sys.path.insert(0, _p)

from reportlab.lib.pagesizes import letter as LETTER_SIZE
from reportlab.lib.units import inch
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, HRFlowable

OUTPUT_DIR = "/mnt/user-data/outputs/letters"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ---------------------------------------------------------------------------
# Styles — minimal, matches a typed letter
# ---------------------------------------------------------------------------
_BASE = getSampleStyleSheet()

STYLES = {
    "normal": ParagraphStyle(
        "rd_normal",
        parent=_BASE["Normal"],
        fontName="Helvetica",
        fontSize=10.5,
        leading=16,
    ),
    "bold": ParagraphStyle(
        "rd_bold",
        parent=_BASE["Normal"],
        fontName="Helvetica-Bold",
        fontSize=10.5,
        leading=16,
    ),
    "small": ParagraphStyle(
        "rd_small",
        parent=_BASE["Normal"],
        fontName="Helvetica",
        fontSize=9,
        leading=13,
        textColor=colors.HexColor("#444444"),
    ),
    "certified": ParagraphStyle(
        "rd_certified",
        parent=_BASE["Normal"],
        fontName="Helvetica-Bold",
        fontSize=8.5,
        textColor=colors.HexColor("#8B0000"),
    ),
}

MARGIN     = 1.15 * inch
LINE_BREAK = 10   # pts


def _sp(pts: int = LINE_BREAK) -> Spacer:
    return Spacer(1, pts)


def _hr() -> HRFlowable:
    return HRFlowable(
        width="100%", thickness=0.4,
        color=colors.HexColor("#CCCCCC"),
        spaceBefore=4, spaceAfter=4,
    )


def _make_doc(path: str) -> SimpleDocTemplate:
    return SimpleDocTemplate(
        path, pagesize=LETTER_SIZE,
        leftMargin=MARGIN, rightMargin=MARGIN,
        topMargin=0.9 * inch, bottomMargin=0.9 * inch,
        title="Report Defence – Credit Dispute Letter",
        author="Report Defence",
    )


def _esc(text: str) -> str:
    """
    Escape for ReportLab XML and sanitize Unicode characters that
    Helvetica (Latin-1 / Type-1) cannot render — they appear as 'a^'
    or other garbage in the PDF output.
    """
    text = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    text = text.replace("—", "-")   # em dash
    text = text.replace("–", "-")   # en dash
    text = text.replace("‘", "'")   # left single quote
    text = text.replace("’", "'")   # right single quote
    text = text.replace("“", '"')  # left double quote
    text = text.replace("”", '"')  # right double quote
    text = text.replace("•", "-")   # bullet
    text = text.replace(" ", " ")   # non-breaking space
    text = text.replace("…", "...") # ellipsis
    return text


# ---------------------------------------------------------------------------
# Plain-text letter → PDF story
# ---------------------------------------------------------------------------

def text_to_story(letter_text: str, certified: bool = True) -> list:
    """
    Convert the plain-text output of build_dispute_letter_engine()
    or build_furnisher_letter_engine() into ReportLab flowables.

    Rules (mirrors the reference letter format exactly):
    - Certified mail tag at top
    - First block (no blank lines yet) = address header → smaller font
    - Blank line = paragraph break + small vertical space
    - "1. FURNISHER —" lines → bold
    - "Reason: ..." lines → normal (stay attached to their account)
    - Short ALL-CAPS lines → bold section headers
    - Everything else → normal paragraphs
    """
    story: list = []

    if certified:
        story.append(Paragraph(
            "VIA CERTIFIED MAIL - RETURN RECEIPT REQUESTED",
            STYLES["certified"],
        ))
        story.append(_sp(5))
        story.append(_hr())
        story.append(_sp(6))

    lines        = letter_text.split("\n")
    in_header    = True   # True until first blank line
    para_lines: list[str] = []

    def flush(buf: list[str], is_hdr: bool) -> list:
        if not buf:
            return []
        joined = "<br/>".join(_esc(l) for l in buf)
        style  = STYLES["small"] if is_hdr else STYLES["normal"]
        return [Paragraph(joined, style), _sp(LINE_BREAK)]

    for line in lines:
        stripped = line.strip()

        # ── First blank line ends the address header block ────────────────
        if in_header and stripped == "":
            story += flush(para_lines, is_hdr=True)
            para_lines = []
            in_header  = False
            story.append(_sp(LINE_BREAK))
            continue

        # ── Blank line = paragraph break ──────────────────────────────────
        if stripped == "":
            story += flush(para_lines, is_hdr=False)
            para_lines = []
            story.append(_sp(LINE_BREAK))
            continue

        # ── From here on: body content ────────────────────────────────────

        # Short ALL-CAPS section headers  (e.g. "LEGAL NOTICE", "DISPUTED ACCOUNT(S):")
        if (not in_header
                and stripped.isupper()
                and len(stripped) < 70
                and not re.match(r"^\d+\.", stripped)):
            story += flush(para_lines, is_hdr=False)
            para_lines = []
            story.append(Paragraph(_esc(stripped), STYLES["bold"]))
            story.append(_sp(6))
            continue

        # Account item: "1. FURNISHER — Account #: ..."
        if not in_header and re.match(r"^\d+\.\s", stripped):
            story += flush(para_lines, is_hdr=False)
            para_lines = []
            story.append(Paragraph(_esc(stripped), STYLES["bold"]))
            story.append(_sp(4))
            continue

        # Reason line: "Reason: ..."
        if not in_header and stripped.startswith("Reason:"):
            story += flush(para_lines, is_hdr=False)
            para_lines = []
            story.append(Paragraph(_esc(stripped), STYLES["normal"]))
            story.append(_sp(LINE_BREAK))
            continue

        # Bullet / numbered list items in furnisher letter
        if not in_header and re.match(r"^[1-9]\.\s|^\u2022\s|^•\s", stripped):
            story += flush(para_lines, is_hdr=False)
            para_lines = []
            story.append(Paragraph(_esc(stripped), STYLES["normal"]))
            story.append(_sp(4))
            continue

        # Default: accumulate into current paragraph
        para_lines.append(stripped)

    story += flush(para_lines, is_hdr=in_header)
    return story


def _clean_letter(text: str) -> str:
    """Remove duplicate 'DELETE OFF MY CREDIT REPORT.' the engine double-appends."""
    return re.sub(
        r"(DELETE OFF MY CREDIT REPORT\.)\s+DELETE OFF MY CREDIT REPORT\.",
        r"\1",
        text,
    )


def write_pdf(letter_text: str, path: str, certified: bool = True) -> str:
    doc   = _make_doc(path)
    story = text_to_story(_clean_letter(letter_text), certified=certified)
    doc.build(story)
    return path


def _safe(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")


# ---------------------------------------------------------------------------
# Render all letters to PDF
# ---------------------------------------------------------------------------

def generate_all_letters(
    consumer_name: str,
    letter_engine_output: dict,
    furnisher_engine_output: dict,
    client_slug: str = "client",
) -> list[str]:
    produced: list[str] = []

    for bureau, groups in letter_engine_output.items():
        for group_key, rounds in groups.items():
            for round_key, letter_text in rounds.items():
                if not letter_text.strip():
                    continue
                rn    = round_key.replace("round_", "")
                fname = f"{_safe(client_slug)}_{_safe(bureau)}_{_safe(group_key)}_round{rn}.pdf"
                path  = os.path.join(OUTPUT_DIR, fname)
                write_pdf(letter_text, path)
                produced.append(path)
                print(f"  ✓ {fname}  ({os.path.getsize(path)//1024} KB)")

    for furnisher_name, rounds in furnisher_engine_output.items():
        for round_key, letter_text in rounds.items():
            if not letter_text.strip():
                continue
            rn    = round_key.replace("round_", "")
            fname = f"{_safe(client_slug)}_furnisher_{_safe(furnisher_name)}_round{rn}.pdf"
            path  = os.path.join(OUTPUT_DIR, fname)
            write_pdf(letter_text, path)
            produced.append(path)
            print(f"  ✓ {fname}  ({os.path.getsize(path)//1024} KB)")

    return produced


# ---------------------------------------------------------------------------
# First-dispute promotion — ensures round_2-only items also appear in round_1
# ---------------------------------------------------------------------------

def promote_first_dispute(letter_input: dict) -> dict:
    """
    For first-time clients, any item assigned to round_2 that has NO
    corresponding round_1 item in the same bureau+group gets a round_1 copy.

    The original engine assigns certain attacks (e.g. same_account_number_same_balance)
    to round_2 assuming a prior round_1 already happened. For new clients that
    assumption is wrong — every account must be disputed starting in round_1.
    """
    import copy
    result = copy.deepcopy(letter_input)

    for bureau, groups in result.items():
        for group_key, items in groups.items():
            r1_accts = {
                it["account_number"]
                for it in items
                if it.get("recommended_round") == "round_1"
            }
            r2_only = [
                it for it in items
                if it.get("recommended_round") == "round_2"
                and it["account_number"] not in r1_accts
            ]
            for it in r2_only:
                r1_copy = dict(it)
                r1_copy["recommended_round"] = "round_1"
                items.append(r1_copy)

    return result


# ---------------------------------------------------------------------------
# IdentityIQ convenience function — full pipeline for any client
# ---------------------------------------------------------------------------

def build_identityiq_letters(
    pdf_path: str,
    consumer_name: str,
    client_slug: str,
) -> list[str]:
    """
    Full pipeline for an IdentityIQ PDF:
      1. Parse PDF with build_report()
      2. Promote round_2-only items to round_1 (first dispute fix)
      3. Rebuild letters with consumer_name
      4. Render all PDFs

    Returns list of PDF paths written.
    """
    import sys
    sys.path.insert(0, "/home/claude")
    sys.path.insert(0, "/mnt/user-data/outputs")

    from original_parser import (
        build_report,
        build_dispute_letter_engine,
        build_furnisher_letter_engine,
    )

    result      = build_report(pdf_path)
    report_date = result["report_date"]

    # round_1 is now guaranteed by the parser for all negative accounts.
    # promote_first_dispute() is no longer needed.
    letter_input = result["letter_input_engine"]

    dispute_letters = build_dispute_letter_engine(
        letter_input,
        consumer_name=consumer_name,
        report_date=report_date,
        personal_info=result["personal_info"],
        personal_info_issues=result["personal_info_issues"],
    )
    furnisher_letters = build_furnisher_letter_engine(
        letter_input,
        consumer_name=consumer_name,
        report_date=report_date,
    )

    print(f"\nClient      : {consumer_name}")
    print(f"Report date : {report_date}")
    neg = result["negatives_by_bureau"]
    total_neg = sum(len(v) for v in neg.values())
    print(f"Negatives   : {total_neg} across {len(neg)} bureaus")
    print(f"PI issues   : {len(result['personal_info_issues'])}")
    print("\nGenerating PDFs …")

    return generate_all_letters(
        consumer_name=consumer_name,
        letter_engine_output=dispute_letters,
        furnisher_engine_output=furnisher_letters,
        client_slug=client_slug,
    )




def _map_reason_code(reason_code: str) -> tuple[str, str]:
    return {
        "BK_INCLUDED_DEROGATORY":   ("bankruptcy", "bankruptcy_discharge_violation"),
        "BK_POST_DISCHARGE_LATES":  ("bankruptcy", "bankruptcy_discharge_violation"),
        "BK_BALANCE_NONZERO":       ("bankruptcy", "bankruptcy_discharge_violation"),
        "BK_COLLECTION_DISCHARGED": ("collection", "collector_original_creditor_pattern"),
        "BK_NOT_ON_ALL_BUREAUS":    ("bankruptcy", "bankruptcy_discharge_violation"),
        "CREDIT_LIMIT_MISSING":     ("derogatory", "cross_bureau_balance_conflict"),
        "BALANCE_INCONSISTENT":     ("derogatory", "cross_bureau_balance_conflict"),
        "STATUS_INCONSISTENT":      ("derogatory", "cross_bureau_account_status_conflict"),
        "INQUIRY_FRAUD_ALERT":      ("inquiry",    "inquiry_fraud_alert"),
    }.get(reason_code, ("derogatory", "requires_basic_verification"))


def _laws_for(reason_code: str) -> list[str]:
    laws = ["15 USC 1681i(a)", "15 USC 1681e(b)", "15 USC 1681s-2(a)(1)"]
    if "BK" in reason_code:
        laws += ["11 USC 524(a)(2)", "15 USC 1681c(a)(1)"]
    if "COLLECTION" in reason_code:
        laws += ["15 USC 1692g", "15 USC 1681s-2(b)"]
    return laws


def _extract_field(facts: list[str], pattern: str) -> str:
    for f in facts:
        m = re.search(pattern, f, re.IGNORECASE)
        if m:
            return m.group(1).strip(".,")
    return ""


def generate_cesar_letters() -> list[str]:
    from original_parser import (
        build_dispute_letter_engine,
        build_furnisher_letter_engine,
    )
    from report_parser import parse_cesar, Bureau
    from dispute_engine import DisputeEngine, extract_bk_context
    from collections import defaultdict

    profile = parse_cesar()
    ctx     = extract_bk_context(profile)
    items   = DisputeEngine().generate(profile)

    consumer_name = "Cesar Miranda Arcela"
    report_date   = profile.report_date

    # ── Build account number index from profile ───────────────────────────
    # Key: (section, bureau_value_lower) → account_number string
    acct_num_index: dict[tuple[str, str], str] = {}
    for tl in profile.tradelines:
        for b in [Bureau.EQUIFAX, Bureau.EXPERIAN, Bureau.TRANSUNION]:
            bd = tl.bureau_data(b)
            if bd and bd.reported and bd.account_number:
                acct_num_index[(tl.section, b.value.lower())] = bd.account_number

    # Also index collections by account_name + bureau for COL section items
    col_acct_index: dict[tuple[str, str], str] = {}
    for col in profile.collections:
        key = (col.agency_client.upper(), col.bureau.value.lower())
        col_acct_index[key] = col.account_number

    def _get_acct_num(it) -> str:
        """
        Look up account number from the profile directly.
        Falls back to extracting from facts if index misses.
        """
        bureau_key = it.bureau.value.lower()

        # Tradeline items: use section + bureau index
        if it.section not in ("COL", "INQ", "PR"):
            num = acct_num_index.get((it.section, bureau_key), "")
            if num:
                return num

        # Collection items: use agency name + bureau
        if it.section == "COL":
            num = col_acct_index.get((it.account_name.upper(), bureau_key), "")
            if num:
                return num

        # Fallback: scan facts for any pattern that looks like a masked account
        for f in it.facts:
            # Matches patterns like "xxxxxxxx3353", "xxxxxxxxxxxx6556", "xxxxxxxxxxxxx1002"
            m = re.search(r"(x{4,}\d{3,})", f, re.IGNORECASE)
            if m:
                return m.group(1)
            # "Account number: xxxxxxx4577"
            m = re.search(r"account\s*(?:number|#)[:\s]+(\S+)", f, re.IGNORECASE)
            if m:
                return m.group(1).strip(".,")

        return ""

    # ── Map DisputeItems → letter_input_engine format ─────────────────────
    letter_input: dict[str, dict[str, list[dict]]] = defaultdict(
        lambda: {"collections": [], "charge_offs": [], "late_payments": [], "other_derogatory": []}
    )
    seen: set[tuple] = set()

    # ── Priority order for deduplication — keep strongest reason per account ──
    REASON_PRIORITY = {
        "BK_INCLUDED_DEROGATORY":   1,
        "BK_COLLECTION_DISCHARGED": 1,
        "BK_POST_DISCHARGE_LATES":  2,
        "BK_BALANCE_NONZERO":       3,
        "BK_NOT_ON_ALL_BUREAUS":    4,
        "CREDIT_LIMIT_MISSING":     5,
        "BALANCE_INCONSISTENT":     5,
        "INQUIRY_FRAUD_ALERT":      6,
    }

    # First pass: collect best item per (bureau, section/name)
    best_item: dict[tuple, object] = {}
    for it in items:
        if it.outcome == "VERIFY" and it.reason_code == "STATUS_INCONSISTENT":
            continue
        if it.section in ("INQ", "PR"):
            continue
        bureau_key = it.bureau.value.lower()
        # Dedup key: same account = same section + bureau
        dedup_key  = (bureau_key, it.section if it.section != "COL" else it.account_name)
        priority   = REASON_PRIORITY.get(it.reason_code, 9)
        existing   = best_item.get(dedup_key)
        if existing is None:
            best_item[dedup_key] = (priority, it)
        elif priority < existing[0]:
            best_item[dedup_key] = (priority, it)

    for _priority, it in best_item.values():

        bureau_key = it.bureau.value.lower()
        key        = (bureau_key, it.account_name, it.section, it.reason_code)
        if key in seen:
            continue
        seen.add(key)

        neg_type, attack_type = _map_reason_code(it.reason_code)
        acct_num              = _get_acct_num(it)

        entry = {
            "furnisher_name":             it.account_name,
            "account_number":             acct_num,
            "masked_account_number":      acct_num,
            "negative_type":              neg_type,
            "attack_type":                attack_type,
            "laws":                       _laws_for(it.reason_code),
            "recommended_round":          "round_1",
            "recommended_methods":        ["bureau_dispute"],
            "reason":                     it.reason_text,
            "dofd_estimated":             None,
            "dofd_confidence":            "unknown",
            "fcra_expiration":            None,
            "days_until_expiration":      None,
            "is_obsolete":                False,
            "re_aging_flag":              False,
            "re_aging_gap_days":          None,
            "dofd_verification_required": False,
            "dla_suspected_refresh":      False,
            "date_of_last_payment":       "",
            "date_last_active":           "",
            "date_opened":                "",
            "last_reported":              "",
            "balance":                    _extract_field(it.facts, r"balance[^:$]*:\s*\$?([\d,]+\.?\d*)"),
            "past_due":                   _extract_field(it.facts, r"past\s*due[^:$]*:\s*\$?([\d,]+\.?\d*)"),
        }

        if neg_type in ("collection", "paid_collection"):
            letter_input[bureau_key]["collections"].append(entry)
        elif neg_type in ("charge_off", "charge_off_deficiency"):
            letter_input[bureau_key]["charge_offs"].append(entry)
        elif neg_type == "late_payment":
            letter_input[bureau_key]["late_payments"].append(entry)
        else:
            letter_input[bureau_key]["other_derogatory"].append(entry)

    # ── Run original engines ──────────────────────────────────────────────
    bureau_letters    = build_dispute_letter_engine(
        dict(letter_input),
        consumer_name=consumer_name,
        report_date=report_date,
    )
    furnisher_letters = build_furnisher_letter_engine(
        dict(letter_input),
        consumer_name=consumer_name,
        report_date=report_date,
    )

    # ── Print what we're generating ───────────────────────────────────────
    b_count = sum(len(r) for g in bureau_letters.values() for r in g.values())
    f_count = sum(len(r) for r in furnisher_letters.values())
    print(f"\nClient           : {consumer_name}")
    print(f"Bureau PDFs      : {b_count}")
    print(f"Furnisher PDFs   : {f_count}")
    print("\nGenerating PDFs …")

    return generate_all_letters(
        consumer_name=consumer_name,
        letter_engine_output=bureau_letters,
        furnisher_engine_output=furnisher_letters,
        client_slug="cesar_miranda",
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("=" * 60)
    print("REPORT DEFENCE – PDF Letter Generator")
    print("=" * 60)
    paths = generate_cesar_letters()
    print(f"\n{len(paths)} PDF(s) → {OUTPUT_DIR}/")
    for p in paths:
        print(f"  {os.path.basename(p)}  ({os.path.getsize(p)//1024} KB)")
