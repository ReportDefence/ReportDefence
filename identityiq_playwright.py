"""
identityiq_playwright.py
========================
Uses Playwright (headless Chromium) to authenticate to IdentityIQ
and fetch the JSON credit report.

Playwright runs a real browser — passes Imperva/Incapsula WAF that
blocks server-side HTTP requests from datacenter IPs.

Install: pip install playwright && playwright install chromium
"""

from __future__ import annotations
import json
import re
from typing import Any


def login_and_fetch_json(username: str, password: str, ssn_last4: str) -> dict:
    """
    Uses Playwright headless Chromium to:
    1. Navigate to IdentityIQ login page
    2. Fill in credentials and submit
    3. Handle SSN verification step if present
    4. Fetch the JSON report
    5. Return parsed dict
    
    Raises ValueError on auth failure or parse error.
    """
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

    with sync_playwright() as p:
        import shutil, os

        # Search for system Chromium in common locations
        chromium_candidates = [
            shutil.which("chromium"),
            shutil.which("chromium-browser"),
            shutil.which("google-chrome"),
            shutil.which("google-chrome-stable"),
            "/usr/bin/chromium",
            "/usr/bin/chromium-browser",
            "/usr/bin/google-chrome",
        ]
        system_chromium = next(
            (p for p in chromium_candidates if p and os.path.exists(p)),
            None
        )

        launch_kwargs = {
            "headless": True,
            "args": [
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--single-process",
                "--disable-extensions",
            ]
        }
        if system_chromium:
            launch_kwargs["executable_path"] = system_chromium
            print(f"[PW] Using system Chromium: {system_chromium}")
        else:
            print(f"[PW] No system Chromium found, trying Playwright default")

        browser = p.chromium.launch(**launch_kwargs)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800},
        )
        page = context.new_page()

        try:
            # ── Step 1: Navigate to login page ───────────────────────────
            print("[PW] Navigating to IdentityIQ login...")
            page.goto("https://member.identityiq.com/", wait_until="networkidle", timeout=30000)
            print(f"[PW] Login page loaded: {page.url}")

            # ── Step 2: Fill credentials ──────────────────────────────────
            print("[PW] Filling credentials...")
            
            # Username field
            page.wait_for_selector("input[type='text'], input[name*='user'], input[placeholder*='Username']", timeout=10000)
            username_sel = (
                "input[placeholder='Enter Username']",
                "input[name='username']",
                "input[type='text']",
            )
            for sel in username_sel:
                try:
                    page.fill(sel, username, timeout=3000)
                    print(f"[PW] Filled username with selector: {sel}")
                    break
                except Exception:
                    continue

            # Password field
            password_sel = (
                "input[placeholder='Enter Password']",
                "input[name='password']",
                "input[type='password']",
            )
            for sel in password_sel:
                try:
                    page.fill(sel, password, timeout=3000)
                    print(f"[PW] Filled password with selector: {sel}")
                    break
                except Exception:
                    continue

            # ── Step 3: Click Login ───────────────────────────────────────
            print("[PW] Clicking login button...")
            login_btn_sel = (
                "button:has-text('Login')",
                "button[type='submit']",
                "input[type='submit']",
                "button:has-text('Sign In')",
            )
            for sel in login_btn_sel:
                try:
                    page.click(sel, timeout=3000)
                    print(f"[PW] Clicked login with selector: {sel}")
                    break
                except Exception:
                    continue

            # Wait for navigation after login
            page.wait_for_load_state("networkidle", timeout=20000)
            print(f"[PW] After login URL: {page.url}")

            # ── Step 4: SSN verification (if required) ────────────────────
            current_url = page.url.lower()
            page_text   = page.content().lower()

            ssn_required = (
                "ssn" in page_text or
                "social security" in page_text or
                "last 4" in page_text or
                "verify" in current_url
            )

            if ssn_required and ssn_last4:
                print("[PW] SSN verification required, filling SSN...")
                ssn_sel = (
                    "input[placeholder*='SSN']",
                    "input[placeholder*='last 4']",
                    "input[name*='ssn']",
                    "input[maxlength='4']",
                    "input[type='text']",
                    "input[type='number']",
                )
                for sel in ssn_sel:
                    try:
                        page.fill(sel, ssn_last4, timeout=3000)
                        print(f"[PW] Filled SSN with selector: {sel}")
                        break
                    except Exception:
                        continue

                # Click verify/continue button
                verify_sel = (
                    "button:has-text('Verify')",
                    "button:has-text('Continue')",
                    "button:has-text('Submit')",
                    "button[type='submit']",
                )
                for sel in verify_sel:
                    try:
                        page.click(sel, timeout=3000)
                        print(f"[PW] Clicked verify with selector: {sel}")
                        break
                    except Exception:
                        continue

                page.wait_for_load_state("networkidle", timeout=20000)
                print(f"[PW] After SSN URL: {page.url}")

            # ── Step 5: Verify we're logged in ────────────────────────────
            final_url = page.url.lower()
            if "login" in final_url and "dashboard" not in final_url:
                page_content = page.content().lower()
                if "invalid" in page_content or "incorrect" in page_content:
                    raise ValueError("Invalid username or password")
                raise ValueError(
                    f"Login failed — still on login page after credentials. "
                    f"URL: {page.url}"
                )
            print(f"[PW] Successfully logged in. URL: {page.url}")

            # ── Step 6: Fetch JSON report ─────────────────────────────────
            print("[PW] Fetching JSON report...")
            
            # Use page.evaluate to fetch the JSON with session cookies
            json_text = page.evaluate("""
                async () => {
                    const resp = await fetch(
                        '/CreditReport.aspx?view=json',
                        {
                            credentials: 'include',
                            headers: { 'Accept': 'application/json, text/javascript, */*' }
                        }
                    );
                    return await resp.text();
                }
            """)

            print(f"[PW] JSON response length: {len(json_text)}")
            print(f"[PW] JSON first 100 chars: {json_text[:100]}")

            if not json_text or len(json_text) < 10:
                raise ValueError(
                    "Empty JSON response from IdentityIQ. "
                    "The account may not have a credit report available."
                )

            # Strip JSONP wrapper
            raw = json_text.strip()
            if raw.startswith("JSON_CALLBACK("):
                raw = raw[len("JSON_CALLBACK("):]
                if raw.endswith(")"):
                    raw = raw[:-1]

            data = json.loads(raw)
            print("[PW] JSON parsed successfully")
            return data

        except ValueError:
            raise
        except PWTimeout as e:
            raise ValueError(f"IdentityIQ page timed out: {e}")
        except Exception as e:
            import traceback
            print(f"[PW] Unexpected error: {traceback.format_exc()}")
            raise ValueError(f"IdentityIQ connection error: {e}")
        finally:
            browser.close()


def pull_and_parse(username: str, password: str, ssn_last4: str) -> dict:
    """
    Full pipeline using Playwright:
    1. Login via headless browser
    2. Fetch JSON report
    3. Parse into Report Defence format
    """
    from identityiq_connector import parse_identityiq_json
    
    data   = login_and_fetch_json(username, password, ssn_last4)
    result = parse_identityiq_json(data)
    return result
