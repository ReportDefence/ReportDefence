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
                "--no-zygote",
                # NOTE: --single-process removed — causes crashes in Docker
                "--disable-extensions",
                "--disable-background-networking",
                "--disable-default-apps",
                "--disable-sync",
                "--disable-translate",
                "--hide-scrollbars",
                "--metrics-recording-only",
                "--mute-audio",
                "--no-first-run",
                "--safebrowsing-disable-auto-update",
                "--ignore-certificate-errors",
                "--ignore-ssl-errors",
                "--ignore-certificate-errors-spki-list",
            ]
        }
        if system_chromium:
            launch_kwargs["executable_path"] = system_chromium
            print(f"[PW] Using system Chromium: {system_chromium}")
        else:
            print(f"[PW] No system Chromium found, trying Playwright default")

        print(f"[PW] Launching browser...")
        browser = p.chromium.launch(**launch_kwargs)
        print(f"[PW] Browser launched successfully")
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
            try:
                page.goto("https://member.identityiq.com/", wait_until="domcontentloaded", timeout=20000)
                print(f"[PW] Login page loaded: {page.url}")
            except Exception as e:
                print(f"[PW] goto failed: {e}")
                # Try direct login URL
                try:
                    page.goto("https://member.identityiq.com/login", wait_until="domcontentloaded", timeout=20000)
                    print(f"[PW] Login page (alt) loaded: {page.url}")
                except Exception as e2:
                    print(f"[PW] Both goto attempts failed: {e2}")
                    raise ValueError(f"Cannot reach IdentityIQ: {e2}")

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
            for sel in ("button:has-text('Login')", "button[type='submit']", "input[type='submit']"):
                try:
                    page.click(sel, timeout=3000)
                    print(f"[PW] Clicked login with selector: {sel}")
                    break
                except Exception:
                    continue

            # Wait for URL to change after login click
            print("[PW] Waiting for post-login redirect...")
            try:
                page.wait_for_url(
                    lambda url: url != "https://member.identityiq.com/" and "member.identityiq.com" in url,
                    timeout=15000
                )
            except Exception:
                page.wait_for_timeout(3000)
            print(f"[PW] After login URL: {page.url}")

            # ── Step 4: Handle verification steps in a loop ───────────────
            for attempt in range(4):
                cur = page.url.lower()
                print(f"[PW] Verification loop {attempt+1}: {page.url}")

                if "security-question" in cur or "verify" in cur:
                    print("[PW] Verification page — waiting for React to render input...")
                    # SPA renders the form via JS — wait longer and poll
                    input_found = False
                    for wait_attempt in range(10):
                        page.wait_for_timeout(1000)
                        # Try to find any input in the page
                        inputs = page.query_selector_all("input")
                        print(f"[PW] Wait {wait_attempt+1}/10 — inputs found: {len(inputs)}")
                        if inputs:
                            input_found = True
                            break
                    
                    if not input_found:
                        print(f"[PW] No input found after 10s. Page: {page.content()[:500]}")
                        break

                    # Fill SSN into the first available input
                    filled = False
                    for sel in (
                        "input[placeholder*='SSN']",
                        "input[placeholder*='last four']",
                        "input[placeholder*='Last four']",
                        "input[placeholder*='last 4']",
                        "input[placeholder*='Last 4']",
                        "input[name*='ssn']",
                        "input[name*='answer']",
                        "input[maxlength='4']",
                        "input[type='text']",
                        "input[type='number']",
                        "input[type='password']",
                        "input",
                    ):
                        try:
                            el = page.query_selector(sel)
                            if el:
                                el.scroll_into_view_if_needed()
                                el.click()
                                el.fill(ssn_last4)
                                print(f"[PW] Filled SSN with: {sel} value={ssn_last4}")
                                filled = True
                                break
                        except Exception as e:
                            print(f"[PW] Selector {sel} failed: {e}")
                            continue

                    if not filled:
                        print(f"[PW] WARNING: no input filled.")

                    # Click submit button
                    for sel in (
                        "button:has-text('Submit')",
                        "button:has-text('Verify')",
                        "button:has-text('Continue')",
                        "button[type='submit']",
                        "input[type='submit']",
                    ):
                        try:
                            el = page.query_selector(sel)
                            if el and el.is_visible():
                                el.click()
                                print(f"[PW] Clicked submit: {sel}")
                                break
                        except Exception:
                            continue

                    # Wait for redirect away from verification page
                    try:
                        page.wait_for_url(
                            lambda url: "security-question" not in url.lower() and "verify" not in url.lower(),
                            timeout=10000
                        )
                    except Exception:
                        page.wait_for_timeout(3000)
                    print(f"[PW] After verification: {page.url}")

                else:
                    # Not on a verification page — done
                    print(f"[PW] No more verification steps needed")
                    break

            # ── Step 5: Verify authenticated ──────────────────────────────
            final_url = page.url.lower()
            print(f"[PW] Final URL: {page.url}")
            if "login" in final_url and "security-question" not in final_url and "dashboard" not in final_url:
                raise ValueError("Login failed — invalid credentials")
            print(f"[PW] Successfully authenticated. URL: {page.url}")

            # ── Step 6: Fetch JSON via page.evaluate (proven working) ────
            print("[PW] Fetching JSON report via page.evaluate...")
            try:
                json_text = page.evaluate("""
                    async () => {
                        try {
                            const resp = await fetch(
                                '/CreditReport.aspx?view=json',
                                {
                                    credentials: 'include',
                                    headers: { 'Accept': 'application/json, text/javascript, */*' }
                                }
                            );
                            const text = await resp.text();
                            return text || 'EMPTY_RESPONSE';
                        } catch(e) {
                            return 'FETCH_ERROR: ' + e.toString();
                        }
                    }
                """, timeout=60000)
                print(f"[PW] JSON response length: {len(json_text) if json_text else 0}")
                print(f"[PW] JSON first 150 chars: {json_text[:150] if json_text else 'EMPTY'}")
            except Exception as e:
                print(f"[PW] page.evaluate failed: {e}")
                json_text = ""

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
