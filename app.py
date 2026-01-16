import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import random
import threading
import io
import re
import json
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

st.set_page_config(page_title="TataCliq Crawler", layout="wide")

# -----------------------------
# Thread-safe stop signal
# -----------------------------
stop_event = threading.Event()

def request_stop():
    stop_event.set()

# -----------------------------
# Config
# -----------------------------
DEFAULT_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"

BASE_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "en-US,en;q=0.9",
    "content-type": "application/json",
    "user-agent": DEFAULT_UA,
    "origin": "https://www.tatacliq.com",
    "referer": "https://www.tatacliq.com/",
}

def make_headers(cookie_text: str | None):
    headers = dict(BASE_HEADERS)
    if cookie_text and cookie_text.strip():
        headers["cookie"] = cookie_text.strip()
    return headers

# -----------------------------
# URL Validation
# -----------------------------
def normalize_url(u: str) -> str:
    u = str(u).strip()
    if not u:
        return ""
    if u.startswith("www."):
        u = "https://" + u
    return u

def is_valid_tatacliq_url(u: str) -> bool:
    try:
        p = urlparse(u)
        if p.scheme not in ("http", "https"):
            return False
        if "tatacliq.com" not in (p.netloc or ""):
            return False
        return True
    except Exception:
        return False

# -----------------------------
# Network helpers
# -----------------------------
def safe_get(url, headers, retry_count=3, timeout=25):
    last_err = None
    for attempt in range(1, retry_count + 1):
        if stop_event.is_set():
            raise RuntimeError("Stopped by user")
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            return r
        except Exception as e:
            last_err = str(e)
        time.sleep(0.6 * attempt + random.random())
    raise RuntimeError(f"GET failed after {retry_count} attempts: {last_err}")

# -----------------------------
# Extraction helpers
# -----------------------------
def extract_mp_code(url: str):
    """
    Extract TataCliq SKU from URL.
    Example: .../p-MP000000029530017 -> MP000000029530017
    """
    m = re.search(r"p-(MP\d+)", str(url), re.IGNORECASE)
    if m:
        return m.group(1).upper()
    m = re.search(r"(MP\d+)", str(url), re.IGNORECASE)
    if m:
        return m.group(1).upper()
    return None

def safe_json_dumps(obj):
    try:
        return json.dumps(obj, ensure_ascii=False)
    except Exception:
        try:
            return json.dumps(str(obj), ensure_ascii=False)
        except Exception:
            return ""

def extract_basic_meta(soup: BeautifulSoup):
    out = {}

    # Title
    if soup.title:
        out["Meta_Title"] = soup.title.get_text(strip=True)

    # OG tags
    def meta(prop):
        t = soup.find("meta", {"property": prop})
        return t.get("content") if t and t.get("content") else ""

    out["Meta_OG_Title"] = meta("og:title")
    out["Meta_OG_Description"] = meta("og:description")
    out["Meta_OG_Image"] = meta("og:image")

    # canonical
    canon = soup.find("link", {"rel": "canonical"})
    if canon and canon.get("href"):
        out["Canonical"] = canon["href"]

    return out

# -----------------------------
# Full PDP JSON extraction (main requirement)
# -----------------------------
def extract_pdp_json_from_html(url, headers, retry_count):
    """
    Extract embedded PDP JSON from:
    1) __NEXT_DATA__
    2) window.__PRELOADED_STATE__ / __APOLLO_STATE__
    3) JSON-LD (fallback)
    """
    r = safe_get(url, headers=headers, retry_count=retry_count)
    if r.status_code != 200:
        return {"Error": f"HTML fetch failed HTTP {r.status_code}"}

    html = r.text
    soup = BeautifulSoup(html, "html.parser")

    out = extract_basic_meta(soup)

    # 1) __NEXT_DATA__
    next_data = soup.find("script", {"id": "__NEXT_DATA__"})
    if next_data and next_data.string:
        try:
            jd = json.loads(next_data.string)
            out["PDP_Source"] = "__NEXT_DATA__"
            out["PDP_JSON"] = safe_json_dumps(jd)
            return out
        except Exception:
            pass

    # 2) Preloaded state patterns in scripts
    for s in soup.find_all("script"):
        if not s.string:
            continue
        txt = s.string.strip()

        if "__PRELOADED_STATE__" in txt or "__APOLLO_STATE__" in txt:
            # attempt JSON object extraction
            m = re.search(r"=\s*({.*})\s*;?\s*$", txt, re.DOTALL)
            if m:
                try:
                    jd = json.loads(m.group(1))
                    out["PDP_Source"] = "PRELOADED_STATE/APOLLO"
                    out["PDP_JSON"] = safe_json_dumps(jd)
                    return out
                except Exception:
                    pass

    # 3) JSON-LD fallback
    scripts = soup.find_all("script", {"type": "application/ld+json"})
    for s in scripts:
        if not s.string:
            continue
        try:
            jd = json.loads(s.string)
            if isinstance(jd, dict) or isinstance(jd, list):
                out["PDP_Source"] = "JSON-LD"
                out["PDP_JSON"] = safe_json_dumps(jd)
                return out
        except Exception:
            continue

    out["PDP_Source"] = "NOT_FOUND"
    out["PDP_JSON"] = ""
    out["Error"] = "No embedded PDP JSON found in HTML"
    return out

# -----------------------------
# Crawl one URL
# -----------------------------
def crawl_single(url, headers, retry_count):
    if stop_event.is_set():
        return {"URL": url, "Error": "Stopped by user"}

    out = {"URL": url}
    sku = extract_mp_code(url)
    out["SKU"] = sku or ""

    try:
        pdp = extract_pdp_json_from_html(url, headers, retry_count)
        out.update(pdp)

        # If we got PDP_JSON, mark success
        if out.get("PDP_JSON"):
            out["Error"] = ""
        else:
            out["Error"] = out.get("Error", "No PDP JSON")
    except Exception as e:
        out["Error"] = str(e)

    return out

# -----------------------------
# Crawl runner (multi-thread)
# -----------------------------
def run_crawl(urls, headers, retry_count, max_workers, sleep_min, sleep_max):
    stop_event.clear()

    results = []
    total = len(urls)
    completed = 0
    errors = 0

    lock = threading.Lock()
    progress = st.progress(0.0)
    status = st.empty()

    def task(u):
        if stop_event.is_set():
            return {"URL": u, "Error": "Stopped by user"}
        time.sleep(random.uniform(sleep_min, sleep_max))
        return crawl_single(u, headers, retry_count)

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(task, u): u for u in urls}

        for fut in as_completed(futures):
            u = futures[fut]
            if stop_event.is_set():
                break

            try:
                res = fut.result()
            except Exception as e:
                res = {"URL": u, "Error": str(e)}

            with lock:
                results.append(res)
                completed += 1
                if res.get("Error"):
                    errors += 1

                progress.progress(min(completed / total, 1.0))
                status.write(f"Completed {completed}/{total} | Errors: {errors}")

    if stop_event.is_set():
        status.warning(f"Stopped by user. Completed {len(results)}/{total}.")
    else:
        status.success(f"Done. Total: {total} | Errors: {errors}")

    return pd.DataFrame(results)

# -----------------------------
# ZIP builder for PDP JSON files
# -----------------------------
def build_pdp_zip(df: pd.DataFrame):
    """
    Creates a zip in-memory:
    - output_summary.csv
    - pdp_json/<sku_or_row>.json (only when PDP_JSON exists)
    """
    mem_zip = io.BytesIO()
    with zipfile.ZipFile(mem_zip, "w", zipfile.ZIP_DEFLATED) as z:
        # summary CSV
        z.writestr("output_summary.csv", df.to_csv(index=False))

        # json files
        for i, row in df.iterrows():
            pdp_json = row.get("PDP_JSON", "")
            if not isinstance(pdp_json, str) or not pdp_json.strip():
                continue

            sku = row.get("SKU", "")
            name = sku if sku else f"row_{i+1}"
            name = re.sub(r"[^A-Za-z0-9_\-]", "_", str(name))

            z.writestr(f"pdp_json/{name}.json", pdp_json)

    mem_zip.seek(0)
    return mem_zip.getvalue()

# -----------------------------
# UI
# -----------------------------
st.title("TataCliq PDP Crawler")
st.caption("Uploads → validates → downloads FULL PDP embedded JSON")

with st.expander("Upload & settings", expanded=True):
    file = st.file_uploader("Upload Excel or CSV", type=["xlsx", "xls", "csv"])
    cookie_text = st.text_area("Optional Cookie header (if TataCliq blocks)", height=100)

    c1, c2, c3 = st.columns(3)
    with c1:
        retry_count = st.number_input("Retry count", min_value=1, max_value=10, value=3, step=1)
    with c2:
        max_workers = st.number_input("Threads", min_value=1, max_value=20, value=6, step=1)
    with c3:
        validate_only = st.checkbox("Only validate + dedupe", value=False)

    c4, c5 = st.columns(2)
    with c4:
        sleep_min = st.number_input("Min jitter (sec)", min_value=0.0, max_value=10.0, value=0.1, step=0.1)
    with c5:
        sleep_max = st.number_input("Max jitter (sec)", min_value=0.0, max_value=20.0, value=0.6, step=0.1)

    st.button("Stop Crawl", on_click=request_stop)

if not file:
    st.info("Upload a file to begin.")
    st.stop()

# load
try:
    if file.name.lower().endswith(".csv"):
        df = pd.read_csv(file)
    else:
        df = pd.read_excel(file)
except Exception as e:
    st.error(f"Could not read file: {e}")
    st.stop()

st.subheader("Preview")
st.dataframe(df.head(20), use_container_width=True)

cols = list(df.columns)
url_col = st.selectbox("Select URL column", cols)

raw_urls = df[url_col].fillna("").astype(str).tolist()
normalized = [normalize_url(u) for u in raw_urls if str(u).strip()]

valid_urls, invalid_urls, seen = [], [], set()

for u in normalized:
    if not is_valid_tatacliq_url(u):
        invalid_urls.append(u)
        continue
    if u in seen:
        continue
    seen.add(u)
    valid_urls.append(u)

st.markdown("### URL Validation Summary")
a, b, c = st.columns(3)
a.metric("Input rows", len(raw_urls))
b.metric("Valid URLs", len(valid_urls))
c.metric("Invalid/Skipped", len(invalid_urls))

if invalid_urls:
    with st.expander("Invalid URLs"):
        st.write(pd.DataFrame({"Invalid URL": invalid_urls}))

if not valid_urls:
    st.error("No valid TataCliq URLs found.")
    st.stop()

if validate_only:
    st.success("Validation + dedupe completed (crawl not started).")
    st.dataframe(pd.DataFrame({"Valid URLs": valid_urls}), use_container_width=True)
    st.stop()

if st.button("Start Crawl", type="primary"):
    headers = make_headers(cookie_text)

    out_df = run_crawl(
        urls=valid_urls,
        headers=headers,
        retry_count=int(retry_count),
        max_workers=int(max_workers),
        sleep_min=float(sleep_min),
        sleep_max=float(sleep_max),
    )

    st.subheader("Extracted output")
    st.dataframe(out_df, use_container_width=True)

    # Excel summary
    excel_buffer = io.BytesIO()
    with pd.ExcelWriter(excel_buffer, engine="xlsxwriter") as writer:
        out_df.drop(columns=["PDP_JSON"], errors="ignore").to_excel(writer, index=False, sheet_name="Summary")
    excel_buffer.seek(0)

    st.download_button(
        label="Download Summary Excel (without PDP_JSON column)",
        data=excel_buffer.getvalue(),
        file_name="tatacliq_summary.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    # CSV full
    st.download_button(
        label="Download Full CSV (includes PDP_JSON)",
        data=out_df.to_csv(index=False).encode("utf-8"),
        file_name="tatacliq_output.csv",
        mime="text/csv",
    )

    # ZIP with individual JSONs
    zip_bytes = build_pdp_zip(out_df)
    st.download_button(
        label="Download PDP JSON ZIP (Recommended)",
        data=zip_bytes,
        file_name="tatacliq_pdp_json.zip",
        mime="application/zip",
    )
