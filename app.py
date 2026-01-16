import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import random
import threading
import io
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse
import re

st.set_page_config(page_title="TataCliq Crawler", layout="wide")

# -----------------------------
# Thread-safe stop signal
# -----------------------------
stop_event = threading.Event()

def request_stop():
    stop_event.set()

# -----------------------------
# Config / constants
# -----------------------------
DEFAULT_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"

BASE_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "en-US,en;q=0.9",
    "content-type": "application/json",
    "user-agent": DEFAULT_UA,
    "sec-ch-ua": '"Not.A/Brand";v="8", "Chromium";v="125", "Google Chrome";v="125"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "origin": "https://www.tatacliq.com",
    "referer": "https://www.tatacliq.com/",
}

# -----------------------------
# Headers helper
# -----------------------------
def make_headers(cookie_text: str | None):
    headers = dict(BASE_HEADERS)
    if cookie_text and cookie_text.strip():
        headers["cookie"] = cookie_text.strip()
    return headers

# -----------------------------
# URL Validation / Normalization
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
# Networking helpers
# -----------------------------
def safe_get_text(url, headers, retry_count=3, timeout=25):
    last_err = None
    for attempt in range(1, retry_count + 1):
        if stop_event.is_set():
            raise RuntimeError("Stopped by user")

        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            if r.status_code == 200:
                return r.text
            last_err = f"HTTP {r.status_code}"
        except Exception as e:
            last_err = str(e)

        time.sleep(0.6 * attempt + random.random())

    raise RuntimeError(f"Failed after {retry_count} attempts. Last error: {last_err}")

def safe_post_json(url, headers, payload, retry_count=3, timeout=25):
    last_err = None
    for attempt in range(1, retry_count + 1):
        if stop_event.is_set():
            raise RuntimeError("Stopped by user")

        try:
            r = requests.post(url, headers=headers, json=payload, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            last_err = f"HTTP {r.status_code}"
        except Exception as e:
            last_err = str(e)

        time.sleep(0.6 * attempt + random.random())

    raise RuntimeError(f"Failed after {retry_count} attempts. Last error: {last_err}")

# -----------------------------
# TataCliq specific extraction
# -----------------------------
def extract_tatacliq_code(url: str):
    """
    Extract TataCliq SKU code from URL.
    Example: p-MP000000029530017 -> MP000000029530017
    """
    url = str(url).strip()

    m = re.search(r"p-(MP\d+)", url, re.IGNORECASE)
    if m:
        return m.group(1).upper()

    m = re.search(r"(MP\d+)", url, re.IGNORECASE)
    if m:
        return m.group(1).upper()

    return None

def flatten_list(x):
    if x is None:
        return ""
    if isinstance(x, list):
        return ", ".join([str(i) for i in x])
    return str(x)

# -----------------------------
# TataCliq API - SKU based (works for p-MP URLs)
# -----------------------------
def get_details_by_sku(mp_code, headers, retry_count):
    url = "https://www.tatacliq.com/marketplacewebservices/v2/mpl/products/productDetailsBySKUs"
    payload = {"skuIds": [mp_code]}
    return safe_post_json(url, headers=headers, payload=payload, retry_count=retry_count)

# -----------------------------
# Optional Size Guide extraction (HTML-based)
# -----------------------------
def get_size_guide(product_url, headers, retry_count):
    html = safe_get_text(product_url, headers=headers, retry_count=retry_count)
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True).lower()
    if "size guide" not in text:
        return None
    return "Size guide available on page"

# -----------------------------
# Crawl single URL
# -----------------------------
def crawl_single(url, headers, retry_count):
    if stop_event.is_set():
        return {"URL": url, "Error": "Stopped by user"}

    mp_code = extract_tatacliq_code(url)
    if not mp_code:
        return {"URL": url, "Error": "Could not extract MP code (SKU) from URL"}

    try:
        details = get_details_by_sku(mp_code, headers, retry_count)
    except Exception as e:
        return {"URL": url, "SKU Code": mp_code, "Error": str(e)}

    out = {"URL": url, "SKU Code": mp_code}

    # Extract product block
    product = None
    try:
        if isinstance(details, dict):
            if "products" in details and isinstance(details["products"], list) and len(details["products"]) > 0:
                product = details["products"][0]
            elif "productDetails" in details and isinstance(details["productDetails"], list) and len(details["productDetails"]) > 0:
                product = details["productDetails"][0]
    except Exception:
        product = None

    if product:
        out["Brand"] = product.get("brand", "")
        out["Product Name"] = product.get("productName", "") or product.get("name", "")
        out["MRP"] = product.get("mrp", "")
        out["Selling Price"] = product.get("offerPrice", "") or product.get("sellingPrice", "")
        out["Category"] = flatten_list(product.get("category", ""))
        out["Sub Category"] = flatten_list(product.get("subCategory", ""))
        out["In Stock"] = product.get("inStock", "")

    # HTML size guide check
    try:
        out["Size Guide"] = get_size_guide(url, headers, retry_count)
    except Exception:
        out["Size Guide"] = None

    return out

# -----------------------------
# Crawl runner (multithread)
# -----------------------------
def run_crawl(urls, headers, retry_count, max_workers, sleep_min, sleep_max):
    stop_event.clear()

    results = []
    errors = 0
    total = len(urls)
    completed = 0
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
# Streamlit UI
# -----------------------------
st.title("TataCliq Product Crawler")
st.caption("Upload URLs (Excel/CSV) → validate/dedupe → multithread crawl → download extracted data")

with st.expander("Upload & settings", expanded=True):
    file = st.file_uploader("Upload Excel or CSV", type=["xlsx", "xls", "csv"])
    cookie_text = st.text_area(
        "Optional: Cookie header (if TataCliq blocks requests). Leave empty for public use.",
        height=110
    )

    c1, c2, c3 = st.columns(3)
    with c1:
        retry_count = st.number_input("Retry count (per request)", min_value=1, max_value=10, value=3, step=1)
    with c2:
        max_workers = st.number_input("Threads (parallel URLs)", min_value=1, max_value=20, value=6, step=1)
    with c3:
        validate_only = st.checkbox("Only validate + dedupe (no crawl)", value=False)

    c4, c5 = st.columns(2)
    with c4:
        sleep_min = st.number_input("Min jitter per URL (sec)", min_value=0.0, max_value=10.0, value=0.1, step=0.1)
    with c5:
        sleep_max = st.number_input("Max jitter per URL (sec)", min_value=0.0, max_value=20.0, value=0.6, step=0.1)

    st.button("Stop Crawl", on_click=request_stop, type="secondary")

if not file:
    st.info("Upload a file to begin. Your sheet must contain a column with product URLs.")
    st.stop()

# Load file
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

# URL column selection
cols = list(df.columns)
default_col = None
for c in cols:
    if str(c).strip().lower() in ["url", "product url", "product_url", "link", "pdp", "pdp url"]:
        default_col = c
        break

url_col = st.selectbox(
    "Select the column containing product URLs",
    cols,
    index=cols.index(default_col) if default_col in cols else 0
)

# Validate + dedupe
raw_urls = df[url_col].fillna("").astype(str).tolist()
normalized = [normalize_url(u) for u in raw_urls if str(u).strip()]

valid_urls = []
invalid_urls = []
seen = set()

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
    with st.expander("View invalid URLs"):
        st.write(pd.DataFrame({"Invalid URL": invalid_urls}))

if not valid_urls:
    st.error("No valid TataCliq URLs found after validation. Please check your file.")
    st.stop()

if validate_only:
    st.success("Validation + dedupe completed (crawl not started).")
    st.dataframe(pd.DataFrame({"Valid URLs": valid_urls}), use_container_width=True)
    st.stop()

# Start crawl
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

    # Download Excel
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        out_df.to_excel(writer, index=False, sheet_name="Extracted")

    st.download_button(
        label="Download Extracted Data (Excel)",
        data=buffer.getvalue(),
        file_name="tatacliq_extracted.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    # Download CSV
    st.download_button(
        label="Download Extracted Data (CSV)",
        data=out_df.to_csv(index=False).encode("utf-8"),
        file_name="tatacliq_extracted.csv",
        mime="text/csv",
    )
