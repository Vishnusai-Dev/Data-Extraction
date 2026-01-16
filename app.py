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
import json

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

def safe_post(url, headers, payload, retry_count=3, timeout=25):
    last_err = None
    for attempt in range(1, retry_count + 1):
        if stop_event.is_set():
            raise RuntimeError("Stopped by user")
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=timeout)
            return r
        except Exception as e:
            last_err = str(e)
        time.sleep(0.6 * attempt + random.random())
    raise RuntimeError(f"POST failed after {retry_count} attempts: {last_err}")

# -----------------------------
# Extraction helpers
# -----------------------------
def extract_mp_code(url: str):
    m = re.search(r"p-(MP\d+)", str(url), re.IGNORECASE)
    if m:
        return m.group(1).upper()
    m = re.search(r"(MP\d+)", str(url), re.IGNORECASE)
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
# API attempts
# -----------------------------
def try_sku_api_variants(mp_code, headers, retry_count):
    """
    Try multiple TataCliq API endpoint/payload styles.
    Return product dict if found, else None.
    """
    endpoints = [
        ("https://www.tatacliq.com/marketplacewebservices/v2/mpl/products/productDetailsBySKUs", {"skuIds": [mp_code]}),
        ("https://www.tatacliq.com/marketplacewebservices/v2/mpl/products/productDetailsBySKUs", {"skuId": mp_code}),
        ("https://www.tatacliq.com/marketplacewebservices/v2/mpl/products/productDetailsBySku", {"skuId": mp_code}),
    ]

    for ep, payload in endpoints:
        r = safe_post(ep, headers=headers, payload=payload, retry_count=retry_count)
        if r.status_code != 200:
            continue

        try:
            data = r.json()
        except Exception:
            continue

        # common structures
        if isinstance(data, dict):
            if "products" in data and isinstance(data["products"], list) and data["products"]:
                return data["products"][0]
            if "productDetails" in data and isinstance(data["productDetails"], list) and data["productDetails"]:
                return data["productDetails"][0]
            if "product" in data and isinstance(data["product"], dict):
                return data["product"]

    return None

# -----------------------------
# HTML fallback extraction
# -----------------------------
def extract_from_html(url, headers, retry_count):
    """
    If API fails, scrape minimal info from HTML.
    This avoids total failure due to API 404/blocks.
    """
    r = safe_get(url, headers=headers, retry_count=retry_count)
    if r.status_code != 200:
        return {"Error": f"HTML fetch failed HTTP {r.status_code}"}

    html = r.text
    soup = BeautifulSoup(html, "html.parser")

    out = {}

    # Title
    if soup.title:
        out["Product Name"] = soup.title.get_text(strip=True)

    # Meta tags
    og_title = soup.find("meta", {"property": "og:title"})
    if og_title and og_title.get("content"):
        out["Product Name"] = og_title["content"]

    og_desc = soup.find("meta", {"property": "og:description"})
    if og_desc and og_desc.get("content"):
        out["Description"] = og_desc["content"]

    og_image = soup.find("meta", {"property": "og:image"})
    if og_image and og_image.get("content"):
        out["Image"] = og_image["content"]

    # JSON-LD (often contains brand/price)
    scripts = soup.find_all("script", {"type": "application/ld+json"})
    for s in scripts:
        try:
            jd = json.loads(s.string)
            if isinstance(jd, dict):
                if "brand" in jd:
                    if isinstance(jd["brand"], dict):
                        out["Brand"] = jd["brand"].get("name", "")
                    else:
                        out["Brand"] = str(jd["brand"])
                if "name" in jd and not out.get("Product Name"):
                    out["Product Name"] = jd["name"]
                if "offers" in jd and isinstance(jd["offers"], dict):
                    out["Selling Price"] = jd["offers"].get("price", "")
                    out["Currency"] = jd["offers"].get("priceCurrency", "")
        except Exception:
            continue

    return out

# -----------------------------
# Crawl one URL
# -----------------------------
def crawl_single(url, headers, retry_count):
    if stop_event.is_set():
        return {"URL": url, "Error": "Stopped by user"}

    out = {"URL": url}

    mp_code = extract_mp_code(url)
    out["SKU Code"] = mp_code or ""

    # Try API first
    product = None
    if mp_code:
        try:
            product = try_sku_api_variants(mp_code, headers, retry_count)
        except Exception as e:
            out["API Error"] = str(e)

    if product:
        out["Brand"] = product.get("brand", "")
        out["Product Name"] = product.get("productName", "") or product.get("name", "")
        out["MRP"] = product.get("mrp", "")
        out["Selling Price"] = product.get("offerPrice", "") or product.get("sellingPrice", "")
        out["Category"] = flatten_list(product.get("category", ""))
        out["Sub Category"] = flatten_list(product.get("subCategory", ""))
        out["In Stock"] = product.get("inStock", "")
        return out

    # Fallback: HTML extraction
    try:
        html_data = extract_from_html(url, headers, retry_count)
        out.update(html_data)
        if "Error" in html_data:
            out["Error"] = html_data["Error"]
        else:
            out["Error"] = ""  # no error, just fallback
    except Exception as e:
        out["Error"] = str(e)

    return out

# -----------------------------
# Crawl runner (multithread)
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
# UI
# -----------------------------
st.title("TataCliq Product Crawler")
st.caption("Upload URLs (Excel/CSV) → validate/dedupe → crawl → download extracted data")

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
    st.error("No valid URLs found.")
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

    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        out_df.to_excel(writer, index=False, sheet_name="Extracted")

    st.download_button(
        label="Download Extracted Data (Excel)",
        data=buffer.getvalue(),
        file_name="tatacliq_extracted.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    st.download_button(
        label="Download Extracted Data (CSV)",
        data=out_df.to_csv(index=False).encode("utf-8"),
        file_name="tatacliq_extracted.csv",
        mime="text/csv",
    )

