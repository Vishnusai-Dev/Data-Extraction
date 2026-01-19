import json
import time
import random
import io
import threading
import concurrent.futures
from collections import OrderedDict

import requests
import pandas as pd
import streamlit as st
from bs4 import BeautifulSoup


# =========================================================
# Streamlit Config
# =========================================================
st.set_page_config(page_title="TataCliq Data Extractor", layout="wide")


# =========================================================
# Stop Control (Thread Safe)
# =========================================================
stop_event = threading.Event()

def request_stop():
    stop_event.set()


# =========================================================
# Headers (same base as your working code)
# =========================================================
BASE_HEADERS = {
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9",
    "mode": "no-cors",
    "priority": "u=1, i",
    "referer": "https://www.tatacliq.com/",
    "sec-ch-ua": '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
}

def build_headers(cookie_text: str):
    """
    Cookie is often required for TataCliq.
    But even without cookie, TataCliq may return raw JSON (mobile payload) sometimes.
    """
    headers = dict(BASE_HEADERS)
    if cookie_text and cookie_text.strip():
        headers["cookie"] = cookie_text.strip()
    return headers


# =========================================================
# Retry helper
# =========================================================
def safe_get(url, headers, params=None, retry_count=3, timeout=25):
    last_err = None

    for attempt in range(1, retry_count + 1):
        if stop_event.is_set():
            raise RuntimeError("Stopped by user")

        try:
            r = requests.get(url, headers=headers, params=params, timeout=timeout)
            return r
        except Exception as e:
            last_err = str(e)

        time.sleep(0.7 * attempt + random.random())

    raise RuntimeError(f"Request failed after {retry_count} attempts. Last error: {last_err}")


# =========================================================
# Size Guide logic (from your working script)
# =========================================================
def format_size_header(raw_dim, unit=None):
    if unit:
        unit = "Inches" if unit.lower() == "in" else unit
        return f"{raw_dim} ( {unit} )"
    return raw_dim


def get_size_guide(product_id, sizeGuideId, headers, retry_count=3):
    url = f"https://www.tatacliq.com/marketplacewebservices/v2/mpl/products/{product_id}/sizeGuideChart"
    params = {
        "isPwa": "true",
        "sizeGuideId": sizeGuideId,
        "rootCategory": "Clothing"
    }

    res = safe_get(url, params=params, headers=headers, retry_count=retry_count)
    js = res.json()

    unit_data = OrderedDict()
    main_size = []

    for size_map in js["sizeGuideTabularWsData"]["unitList"]:
        unit = size_map["displaytext"]

        if unit not in unit_data:
            unit_data[unit] = OrderedDict()

        for size_name in size_map["sizeGuideList"]:
            size = size_name["dimensionSize"]
            if size not in main_size:
                main_size.append(size)

            for dim in size_name["dimensionList"]:
                d = dim["dimension"]
                v = dim["dimensionValue"]
                unit_data[unit].setdefault(d, []).append(v)

    final = OrderedDict()
    final["Brand Size"] = main_size

    dims = set(unit_data.get("Cm", {})) | set(unit_data.get("In", {}))

    for dim in dims:
        cm = unit_data.get("Cm", {}).get(dim)
        inch = unit_data.get("In", {}).get(dim)

        if cm == inch:
            final[format_size_header(dim)] = cm
        else:
            if cm:
                final[format_size_header(dim, "Cm")] = cm
            if inch:
                final[format_size_header(dim, "In")] = inch

    final["measurement_image"] = js.get("imageURL")
    return final


# =========================================================
# Product extraction - FINAL robust version
# =========================================================
def extract_product(input_value: str, headers: dict, retry_count: int):
    """
    Uses the SAME TataCliq API as your notebook:
    /productDetails/{product_id}?isPwa=true&isMDE=true&isDynamicVar=true

    But handles both response formats:
    1) <p>{json}</p> (HTML wrapper)
    2) raw JSON string: { ... }
    """
    data = {}
    raw_input = str(input_value).strip()

    if not raw_input:
        return {"Input": input_value, "Error": "Empty input"}

    # supports URL or SKU
    if "tatacliq.com" in raw_input:
        product_id = raw_input.split("/p-")[-1].strip()
    else:
        product_id = raw_input.strip()

    api_url = (
        f"https://www.tatacliq.com/marketplacewebservices/v2/mpl/products/productDetails/"
        f"{product_id}?isPwa=true&isMDE=true&isDynamicVar=true"
    )

    data["Input"] = input_value
    data["product_id"] = product_id
    data["api_url"] = api_url

    try:
        res = safe_get(api_url, headers=headers, retry_count=retry_count)
        data["http_status"] = res.status_code

        body = res.text or ""
        body_stripped = body.strip()

        # ----------------------------
        # FORMAT A: Raw JSON directly
        # ----------------------------
        if body_stripped.startswith("{") and body_stripped.endswith("}"):
            try:
                json_data = json.loads(body_stripped)
            except Exception as e:
                data["blocked"] = True
                data["Error"] = f"Raw JSON parse failed: {e}"
                data["Raw_Response_Preview"] = body_stripped[:500]
                return data

        # ----------------------------
        # FORMAT B: HTML with <p>{json}</p>
        # ----------------------------
        elif "<p>" in body:
            json_text = body.split("<p>")[-1].split("</p>")[0].strip()
            try:
                json_data = json.loads(json_text)
            except Exception as e:
                data["blocked"] = True
                data["Error"] = f"<p> JSON parse failed: {e}"
                data["Raw_Response_Preview"] = json_text[:500]
                return data

        # ----------------------------
        # UNKNOWN FORMAT (blocked/captcha/etc.)
        # ----------------------------
        else:
            data["blocked"] = True
            data["Error"] = "Unexpected response format (not raw JSON, not <p> JSON)"
            data["Raw_Response_Preview"] = body_stripped[:500]
            return data

    except Exception as e:
        data["blocked"] = True
        data["Error"] = str(e)
        return data

    # ------------------------------
    # NORMAL EXTRACTION
    # ------------------------------
    data["blocked"] = False
    data["Error"] = ""

    data["productTitle"] = json_data.get("productTitle")
    data["brandName"] = json_data.get("brandName")
    data["productDescription"] = json_data.get("productDescription")
    data["productColor"] = json_data.get("productColor")
    data["styleNote"] = json_data.get("styleNote")

    # Pricing
    if json_data.get("mrpPrice"):
        data["MRP"] = json_data["mrpPrice"].get("value")
    if json_data.get("winningSellerPrice"):
        data["Price"] = json_data["winningSellerPrice"].get("value")
    if json_data.get("discount") is not None:
        data["Discount"] = json_data.get("discount")

    # Breadcrumbs
    for i, c in enumerate(json_data.get("categoryHierarchy", [])):
        data[f"Breadcrum_{i+1}"] = c.get("category_name")

    # Details
    for d in json_data.get("details", []):
        k = d.get("key")
        v = d.get("value")
        if k:
            data[k] = v

    # Images
    imgs = []
    for g in json_data.get("galleryImagesList", []):
        for k in g.get("galleryImages", []):
            if k.get("key") == "superZoom":
                val = k.get("value", "")
                if val:
                    imgs.append("https:" + val)

    for i, im in enumerate(imgs):
        data[f"image_{i+1}"] = im

    # Manufacturer details
    if json_data.get("mfgDetails"):
        for k, v in json_data["mfgDetails"].items():
            data[k] = v[0]["value"] if isinstance(v, list) else v

    # Size guide
    if json_data.get("sizeGuideId"):
        try:
            size_data = get_size_guide(product_id, json_data["sizeGuideId"], headers=headers, retry_count=retry_count)
            data.update(size_data)
        except Exception as e:
            data["SizeGuide_Error"] = str(e)

    return data


# =========================================================
# Streamlit UI
# =========================================================
st.title("TataCliq Product Extractor")
st.caption("Uses your same API endpoint + logic, but supports both HTML <p> JSON and Raw JSON responses.")

with st.expander("Cookie Instructions (Important)", expanded=True):
    st.write(
        """
**Why Cookie is useful?**
TataCliq sometimes blocks cloud/bot traffic. Cookie improves success rate.

**How to get cookie:**
1. Open TataCliq in Chrome
2. Press F12 → Network tab
3. Reload
4. Click a request → Headers → copy full `cookie:` value
5. Paste below
"""
    )

cookie_text = st.text_area("Paste cookie header here (optional but recommended)", height=160)

uploaded_file = st.file_uploader("Upload input file (Excel/CSV)", type=["xlsx", "csv"])

c1, c2, c3, c4 = st.columns(4)
with c1:
    max_workers = st.number_input("Threads", min_value=1, max_value=30, value=8, step=1)
with c2:
    retry_count = st.number_input("Retry count", min_value=1, max_value=10, value=3, step=1)
with c3:
    validate_only = st.checkbox("Only validate inputs", value=False)
with c4:
    show_failed_preview = st.checkbox("Show failed preview table", value=True)

st.button("Stop Extraction", on_click=request_stop)

if not uploaded_file:
    st.info("Upload a file to start.")
    st.stop()

# read input
try:
    if uploaded_file.name.lower().endswith(".csv"):
        df = pd.read_csv(uploaded_file)
    else:
        df = pd.read_excel(uploaded_file)
except Exception as e:
    st.error(f"Could not read file: {e}")
    st.stop()

st.subheader("Input Preview")
st.dataframe(df.head(20), use_container_width=True)

col = st.selectbox("Select column containing TataCliq URL or product_id", df.columns)

# build inputs + dedupe
inputs = df[col].dropna().astype(str).tolist()
inputs = [x.strip() for x in inputs if x and x.strip()]
inputs = list(dict.fromkeys(inputs))

st.write(f"Rows in file: {len(df)}")
st.write(f"Valid inputs after dedupe: {len(inputs)}")

if validate_only:
    st.success("Validation completed.")
    st.dataframe(pd.DataFrame({"Input": inputs}).head(100), use_container_width=True)
    st.stop()

if st.button("Start Extraction", type="primary"):
    stop_event.clear()
    headers = build_headers(cookie_text)

    results = []
    progress = st.progress(0.0)
    status = st.empty()

    with st.spinner("Extracting data..."):
        with concurrent.futures.ThreadPoolExecutor(max_workers=int(max_workers)) as exe:
            futures = {exe.submit(extract_product, inp, headers, int(retry_count)): inp for inp in inputs}

            done = 0
            for fut in concurrent.futures.as_completed(futures):
                if stop_event.is_set():
                    break

                res = fut.result()
                results.append(res)

                done += 1
                progress.progress(done / len(inputs))
                status.write(f"Completed {done}/{len(inputs)}")

    out_df = pd.DataFrame(results)

    st.success("Extraction completed!")
    st.subheader("Output Preview")
    st.dataframe(out_df.head(50), use_container_width=True)

    # show failures
    if show_failed_preview:
        failed = out_df[out_df.get("blocked", False) == True]
        if len(failed) > 0:
            st.warning(f"Failed/Unexpected format rows: {len(failed)}")
            keep_cols = [c for c in ["Input", "product_id", "http_status", "Error", "Raw_Response_Preview"] if c in failed.columns]
            st.dataframe(failed[keep_cols].head(50), use_container_width=True)

    # Downloads
    out_xlsx = io.BytesIO()
    with pd.ExcelWriter(out_xlsx, engine="xlsxwriter") as writer:
        out_df.to_excel(writer, index=False, sheet_name="Output")
    out_xlsx.seek(0)

    st.download_button(
        label="Download Output Excel",
        data=out_xlsx.getvalue(),
        file_name="tatacliq_output.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    st.download_button(
        label="Download Output CSV",
        data=out_df.to_csv(index=False).encode("utf-8"),
        file_name="tatacliq_output.csv",
        mime="text/csv"
    )
