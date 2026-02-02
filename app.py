import json
import time
import random
import io
import threading
import concurrent.futures
from collections import OrderedDict
from copy import deepcopy

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
# Headers
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
    headers = dict(BASE_HEADERS)
    if cookie_text and cookie_text.strip():
        headers["cookie"] = cookie_text.strip()
    return headers


# =========================================================
# Safe Request with Retry
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
# Size Guide Helpers
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

    res = safe_get(url, headers=headers, params=params, retry_count=retry_count)
    js = res.json()

    unit_data = OrderedDict()
    main_size = []

    for size_map in js["sizeGuideTabularWsData"]["unitList"]:
        unit = size_map["displaytext"]

        unit_data.setdefault(unit, OrderedDict())

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
# PRODUCT EXTRACTION (UPDATED CRAWLER)
# =========================================================
def extract_product(input_value: str, headers: dict, retry_count: int):
    data = {}
    raw_input = str(input_value).strip()

    if not raw_input:
        return {"Input": input_value, "Error": "Empty input"}

    if "tatacliq.com" in raw_input:
        product_id = raw_input.split("/p-")[-1].strip()
    else:
        product_id = raw_input.strip()

    product_id_upper = product_id.upper()

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
        body = res.text.strip()

        if body.startswith("{"):
            json_data = json.loads(body)
        elif "<p>" in body:
            soup = BeautifulSoup(body, "html.parser")
            json_data = json.loads(soup.text)
        else:
            data["blocked"] = True
            data["Error"] = "Unexpected response format"
            data["Raw_Response_Preview"] = body[:500]
            return data

    except Exception as e:
        data["blocked"] = True
        data["Error"] = str(e)
        return data

    data["blocked"] = False
    data["Error"] = ""

    # Variant resolution
    variant_found = False
    if json_data.get("variantOptions"):
        for v in json_data["variantOptions"]:
            sizelink = v.get("sizelink")
            colorlink = v.get("colorlink")
            if sizelink and sizelink.get("productCode") == product_id_upper:
                data.update({
                    "product_url": "https://www.tatacliq.com" + sizelink.get("url", ""),
                    "product_code": sizelink.get("productCode"),
                    "Product_size": sizelink.get("size"),
                    "color": colorlink.get("color") if colorlink else None,
                    "color_hex": colorlink.get("colorHexCode") if colorlink else None
                })
                variant_found = True
                break

    if not variant_found:
        data["product_url"] = "https://www.tatacliq.com" + json_data.get("seo", {}).get("alternateURL", "")
        data["product_code"] = product_id

    # Core fields
    for f in [
        "productTitle", "brandName", "productColor",
        "productDescription", "styleNote",
        "productListingId", "rootCategory"
    ]:
        data[f] = json_data.get(f)

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
        if d.get("key"):
            data[d["key"]] = d.get("value")

    # Images
    imgs = []
    for g in json_data.get("galleryImagesList", []):
        for k in g.get("galleryImages", []):
            if k.get("key") == "superZoom":
                imgs.append("https:" + k.get("value"))
    for i, im in enumerate(imgs):
        data[f"image_{i+1}"] = im

    # Manufacturer
    if json_data.get("mfgDetails"):
        for k, v in json_data["mfgDetails"].items():
            data[k] = v[0]["value"] if isinstance(v, list) else v

    # Seller
    data["Seller_name"] = json_data.get("winningSellerName")
    data["Seller_address"] = json_data.get("winningSellerAddress")

    # Classifications
    for cls in json_data.get("classifications", []):
        for spec in cls.get("specifications", []):
            if spec.get("key"):
                data[spec["key"]] = spec.get("value")

    # Return / Refund
    for i, r in enumerate(json_data.get("returnAndRefund", []), start=1):
        if r.get("refundReturnItem"):
            data[f"refundReturnInfo_{i}"] = r["refundReturnItem"]

    # Available sizes
    sizes = []
    for g in json_data.get("variantGroup", []):
        for s in g.get("sizeOptions", []):
            if s.get("size") and s["size"] not in sizes:
                sizes.append(s["size"])
    if sizes:
        data["Available Size"] = sizes

    # Size guide
    if json_data.get("sizeGuideId"):
        try:
            data.update(get_size_guide(product_id, json_data["sizeGuideId"], headers, retry_count))
        except Exception as e:
            data["SizeGuide_Error"] = str(e)

    return data


# =========================================================
# Streamlit UI
# =========================================================
st.title("TataCliq Product Extractor")

with st.expander("Cookie Instructions", expanded=True):
    st.write(
        """
1. Open TataCliq in Chrome  
2. Press F12 → Network → Reload  
3. Click any request → Headers → Copy **cookie**  
4. Paste below
"""
    )

cookie_text = st.text_area("Paste cookie header (recommended)", height=160)

uploaded_file = st.file_uploader("Upload Excel / CSV", type=["xlsx", "csv"])

c1, c2, c3 = st.columns(3)
with c1:
    max_workers = st.number_input("Threads", 1, 30, 8)
with c2:
    retry_count = st.number_input("Retry count", 1, 10, 3)
with c3:
    validate_only = st.checkbox("Only validate inputs", False)

st.button("Stop Extraction", on_click=request_stop)

if not uploaded_file:
    st.stop()

df = pd.read_excel(uploaded_file) if uploaded_file.name.endswith("xlsx") else pd.read_csv(uploaded_file)

st.dataframe(df.head(20), use_container_width=True)

col = st.selectbox("Column with TataCliq URL / ID", df.columns)
inputs = list(dict.fromkeys(df[col].dropna().astype(str).str.strip().tolist()))

if validate_only:
    st.success("Validation done")
    st.dataframe(pd.DataFrame({"Input": inputs}))
    st.stop()

if st.button("Start Extraction", type="primary"):
    stop_event.clear()
    headers = build_headers(cookie_text)

    results = []
    progress = st.progress(0.0)

    with concurrent.futures.ThreadPoolExecutor(max_workers=int(max_workers)) as exe:
        futures = [exe.submit(extract_product, i, headers, int(retry_count)) for i in inputs]
        for idx, f in enumerate(concurrent.futures.as_completed(futures), 1):
            results.append(f.result())
            progress.progress(idx / len(inputs))

    out_df = pd.DataFrame(results)
    st.dataframe(out_df.head(50), use_container_width=True)

    out_xlsx = io.BytesIO()
    with pd.ExcelWriter(out_xlsx, engine="xlsxwriter") as writer:
        out_df.to_excel(writer, index=False)

    st.download_button("Download Excel", out_xlsx.getvalue(), "tatacliq_output.xlsx")
    st.download_button("Download CSV", out_df.to_csv(index=False).encode(), "tatacliq_output.csv")
