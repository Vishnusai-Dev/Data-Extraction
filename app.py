import json
import time
import random
import re
import io
import threading
import concurrent.futures
from collections import OrderedDict

import requests
import pandas as pd
import streamlit as st
from bs4 import BeautifulSoup


# ================= STOP CONTROL =================
stop_event = threading.Event()

def request_stop():
    stop_event.set()


# ================= HEADERS =================
# NOTE: Cookie is mandatory in many cases for TataCliq
HEADERS = {
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9',
    'mode': 'no-cors',
    'priority': 'u=1, i',
    'referer': 'https://www.tatacliq.com/',
    'sec-ch-ua': '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
}

def add_cookie(headers: dict, cookie_text: str):
    h = dict(headers)
    if cookie_text and cookie_text.strip():
        h["cookie"] = cookie_text.strip()
    return h


# ================= RETRY HELPERS =================
def safe_get(url, headers, params=None, retry_count=3, timeout=25):
    last_err = None
    for attempt in range(1, retry_count + 1):
        if stop_event.is_set():
            raise RuntimeError("Stopped by user")

        try:
            r = requests.get(url, headers=headers, params=params, timeout=timeout)
            if r.status_code == 200:
                return r
            last_err = f"HTTP {r.status_code}"
        except Exception as e:
            last_err = str(e)

        time.sleep(0.7 * attempt + random.random())

    raise RuntimeError(f"Failed after {retry_count} attempts. Last error: {last_err}")


# ================= SIZE GUIDE HELPERS =================
def format_size_header(raw_dim, unit=None):
    if unit:
        unit = "Inches" if unit.lower() == "in" else unit
        return f"{raw_dim} ( {unit} )"
    return raw_dim


def get_size_guide(product_id, sizeGuideId, headers):
    url = f"https://www.tatacliq.com/marketplacewebservices/v2/mpl/products/{product_id}/sizeGuideChart"
    params = {
        "isPwa": "true",
        "sizeGuideId": sizeGuideId,
        "rootCategory": "Clothing"
    }

    res = safe_get(url, params=params, headers=headers)
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


# ================= PRODUCT EXTRACTION =================
def extract_product(input_value: str, headers: dict, retry_count: int):
    data = {}
    sku = str(input_value).strip()

    if not sku:
        return {"Input": input_value, "Error": "Empty input"}

    # supports url or sku
    if "tatacliq.com" in sku:
        product_id = sku.split("/p-")[-1]
    else:
        product_id = sku

    api_url = f"https://www.tatacliq.com/marketplacewebservices/v2/mpl/products/productDetails/{product_id}?isPwa=true&isMDE=true&isDynamicVar=true"

    try:
        res = safe_get(api_url, headers=headers, retry_count=retry_count)
        soup = BeautifulSoup(res.content, "html.parser")

        # JSON returned inside <p> ... </p>
        raw = str(soup)
        if "<p>" not in raw:
            return {"Input": input_value, "product_id": product_id, "Error": "No <p> JSON found (possible blocked request)"}

        json_text = raw.split("<p>")[-1].split("</p>")[0]
        json_data = json.loads(json_text)

    except Exception as e:
        return {"Input": input_value, "product_id": product_id, "Error": str(e)}

    data["Input"] = input_value
    data["product_id"] = product_id
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
    if json_data.get("discount"):
        data["Discount"] = json_data["discount"]

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
                imgs.append("https:" + k.get("value", ""))

    for i, im in enumerate(imgs):
        data[f"image_{i+1}"] = im

    # Manufacturer
    if json_data.get("mfgDetails"):
        for k, v in json_data["mfgDetails"].items():
            data[k] = v[0]["value"] if isinstance(v, list) else v

    # Size Guide
    if json_data.get("sizeGuideId"):
        try:
            size_data = get_size_guide(product_id, json_data["sizeGuideId"], headers=headers)
            data.update(size_data)
        except Exception as e:
            data["SizeGuide_Error"] = str(e)

    return data


# ================= STREAMLIT UI =================
st.title("TataCliq Product Data Extractor (Working API Version)")

uploaded_file = st.file_uploader("Upload Excel/CSV File (URL or SKU column)", type=["xlsx", "csv"])

cookie_text = st.text_area(
    "Paste Cookie (Required for TataCliq access in most cases)",
    height=130
)

c1, c2, c3 = st.columns(3)
with c1:
    max_workers = st.number_input("Threads", min_value=1, max_value=20, value=8, step=1)
with c2:
    retry_count = st.number_input("Retry count", min_value=1, max_value=10, value=3, step=1)
with c3:
    validate_only = st.checkbox("Only validate inputs (no crawl)", value=False)

st.button("Stop Extraction", on_click=request_stop)

if uploaded_file:
    if uploaded_file.name.lower().endswith(".csv"):
        df = pd.read_csv(uploaded_file)
    else:
        df = pd.read_excel(uploaded_file)

    st.success(f"Loaded {len(df)} rows")
    column = st.selectbox("Select column containing URL / SKU", df.columns)

    # Validate + dedupe
    inputs = df[column].dropna().astype(str).tolist()
    inputs = [i.strip() for i in inputs if i.strip()]
    inputs = list(dict.fromkeys(inputs))  # dedupe preserve order

    st.write(f"Valid inputs after dedupe: {len(inputs)}")

    if validate_only:
        st.dataframe(pd.DataFrame({"Inputs": inputs}).head(50), use_container_width=True)
        st.stop()

    if st.button("Start Extraction", type="primary"):
        stop_event.clear()
        headers = add_cookie(HEADERS, cookie_text)

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

        st.dataframe(out_df.head(20), use_container_width=True)

        # Save to Excel in memory
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
