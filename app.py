import json
import time
import random
import io
import threading
import concurrent.futures
from collections import OrderedDict
from copy import deepcopy
from html import unescape
import re

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
# Helpers
# =========================================================
def safe_get(url, headers, params=None, retry_count=3, timeout=25):
    last_err = None
    for attempt in range(1, retry_count + 1):
        if stop_event.is_set():
            raise RuntimeError("Stopped by user")
        try:
            return requests.get(url, headers=headers, params=params, timeout=timeout)
        except Exception as e:
            last_err = str(e)
            time.sleep(0.6 * attempt)
    raise RuntimeError(f"Request failed: {last_err}")

def clean_html(text):
    return BeautifulSoup(unescape(str(text)), "html.parser").get_text(" ", strip=True)


# =========================================================
# Size Guide
# =========================================================
def format_size_header(raw_dim, unit=None):
    if unit:
        unit = "Inches" if unit.lower() == "in" else unit
        return f"{raw_dim} ( {unit} )"
    return raw_dim

def get_size_guide(product_id, sizeGuideId, headers, retry_count=3):
    try:
        res = safe_get(
            f"https://www.tatacliq.com/marketplacewebservices/v2/mpl/products/{product_id}/sizeGuideChart",
            headers=headers,
            params={
                "isPwa": "true",
                "sizeGuideId": sizeGuideId,
                "rootCategory": "Clothing"
            },
            retry_count=retry_count
        )
        js = res.json()
    except:
        return OrderedDict()

    unit_data, main_size = OrderedDict(), []

    for size_map in js.get("sizeGuideTabularWsData", {}).get("unitList", []):
        unit = size_map.get("displaytext")
        unit_data.setdefault(unit, OrderedDict())

        for size_name in size_map.get("sizeGuideList", []):
            size = size_name.get("dimensionSize")
            if size and size not in main_size:
                main_size.append(size)

            for dim in size_name.get("dimensionList", []):
                unit_data[unit].setdefault(dim["dimension"], []).append(dim["dimensionValue"])

    final = OrderedDict()
    if main_size:
        final["Brand Size"] = main_size

    for dim in set(unit_data.get("Cm", {})) | set(unit_data.get("In", {})):
        if unit_data.get("Cm", {}).get(dim) == unit_data.get("In", {}).get(dim):
            final[format_size_header(dim)] = unit_data["Cm"][dim]
        else:
            if unit_data.get("Cm", {}).get(dim):
                final[format_size_header(dim, "Cm")] = unit_data["Cm"][dim]
            if unit_data.get("In", {}).get(dim):
                final[format_size_header(dim, "In")] = unit_data["In"][dim]

    if js.get("imageURL"):
        final["measurement_image"] = js["imageURL"]

    return final


# =========================================================
# PRODUCT EXTRACTION (FULLY UPDATED)
# =========================================================
def extract_product(input_value: str, headers: dict, retry_count: int):
    data = {"Input": input_value}

    try:
        product_id = input_value.split("/p-")[-1] if "tatacliq.com" in input_value else input_value
        product_id = product_id.strip()
        product_id_upper = product_id.upper()

        api_url = f"https://www.tatacliq.com/marketplacewebservices/v2/mpl/products/productDetails/{product_id_upper}?isPwa=true&isMDE=true&isDynamicVar=true"
        data["api_url"] = api_url

        res = safe_get(api_url, headers=headers, retry_count=retry_count)
        data["http_status"] = res.status_code

        json_data = res.json()
        data["blocked"] = False
        data["Error"] = ""

        # ---------------- Core
        for k in [
            "productTitle","brandName","productColor","productDescription",
            "styleNote","productListingId","rootCategory"
        ]:
            data[k] = json_data.get(k)

        # ---------------- Pricing
        data["MRP"] = json_data.get("mrpPrice", {}).get("value")
        data["Price"] = json_data.get("winningSellerPrice", {}).get("value")
        data["Discount"] = json_data.get("discount")

        # ---------------- Breadcrumbs
        for i, c in enumerate(json_data.get("categoryHierarchy", []), 1):
            data[f"Breadcrum_{i}"] = c.get("category_name")

        # ---------------- Images
        imgs = []
        for g in json_data.get("galleryImagesList", []):
            for k in g.get("galleryImages", []):
                if k.get("key") == "superZoom":
                    imgs.append("https:" + k.get("value"))
        for i, im in enumerate(imgs, 1):
            data[f"image_{i}"] = im

        # ---------------- Details
        for d in json_data.get("details", []):
            if d.get("key"):
                data[d["key"]] = d.get("value")

        # ---------------- Overview / Product Details (FIX FOR YOUR ISSUE)
        for group in json_data.get("specificationGroup", []):
            for spec in group.get("specifications", []):
                if spec.get("key") and spec.get("value"):
                    data[spec["key"]] = spec["value"]

        # ---------------- detailsSection
        for item in json_data.get("detailsSection", []):
            if item.get("key") and item.get("value"):
                data[item["key"]] = item["value"]

        # ---------------- classificationList
        for section in json_data.get("classificationList", []):
            key = section.get("key")
            value_block = section.get("value", {})
            if "classificationList" in value_block:
                for i in value_block["classificationList"]:
                    if i.get("key"):
                        data[i["key"]] = i.get("value")
            elif "classificationValues" in value_block and key:
                data[key] = ", ".join(value_block.get("classificationValues", []))

        # ---------------- Composition
        for i in json_data.get("otherIngredients", []):
            if i.get("value"):
                data["Composition"] = i["value"]

        # ---------------- Seller
        data["Seller_name"] = json_data.get("winningSellerName")
        data["Seller_address"] = json_data.get("winningSellerAddress")

        # ---------------- Ratings
        data["averageRating"] = json_data.get("averageRating")
        data["ratingCount"] = json_data.get("ratingCount")
        data["numberOfReviews"] = json_data.get("numberOfReviews")

        # ---------------- Customer Voice
        try:
            voice_res = safe_get(
                f"https://www.tatacliq.com/marketplacewebservices/v2/mpl/products/{product_id_upper}/customerVoice",
                headers=headers,
                retry_count=retry_count
            )
            for v in voice_res.json().get("customerVoiceData", []):
                data[v["text"]] = v["value"]
        except:
            pass

        # ---------------- A+ Content
        count = 1
        for item in json_data.get("APlusContent", {}).get("productContent", []):
            text_list = item.get("value", {}).get("textList")
            if text_list:
                cleaned = " ".join(clean_html(t) for t in text_list)
                data[f"APlus_Content_{count}"] = cleaned
                count += 1

        # ---------------- Sizes
        sizes = []
        for g in json_data.get("variantGroup", []):
            for s in g.get("sizeOptions", []):
                if s.get("size") and s["size"] not in sizes:
                    sizes.append(s["size"])
        if sizes:
            data["Available Size"] = sizes

        # ---------------- Size Guide
        if json_data.get("sizeGuideId"):
            data.update(get_size_guide(product_id_upper, json_data["sizeGuideId"], headers, retry_count))

        return data

    except Exception as e:
        return {
            "Input": input_value,
            "blocked": True,
            "Error": str(e)
        }


# =========================================================
# Streamlit UI
# =========================================================
st.title("TataCliq Product Extractor (Full Coverage)")

cookie_text = st.text_area("Paste cookie header (recommended)", height=150)
uploaded_file = st.file_uploader("Upload Excel / CSV", type=["xlsx", "csv"])

threads = st.number_input("Threads", 1, 30, 8)
retry_count = st.number_input("Retry count", 1, 10, 3)

st.button("Stop Extraction", on_click=request_stop)

if uploaded_file:
    df = pd.read_excel(uploaded_file) if uploaded_file.name.endswith("xlsx") else pd.read_csv(uploaded_file)
    col = st.selectbox("Column with TataCliq URL / ID", df.columns)
    inputs = list(dict.fromkeys(df[col].dropna().astype(str).tolist()))

    if st.button("Start Extraction", type="primary"):
        headers = build_headers(cookie_text)
        results = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=int(threads)) as exe:
            for idx, r in enumerate(exe.map(lambda x: extract_product(x, headers, retry_count), inputs), 1):
                results.append(r)
                st.progress(idx / len(inputs))

        out_df = pd.DataFrame(results)
        st.dataframe(out_df.head(50), use_container_width=True)

        out_xlsx = io.BytesIO()
        with pd.ExcelWriter(out_xlsx, engine="xlsxwriter") as writer:
            out_df.to_excel(writer, index=False)

        st.download_button("Download Excel", out_xlsx.getvalue(), "tatacliq_output.xlsx")
        st.download_button("Download CSV", out_df.to_csv(index=False).encode(), "tatacliq_output.csv")
