import json
import time
import random
import io
import threading
import concurrent.futures
from collections import OrderedDict
from copy import deepcopy
from html import unescape

import requests
import pandas as pd
import streamlit as st
from bs4 import BeautifulSoup


# =========================================================
# Streamlit Config
# =========================================================
st.set_page_config(page_title="TataCliq Full Data Extractor", layout="wide")


# =========================================================
# Stop Control
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

def build_headers(cookie_text):
    headers = dict(BASE_HEADERS)
    if cookie_text.strip():
        headers["cookie"] = cookie_text.strip()
    return headers


# =========================================================
# Helpers
# =========================================================
def safe_get(url, headers, params=None, retry=3, timeout=25):
    for i in range(retry):
        if stop_event.is_set():
            raise RuntimeError("Stopped by user")
        try:
            return requests.get(url, headers=headers, params=params, timeout=timeout)
        except:
            time.sleep(0.6 * (i + 1))
    return None


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


def get_size_guide(pid, sizeGuideId, headers):
    res = safe_get(
        f"https://www.tatacliq.com/marketplacewebservices/v2/mpl/products/{pid}/sizeGuideChart",
        headers=headers,
        params={"isPwa": "true", "sizeGuideId": sizeGuideId, "rootCategory": "Clothing"},
    )
    if not res:
        return {}

    try:
        js = res.json()
    except:
        return {}

    unit_data, main_size = OrderedDict(), []

    for u in js.get("sizeGuideTabularWsData", {}).get("unitList", []):
        unit = u.get("displaytext")
        unit_data.setdefault(unit, OrderedDict())

        for s in u.get("sizeGuideList", []):
            size = s.get("dimensionSize")
            if size and size not in main_size:
                main_size.append(size)

            for d in s.get("dimensionList", []):
                unit_data[unit].setdefault(d["dimension"], []).append(d["dimensionValue"])

    out = OrderedDict()
    if main_size:
        out["Brand Size"] = main_size

    for dim in set(unit_data.get("Cm", {})) | set(unit_data.get("In", {})):
        if unit_data.get("Cm", {}).get(dim) == unit_data.get("In", {}).get(dim):
            out[format_size_header(dim)] = unit_data["Cm"][dim]
        else:
            if unit_data.get("Cm", {}).get(dim):
                out[format_size_header(dim, "Cm")] = unit_data["Cm"][dim]
            if unit_data.get("In", {}).get(dim):
                out[format_size_header(dim, "In")] = unit_data["In"][dim]

    if js.get("imageURL"):
        out["measurement_image"] = js["imageURL"]

    return out


# =========================================================
# MAIN EXTRACTION
# =========================================================
def extract_product(url, headers, retry):
    data = {"Input": url}

    try:
        pid = url.split("/p-")[-1]
        pid_u = pid.upper()

        res = safe_get(
            f"https://www.tatacliq.com/marketplacewebservices/v2/mpl/products/productDetails/{pid_u}",
            headers=headers,
            params={"isPwa": "true", "isMDE": "true", "isDynamicVar": "true"},
            retry=retry
        )

        if not res:
            return data

        js = res.json()

        # Core
        for k in [
            "productTitle","brandName","productColor","productDescription",
            "styleNote","productListingId","rootCategory"
        ]:
            data[k] = js.get(k)

        # Pricing
        data["MRP"] = js.get("mrpPrice", {}).get("value")
        data["Price"] = js.get("winningSellerPrice", {}).get("value")
        data["Discount"] = js.get("discount")

        # Breadcrumbs
        for i, c in enumerate(js.get("categoryHierarchy", []), 1):
            data[f"Breadcrum_{i}"] = c.get("category_name")

        # Images
        idx = 1
        for g in js.get("galleryImagesList", []):
            for k in g.get("galleryImages", []):
                if k.get("key") == "superZoom":
                    data[f"image_{idx}"] = "https:" + k.get("value")
                    idx += 1

        # Details
        for d in js.get("details", []):
            data[d["key"]] = d["value"]

        # Overview / specificationGroup
        for g in js.get("specificationGroup", []):
            for s in g.get("specifications", []):
                data[s["key"]] = s["value"]

        # detailsSection
        for i in js.get("detailsSection", []):
            data[i["key"]] = i["value"]

        # classificationList
        for sec in js.get("classificationList", []):
            key = sec.get("key")
            val = sec.get("value", {})
            if "classificationList" in val:
                for i in val["classificationList"]:
                    data[i["key"]] = i["value"]
            elif "classificationValues" in val:
                data[key] = ", ".join(val["classificationValues"])

        # knowMore → Features
        for i, k in enumerate(js.get("knowMore", []), 1):
            data[f"Feature_{i}"] = k.get("knowMoreItem")

        # Composition
        for i in js.get("otherIngredients", []):
            data["Composition"] = i.get("value")

        # Ratings
        data["averageRating"] = js.get("averageRating")
        data["ratingCount"] = js.get("ratingCount")
        data["numberOfReviews"] = js.get("numberOfReviews")

        # Customer Voice
        cv = safe_get(
            f"https://www.tatacliq.com/marketplacewebservices/v2/mpl/products/{pid_u}/customerVoice",
            headers=headers,
            retry=retry
        )
        if cv:
            for i in cv.json().get("customerVoiceData", []):
                data[i["text"]] = i["value"]

        # Manufacturer / Packer
        try:
            brand = js.get("brandURL", "").split("c-")[-1]
            cat = js.get("categoryHierarchy")[-1]["category_id"]

            mfg = safe_get(
                "https://www.tatacliq.com/marketplacewebservices/v2/mpl/products/manufacturingdetails",
                headers=headers,
                params={"brand": brand.upper(), "category": cat.upper()},
                retry=retry
            )

            mj = mfg.json()
            if mj.get("manufacturer"):
                data["manufacturer"] = mj["manufacturer"][0]["value"]
            if mj.get("packer"):
                data["packer"] = mj["packer"][0]["value"]
        except:
            pass

        # Size Guide
        if js.get("sizeGuideId"):
            data.update(get_size_guide(pid_u, js["sizeGuideId"], headers))

        # A+ Content
        c = 1
        for i in js.get("APlusContent", {}).get("productContent", []):
            tl = i.get("value", {}).get("textList")
            if tl:
                data[f"APlus_Content_{c}"] = " ".join(clean_html(t) for t in tl)
                c += 1

        return data

    except Exception as e:
        data["Error"] = str(e)
        return data


# =========================================================
# STREAMLIT UI
# =========================================================
st.title("TataCliq Full Data Extractor")

cookie_text = st.text_area("Paste cookie header", height=150)
uploaded_file = st.file_uploader("Upload Excel / CSV", type=["xlsx", "csv"])

threads = st.number_input("Threads", 1, 20, 8)
retry_count = st.number_input("Retry count", 1, 10, 3)

st.button("Stop Extraction", on_click=request_stop)

if uploaded_file:
    df = pd.read_excel(uploaded_file) if uploaded_file.name.endswith("xlsx") else pd.read_csv(uploaded_file)
    col = st.selectbox("Select URL column", df.columns)
    urls = df[col].dropna().astype(str).tolist()

    if st.button("Start Extraction", type="primary"):
        headers = build_headers(cookie_text)
        results = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=int(threads)) as exe:
            for idx, r in enumerate(exe.map(lambda u: extract_product(u, headers, retry_count), urls), 1):
                results.append(r)
                st.progress(idx / len(urls))

        out_df = pd.DataFrame(results)
        st.dataframe(out_df.head(50), use_container_width=True)

        out_xlsx = io.BytesIO()
        with pd.ExcelWriter(out_xlsx, engine="xlsxwriter") as writer:
            out_df.to_excel(writer, index=False)

        st.download_button("Download Excel", out_xlsx.getvalue(), "tatacliq_full_output.xlsx")
        st.download_button("Download CSV", out_df.to_csv(index=False).encode(), "tatacliq_full_output.csv")
