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
st.set_page_config(page_title="TataCliq – Exact Python Parity Extractor", layout="wide")


# =========================================================
# Stop Control
# =========================================================
stop_event = threading.Event()
def request_stop():
    stop_event.set()


# =========================================================
# Headers (IDENTICAL BEHAVIOUR)
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

def build_headers(cookie):
    h = dict(BASE_HEADERS)
    if cookie:
        h["cookie"] = cookie.strip()
    return h


# =========================================================
# SAFE REQUEST (NO LOGIC CHANGE)
# =========================================================
def safe_get(url, headers, params=None, retry=3, timeout=25):
    for i in range(retry):
        if stop_event.is_set():
            raise RuntimeError("Stopped")
        try:
            return requests.get(url, headers=headers, params=params, timeout=timeout)
        except:
            time.sleep(0.6 * (i + 1))
    return None


# =========================================================
# SIZE GUIDE (UNCHANGED)
# =========================================================
def format_size_header(raw_dim, unit=None):
    if unit:
        unit = "Inches" if unit.lower() == "in" else unit
        return f"{raw_dim} ( {unit} )"
    return raw_dim


def get_size_guide(pid, sizeGuideId, headers):
    res = safe_get(
        f"https://www.tatacliq.com/marketplacewebservices/v2/mpl/products/{pid.upper()}/sizeGuideChart",
        headers=headers,
        params={"isPwa": "true", "sizeGuideId": sizeGuideId, "rootCategory": "Clothing"},
    )
    if not res:
        return OrderedDict()

    try:
        js = res.json()
    except:
        return OrderedDict()

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
# EXACT PRODUCT EXTRACTION (NO DEVIATION)
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

        json_data = res.json()

        # ---------- VARIANT RESOLUTION ----------
        variant_found = False
        if json_data.get("variantOptions"):
            for v in json_data["variantOptions"]:
                sizelink = v.get("sizelink")
                colorlink = v.get("colorlink")
                if sizelink and sizelink.get("productCode") == pid_u:
                    data["product_url"] = "https://www.tatacliq.com" + sizelink.get("url", "")
                    data["product_code"] = sizelink.get("productCode")
                    data["Product_size"] = sizelink.get("size")
                    if colorlink:
                        data["color"] = colorlink.get("color")
                        data["color_hex"] = colorlink.get("colorHexCode")
                    variant_found = True
                    break

        if not variant_found:
            data["product_url"] = "https://www.tatacliq.com" + json_data.get("seo", {}).get("alternateURL", "")
            data["product_code"] = pid

        # ---------- CORE ----------
        for k in [
            "productTitle","brandName","productColor","productDescription",
            "productListingId","rootCategory","styleNote",
            "brandURL","categoryL4Code","brandInfo"
        ]:
            data[k] = json_data.get(k)

        data["usisd"] = json_data.get("winningUssID")
        data["ussid"] = json_data.get("winningUssID")

        # ---------- PRICING ----------
        if json_data.get("mrpPrice"):
            data["MRP"] = json_data["mrpPrice"]["value"]
        if json_data.get("winningSellerPrice"):
            data["Price"] = json_data["winningSellerPrice"]["value"]
        data["Discount"] = json_data.get("discount")

        # ---------- BREADCRUMS ----------
        for i, c in enumerate(json_data.get("categoryHierarchy", []), start=1):
            data[f"Breadcrums_{i}"] = c.get("category_name")

        # ---------- DETAILS ----------
        for d in json_data.get("details", []):
            data[d["key"]] = d["value"]

        for d in json_data.get("detailsSection", []):
            data[d["key"]] = d["value"]

        # ---------- SPECIFICATIONS ----------
        for g in json_data.get("specificationGroup", []):
            for s in g.get("specifications", []):
                data[s["key"]] = s["value"]

        # ---------- CLASSIFICATIONS ----------
        for cls in json_data.get("classifications", []):
            for s in cls.get("specifications", []):
                data[s["key"]] = s["value"]

        for sec in json_data.get("classificationList", []):
            val = sec.get("value", {})
            if "classificationList" in val:
                for i in val["classificationList"]:
                    data[i["key"]] = i["value"]
            elif "classificationValues" in val:
                data[sec["key"]] = ", ".join(val["classificationValues"])

        # ---------- IMAGES ----------
        img = 1
        for g in json_data.get("galleryImagesList", []):
            for k in g.get("galleryImages", []):
                if k.get("key") == "superZoom":
                    data[f"image_{img}"] = "https:" + k["value"]
                    img += 1

        # ---------- MFG DETAILS ----------
        if json_data.get("mfgDetails"):
            for k, v in json_data["mfgDetails"].items():
                data[k] = v[0]["value"] if isinstance(v, list) else v

        # ---------- SELLER ----------
        data["Seller_name"] = json_data.get("winningSellerName")
        data["Seller_address"] = json_data.get("winningSellerAddress")

        # ---------- RETURN DETAILS ----------
        for i, r in enumerate(json_data.get("returnAndRefund", []), start=1):
            if r.get("refundReturnItem"):
                data[f"Return_Details_{i}"] = r["refundReturnItem"]

        # ---------- AVAILABLE SIZE ----------
        sizes = []
        for g in json_data.get("variantGroup", []):
            for s in g.get("sizeOptions", []):
                if s.get("size") and s["size"] not in sizes:
                    sizes.append(s["size"])
        if sizes:
            data["Available Size"] = sizes

        # ---------- RATINGS ----------
        data["average_ratings"] = json_data.get("averageRating")
        data["ratingCount"] = json_data.get("ratingCount")
        data["numberOfReviews"] = json_data.get("numberOfReviews")

        # ---------- SIZE GUIDE ----------
        if json_data.get("sizeGuideId"):
            data.update(get_size_guide(pid, json_data["sizeGuideId"], headers))

        return data

    except Exception as e:
        data["Error"] = str(e)
        return data


# =========================================================
# STREAMLIT UI
# =========================================================
st.title("TataCliq – Exact Python Parity Extractor")

cookie = st.text_area("Paste Cookie Header", height=160)
uploaded = st.file_uploader("Upload Excel / CSV", type=["xlsx", "csv"])

threads = st.number_input("Threads", 1, 20, 8)
retry = st.number_input("Retry Count", 1, 10, 3)

st.button("Stop Extraction", on_click=request_stop)

if uploaded:
    df = pd.read_excel(uploaded) if uploaded.name.endswith("xlsx") else pd.read_csv(uploaded)
    col = st.selectbox("Select URL Column", df.columns)
    urls = df[col].dropna().astype(str).tolist()

    if st.button("Start Extraction", type="primary"):
        headers = build_headers(cookie)
        results = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=int(threads)) as exe:
            for i, r in enumerate(exe.map(lambda u: extract_product(u, headers, retry), urls), 1):
                results.append(r)
                st.progress(i / len(urls))

        out_df = pd.DataFrame(results)
        st.dataframe(out_df.head(50), use_container_width=True)

        out_xlsx = io.BytesIO()
        with pd.ExcelWriter(out_xlsx, engine="xlsxwriter") as w:
            out_df.to_excel(w, index=False)

        st.download_button("Download Excel", out_xlsx.getvalue(), "tatacliq_exact_output.xlsx")
        st.download_button("Download CSV", out_df.to_csv(index=False).encode(), "tatacliq_exact_output.csv")
