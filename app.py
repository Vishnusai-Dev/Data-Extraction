import json
import requests
import concurrent.futures
import pandas as pd
import streamlit as st
from bs4 import BeautifulSoup
from collections import OrderedDict
from copy import deepcopy
from html import unescape
import io
import threading

# =========================================================
# Streamlit setup
# =========================================================
st.set_page_config(page_title="TataCliq – Full Parity Extractor", layout="wide")

if "all_data" not in st.session_state:
    st.session_state.all_data = []

if "running" not in st.session_state:
    st.session_state.running = False

stop_event = threading.Event()

def request_stop():
    stop_event.set()

# =========================================================
# Headers (UNCHANGED)
# =========================================================
headers = {
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

cookie = st.text_area("Paste FULL cookie header", height=160)
if cookie:
    headers["cookie"] = cookie

# =========================================================
# SIZE GUIDE (UNCHANGED)
# =========================================================
def format_size_header(raw_dim, unit=None):
    if unit:
        unit = "Inches" if unit.lower() == "in" else unit
        return f"{raw_dim} ( {unit} )"
    return raw_dim


def get_size_guide(ID_from_input, sizeGuideId, headers):
    params = {
        'isPwa': 'true',
        'sizeGuideId': sizeGuideId,
        'rootCategory': 'Clothing',
    }

    response = requests.get(
        f'https://www.tatacliq.com/marketplacewebservices/v2/mpl/products/{ID_from_input.upper()}/sizeGuideChart',
        params=params,
        headers=headers,
        timeout=10
    )

    try:
        json_size = response.json()
    except:
        return OrderedDict()

    unit_data = OrderedDict()
    main_size = []

    for size_map in json_size.get('sizeGuideTabularWsData', {}).get('unitList', []):
        unit = size_map.get('displaytext')
        unit_data.setdefault(unit, OrderedDict())

        for size_name in size_map.get('sizeGuideList', []):
            size = size_name.get('dimensionSize')
            if size and size not in main_size:
                main_size.append(size)

            for size_value in size_name.get('dimensionList', []):
                dim = size_value.get('dimension')
                val = size_value.get('dimensionValue')
                if dim and val is not None:
                    unit_data[unit].setdefault(dim, []).append(val)

    final_output = OrderedDict()
    if main_size:
        final_output["Brand Size"] = main_size

    dims = set(unit_data.get('Cm', {})) | set(unit_data.get('In', {}))
    for dim in dims:
        cm_vals = unit_data.get('Cm', {}).get(dim)
        in_vals = unit_data.get('In', {}).get(dim)

        if cm_vals == in_vals:
            final_output[format_size_header(dim)] = cm_vals
        else:
            if cm_vals:
                final_output[format_size_header(dim, "Cm")] = cm_vals
            if in_vals:
                final_output[format_size_header(dim, "In")] = in_vals

    if json_size.get('imageURL'):
        final_output['measurement_image'] = json_size['imageURL']

    return final_output

# =========================================================
# 🔴 ORIGINAL get_data() – LOGIC UNCHANGED
# =========================================================
def get_data(data):
    try:
        ID_from_input = data["url"].split("/p-")[-1]
        newid = ID_from_input.upper()

        product_url = f"https://www.tatacliq.com/marketplacewebservices/v2/mpl/products/productDetails/{newid}?isPwa=true&isMDE=true&isDynamicVar=true"
        res = requests.get(product_url, headers=headers)

        try:
            json_data = res.json()
        except:
            return None

        nettemp = {}
        variant_found = False

        if json_data.get("variantOptions"):
            for i in json_data["variantOptions"]:
                sizelink = i.get("sizelink")
                colorlink = i.get("colorlink")
                if sizelink and sizelink.get("productCode") == newid:
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
            data["product_code"] = ID_from_input

        data["productTitle"] = json_data.get("productTitle")
        data["brandName"] = json_data.get("brandName")
        data["productColor"] = json_data.get("productColor")
        data["productDescription"] = json_data.get("productDescription")
        data["productListingId"] = json_data.get("productListingId")
        data["rootCategory"] = json_data.get("rootCategory")
        data["styleNote"] = json_data.get("styleNote")

        nettemp["usisd"] = json_data.get("winningUssID")
        nettemp["brandURL"] = json_data.get("brandURL", "").split("c-")[-1].upper()
        nettemp["categoryL4Code"] = json_data.get("categoryL4Code")

        if json_data.get("categoryHierarchy"):
            for i, cat in enumerate(json_data["categoryHierarchy"]):
                data[f"Breadcrums_{i+1}"] = cat["category_name"]

        if json_data.get("mrpPrice"):
            data["MRP"] = json_data["mrpPrice"]["value"]
        if json_data.get("winningSellerPrice"):
            data["Price"] = json_data["winningSellerPrice"]["value"]
        if json_data.get("discount"):
            data["Discount"] = json_data["discount"]

        if json_data.get("details"):
            for i in json_data["details"]:
                data[i["key"]] = i["value"]

        if json_data.get("galleryImagesList"):
            img = []
            for g in json_data["galleryImagesList"]:
                for k in g["galleryImages"]:
                    if k["key"] == "superZoom":
                        img.append("https:" + k["value"])

            for i, im in enumerate(img):
                data[f"image_{i+1}"] = im

        if json_data.get("mfgDetails"):
            for k, v in json_data["mfgDetails"].items():
                if isinstance(v, list):
                    data[k] = v[0]["value"]
                else:
                    data[k] = v

        if json_data.get("classifications"):
            for classification in json_data.get("classifications", []):
                for spec in classification.get("specifications", []):
                    if spec.get("key") and spec.get("value"):
                        data[spec["key"]] = spec["value"]

        if json_data.get('winningSellerName'):
            data['Seller_name'] = json_data.get('winningSellerName')

        if json_data.get('winningSellerAddress'):
            data['Seller_address'] = json_data.get('winningSellerAddress')

        if json_data.get("sizeGuideId"):
            data.update(get_size_guide(ID_from_input, json_data["sizeGuideId"], headers))

        data.update(nettemp)
        return deepcopy(data)

    except Exception as e:
        data["Error"] = str(e)
        return deepcopy(data)

# =========================================================
# UI
# =========================================================
st.title("TataCliq – Full Parity Extractor")

uploaded = st.file_uploader("Upload Excel with `url` column", type=["xlsx", "csv"])

threads = st.number_input("Threads", 1, 10, 5)
st.button("Stop Extraction", on_click=request_stop)

if uploaded:
    df = pd.read_excel(uploaded) if uploaded.name.endswith("xlsx") else pd.read_csv(uploaded)
    st.dataframe(df.head())

    if st.button("Start Extraction", type="primary"):
        st.session_state.all_data = []
        st.session_state.running = True

        with concurrent.futures.ThreadPoolExecutor(max_workers=int(threads)) as exe:
            futures = list(exe.map(get_data, df.to_dict("records")))
            for r in futures:
                if r:
                    st.session_state.all_data.append(r)

        st.session_state.running = False
        st.success(f"Completed: {len(st.session_state.all_data)} rows")

# =========================================================
# DOWNLOAD (GUARANTEED)
# =========================================================
if st.session_state.all_data:
    out_df = pd.DataFrame(st.session_state.all_data)
    st.dataframe(out_df.head(50), use_container_width=True)

    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        out_df.to_excel(writer, index=False)

    st.download_button(
        "⬇️ Download Excel",
        buffer.getvalue(),
        file_name="tatacliq_full_parity_output.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
