import json
import requests
import concurrent.futures
import pandas as pd
from bs4 import BeautifulSoup
from collections import OrderedDict
from copy import deepcopy
from html import unescape
import time
import random


# =========================================================
# CONFIG
# =========================================================
INPUT_EXCEL = "tatacliq.xlsx"
INPUT_SHEET = "Sheet1"
INPUT_COLUMN = "url"
OUTPUT_EXCEL = "tatacliq_full_output.xlsx"
MAX_WORKERS = 8
RETRY_COUNT = 3


# =========================================================
# HEADERS (COOKIE REQUIRED)
# =========================================================
HEADERS = {
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
    # 🔴 IMPORTANT: paste your cookie here
    "cookie": "PASTE_YOUR_COOKIE_HERE"
}


# =========================================================
# HELPERS
# =========================================================
def safe_get(url, headers, params=None, retry=3, timeout=25):
    for i in range(retry):
        try:
            return requests.get(url, headers=headers, params=params, timeout=timeout)
        except:
            time.sleep(0.6 * (i + 1))
    return None


def clean_html(text):
    return BeautifulSoup(unescape(str(text)), "html.parser").get_text(" ", strip=True)


def format_size_header(raw_dim, unit=None):
    if unit:
        unit = "Inches" if unit.lower() == "in" else unit
        return f"{raw_dim} ( {unit} )"
    return raw_dim


# =========================================================
# SIZE GUIDE
# =========================================================
def get_size_guide(product_id, sizeGuideId):
    res = safe_get(
        f"https://www.tatacliq.com/marketplacewebservices/v2/mpl/products/{product_id}/sizeGuideChart",
        headers=HEADERS,
        params={
            "isPwa": "true",
            "sizeGuideId": sizeGuideId,
            "rootCategory": "Clothing"
        }
    )

    if not res:
        return OrderedDict()

    try:
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

            for d in size_name.get("dimensionList", []):
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
# MAIN CRAWLER
# =========================================================
def crawl_product(url):
    data = {"Input": url}

    try:
        pid = url.split("/p-")[-1]
        pid_u = pid.upper()

        res = safe_get(
            f"https://www.tatacliq.com/marketplacewebservices/v2/mpl/products/productDetails/{pid_u}",
            headers=HEADERS,
            params={"isPwa": "true", "isMDE": "true", "isDynamicVar": "true"}
        )

        if not res:
            return data

        json_data = res.json()

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
        img_count = 1
        for g in json_data.get("galleryImagesList", []):
            for k in g.get("galleryImages", []):
                if k.get("key") == "superZoom":
                    data[f"image_{img_count}"] = "https:" + k.get("value")
                    img_count += 1

        # ---------------- Details
        for d in json_data.get("details", []):
            if d.get("key"):
                data[d["key"]] = d.get("value")

        # ---------------- specificationGroup (UI Overview)
        for g in json_data.get("specificationGroup", []):
            for s in g.get("specifications", []):
                if s.get("key"):
                    data[s["key"]] = s.get("value")

        # ---------------- detailsSection
        for i in json_data.get("detailsSection", []):
            if i.get("key"):
                data[i["key"]] = i.get("value")

        # ---------------- classificationList
        for sec in json_data.get("classificationList", []):
            key = sec.get("key")
            val = sec.get("value", {})
            if "classificationList" in val:
                for i in val["classificationList"]:
                    data[i["key"]] = i["value"]
            elif "classificationValues" in val:
                data[key] = ", ".join(val["classificationValues"])

        # ---------------- knowMore (features)
        for i, k in enumerate(json_data.get("knowMore", []), 1):
            data[f"Feature_{i}"] = k.get("knowMoreItem")

        # ---------------- setInformation
        if json_data.get("setInformation"):
            for i in json_data["setInformation"].get("values", []):
                data[i["key"]] = i["value"]

        # ---------------- whatElseYouNeedtoKnow
        for i in json_data.get("whatElseYouNeedtoKnow", []):
            data[i["key"]] = i["value"]

        # ---------------- ingredientDetails
        for i in json_data.get("ingredientDetails", []):
            data[i["key"]] = ", ".join(v["key"] for v in i.get("values", []))

        # ---------------- primaryIngredients
        for i in json_data.get("primaryIngredients", []):
            data[i["key"]] = i["value"]

        # ---------------- shortStorySmall
        feats = sorted(json_data.get("shortStorySmall", []), key=lambda x: x.get("order", 0))
        if feats:
            data["additional_features"] = ", ".join(i["key"] for i in feats if i.get("key"))

        # ---------------- Ratings
        data["averageRating"] = json_data.get("averageRating")
        data["ratingCount"] = json_data.get("ratingCount")
        data["numberOfReviews"] = json_data.get("numberOfReviews")

        # ---------------- Customer Voice
        cv = safe_get(
            f"https://www.tatacliq.com/marketplacewebservices/v2/mpl/products/{pid_u}/customerVoice",
            headers=HEADERS
        )
        if cv:
            for i in cv.json().get("customerVoiceData", []):
                data[i["text"]] = i["value"]

        # ---------------- Manufacturer / Packer
        try:
            brand = json_data.get("brandURL", "").split("c-")[-1]
            cat = json_data["categoryHierarchy"][-1]["category_id"]

            mfg = safe_get(
                "https://www.tatacliq.com/marketplacewebservices/v2/mpl/products/manufacturingdetails",
                headers=HEADERS,
                params={"brand": brand.upper(), "category": cat.upper()}
            )

            mj = mfg.json()
            if mj.get("manufacturer"):
                data["manufacturer"] = mj["manufacturer"][0]["value"]
            if mj.get("packer"):
                data["packer"] = mj["packer"][0]["value"]
        except:
            pass

        # ---------------- Size Guide
        if json_data.get("sizeGuideId"):
            data.update(get_size_guide(pid_u, json_data["sizeGuideId"]))

        # ---------------- A+ Content
        count = 1
        for i in json_data.get("APlusContent", {}).get("productContent", []):
            txt = i.get("value", {}).get("textList")
            if txt:
                data[f"APlus_Content_{count}"] = " ".join(clean_html(t) for t in txt)
                count += 1

        return data

    except Exception as e:
        data["Error"] = str(e)
        return data


# =========================================================
# EXECUTION
# =========================================================
df = pd.read_excel(INPUT_EXCEL, INPUT_SHEET)
urls = df[INPUT_COLUMN].dropna().tolist()

all_data = []

with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
    for res in exe.map(crawl_product, urls):
        all_data.append(deepcopy(res))

pd.DataFrame(all_data).to_excel(OUTPUT_EXCEL, index=False)
print("DONE:", OUTPUT_EXCEL)
