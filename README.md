# TataCliq Product Crawler (Streamlit)

## Features
- Upload **Excel or CSV**
- URL validation + TataCliq domain check
- URL **dedupe** before crawling
- Multi-threading using ThreadPoolExecutor
- Retry logic per request
- **Stop Crawl** button
- Download extracted output as Excel or CSV

## Run locally
```bash
pip install -r requirements.txt
streamlit run app.py
```

## Notes
- Cookie header is optional. If TataCliq blocks requests, paste cookie into the app.
- Higher thread count may cause blocking / rate-limits; start with 4–6.
