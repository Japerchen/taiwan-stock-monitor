# -*- coding: utf-8 -*-
import os, time, random, json, subprocess
import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ========== 參數與路徑 ==========
MARKET_CODE = "cn-share"
DATA_SUBDIR = "dayK"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data", MARKET_CODE, DATA_SUBDIR)
CACHE_LIST_PATH = os.path.join(BASE_DIR, "cn_stock_list_cache.json")

THREADS_CN = 4
os.makedirs(DATA_DIR, exist_ok=True)

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

def get_cn_list():
    """使用 akshare 獲取 A 股清單，具備今日快取機制"""
    if os.path.exists(CACHE_LIST_PATH):
        file_mtime = os.path.getmtime(CACHE_LIST_PATH)
        if datetime.fromtimestamp(file_mtime).date() == datetime.now().date():
            log("📦 載入今日 A 股清單快取...")
            with open(CACHE_LIST_PATH, "r", encoding="utf-8") as f:
                return json.load(f)

    log("📡 正在從 akshare 獲取最新 A 股清單...")
    try:
        import akshare as ak
        df = ak.stock_info_a_code_name()
        valid_prefixes = ('000','001','002','003','300','301','302','600','601','603','605','688','689')
        df = df[df['code'].astype(str).str.startswith(valid_prefixes)]
        res = [f"{row['code']}&{row['name']}" for _, row in df.iterrows()]
        
        with open(CACHE_LIST_PATH, "w", encoding="utf-8") as f:
            json.dump(res, f, ensure_ascii=False)
        return res
    except Exception as e:
        log(f"❌ 獲取清單失敗: {e}")
        return ["600519&貴州茅台", "000001&平安銀行"]

def download_one(item):
    try:
        code, name = item.split('&', 1)
        # Yahoo Finance 格式：6開頭.SS, 其餘.SZ
        symbol = f"{code}.SS" if code.startswith('6') else f"{code}.SZ"
        out_path = os.path.join(DATA_DIR, f"{code}_{name}.csv")

        if os.path.exists(out_path) and os.path.getsize(out_path) > 1000:
            return {"status": "exists", "code": code}

        time.sleep(random.uniform(0.5, 1.5))
        tk = yf.Ticker(symbol)
        hist = tk.history(period="2y", timeout=20)
        
        if hist is not None and not hist.empty:
            hist.reset_index(inplace=True)
            hist.columns = [c.lower() for c in hist.columns]
            hist.to_csv(out_path, index=False, encoding='utf-8-sig')
            return {"status": "success", "code": code}
        return {"status": "empty", "code": code}
    except:
        return {"status": "error", "code": item.split('&')[0]}

def main():
    items = get_cn_list()
    log(f"🚀 開始下載中國 A 股 (共 {len(items)} 檔)")
    stats = {"success": 0, "exists": 0, "empty": 0, "error": 0}
    
    with ThreadPoolExecutor(max_workers=THREADS_CN) as executor:
        futs = {executor.submit(download_one, it): it for it in items}
        pbar = tqdm(total=len(items), desc="CN 下載")
        for f in as_completed(futs):
            res = f.result()
            stats[res.get("status", "error")] += 1
            pbar.update(1)
        pbar.close()
    log(f"📊 A 股報告: 成功={stats['success']}, 跳過={stats['exists']}, 失敗={stats['error']}")

if __name__ == "__main__":
    main()
