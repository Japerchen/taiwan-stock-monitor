# -*- coding: utf-8 -*-
import os, io, re, time, random, json
import pandas as pd
import yfinance as yf
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from pathlib import Path

# ========== 核心參數與路徑 ==========
MARKET_CODE = "hk-share"
DATA_SUBDIR = "dayK"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data", MARKET_CODE, DATA_SUBDIR)
LIST_DIR = os.path.join(BASE_DIR, "data", MARKET_CODE, "lists")
CACHE_LIST_PATH = os.path.join(LIST_DIR, "hk_stock_list_cache.json")

MAX_WORKERS = 4 # 保持保守以防被限流
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LIST_DIR, exist_ok=True)

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

# ========== 工具：格式轉換 ==========
def normalize_code5(s: str) -> str:
    """確保為 5 位數補零格式 (用於檔名)"""
    digits = re.sub(r"\D", "", str(s or ""))
    return digits[-5:].zfill(5) if digits else ""

def to_symbol_yf(code: str) -> str:
    """轉換為 Yahoo Finance 格式 (4 位數.HK)"""
    digits = re.sub(r"\D", "", str(code or ""))
    return f"{digits[-4:].zfill(4)}.HK"

def classify_security(name: str) -> str:
    """過濾衍生品與非普通股"""
    n = str(name).upper()
    bad_kw = ["CBBC", "WARRANT", "RIGHTS", "ETF", "ETN", "REIT", "BOND", "TRUST", "FUND", "牛熊", "權證", "輪證"]
    if any(kw in n for kw in bad_kw):
        return "Exclude"
    return "Common Stock"

def get_full_stock_list():
    """
    獲取港股清單，具備門檻防呆與多次重試機制
    """
    threshold = 2000 # 港股普通股應在 2500 檔左右
    max_retries = 3
    
    # 1. 檢查今日快取
    if os.path.exists(CACHE_LIST_PATH):
        try:
            file_mtime = os.path.getmtime(CACHE_LIST_PATH)
            if datetime.fromtimestamp(file_mtime).date() == datetime.now().date():
                with open(CACHE_LIST_PATH, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if len(data) >= threshold:
                        log(f"📦 載入今日港股快取 ({len(data)} 檔)...")
                        return data
        except: pass

    # 2. 網路獲取重試迴圈
    log("📡 正在從 HKEX 獲取證券名單...")
    url = "https://www.hkex.com.hk/-/media/HKEX-Market/Services/Trading/Securities/Securities-Lists/Securities-Using-Standard-Transfer-Form-(including-GEM)-By-Stock-Code-Order/secstkorder.xls"
    
    for i in range(max_retries):
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            df_raw = pd.read_excel(io.BytesIO(r.content), header=None)
            
            # 定位表頭
            hdr_idx = 0
            for row_i in range(20):
                row_str = "".join([str(x) for x in df_raw.iloc[row_i]]).lower()
                if "stock code" in row_str and "short name" in row_str:
                    hdr_idx = row_i
                    break
            
            df = df_raw.iloc[hdr_idx+1:].copy()
            df.columns = df_raw.iloc[hdr_idx].tolist()
            
            col_code = [c for c in df.columns if "Stock Code" in str(c)][0]
            col_name = [c for c in df.columns if "Short Name" in str(c)][0]
            
            res = []
            for _, row in df.iterrows():
                name = str(row[col_name])
                if classify_security(name) == "Common Stock":
                    code5 = normalize_code5(row[col_code])
                    if code5:
                        res.append(f"{code5}&{name}")
            
            final_list = list(set(res))
            
            if len(final_list) >= threshold:
                with open(CACHE_LIST_PATH, "w", encoding="utf-8") as f:
                    json.dump(final_list, f, ensure_ascii=False)
                log(f"✅ 成功獲取港股清單: {len(final_list)} 檔")
                return final_list
            else:
                log(f"⚠️ 數量不足 ({len(final_list)} 檔)，準備重試...")
        except Exception as e:
            log(f"❌ 嘗試失敗: {e}")
        
        if i < max_retries - 1:
            time.sleep(5)

    # 3. 歷史快取備援
    if os.path.exists(CACHE_LIST_PATH):
        log("🔄 使用歷史快取備援...")
        with open(CACHE_LIST_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
            
    return []

def download_stock_data(item):
    """
    單檔下載：加入隨機延遲
    """
    try:
        code5, name = item.split('&', 1)
        yf_sym = to_symbol_yf(code5)
        out_path = os.path.join(DATA_DIR, f"{code5}.HK.csv")
        
        if os.path.exists(out_path) and os.path.getsize(out_path) > 1000:
            return {"status": "exists", "tkr": code5}

        # 🚀 關鍵隨機等待
        time.sleep(random.uniform(0.5, 1.2))
        
        tk = yf.Ticker(yf_sym)
        hist = tk.history(period="2y", timeout=20)
        
        if hist is not None and not hist.empty:
            hist.reset_index(inplace=True)
            hist.columns = [c.lower() for c in hist.columns]
            if 'date' in hist.columns:
                hist['date'] = pd.to_datetime(hist['date'], utc=True).dt.tz_localize(None)
            
            hist.to_csv(out_path, index=False, encoding='utf-8-sig')
            return {"status": "success", "tkr": code5}
            
        return {"status": "empty", "tkr": code5}
    except:
        return {"status": "error"}

def main():
    items = get_full_stock_list()
    if not items:
        log("❌ 無法取得港股清單，終止執行。")
        return
    
    log(f"🚀 開始港股下載任務 (共 {len(items)} 檔)")
    stats = {"success": 0, "exists": 0, "empty": 0, "error": 0}
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_stock_data, it): it for it in items}
        pbar = tqdm(total=len(items), desc="港股下載進度")
        
        for future in as_completed(futures):
            res = future.result()
            stats[res.get("status", "error")] += 1
            pbar.update(1)
            
            # 每成功 100 檔額外休息，避免長時間佔用連線
            if stats["success"] > 0 and stats["success"] % 100 == 0:
                time.sleep(random.uniform(2, 5))
                
        pbar.close()
    
    log(f"📊 港股下載報告: 成功={stats['success']}, 跳過={stats['exists']}, 失敗={stats['error']}")

if __name__ == "__main__":
    main()
