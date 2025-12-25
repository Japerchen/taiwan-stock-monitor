# -*- coding: utf-8 -*-
import os
import time
import random
import requests
import pandas as pd
import yfinance as yf
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from pathlib import Path

# ========== 核心參數設定 ==========
MARKET_CODE = "tw-share"
DATA_SUBDIR = "dayK"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data", MARKET_CODE, DATA_SUBDIR)

# ✅ 效能優化：調低至 3，配合隨機延遲可有效避開 Yahoo 封鎖
MAX_WORKERS = 3 
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

def get_full_stock_list():
    """獲取台股全市場清單 (雙重機制：證交所 JSP + Akshare 備援)"""
    url_configs = [
        {'name': 'listed', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?market=1&issuetype=1&Page=1&chklike=Y', 'suffix': '.TW'},
        {'name': 'dr', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=1&issuetype=J&industry_code=&Page=1&chklike=Y', 'suffix': '.TW'},
        {'name': 'otc', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?market=2&issuetype=4&Page=1&chklike=Y', 'suffix': '.TWO'},
        {'name': 'etf', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=1&issuetype=I&industry_code=&Page=1&chklike=Y', 'suffix': '.TW'},
        {'name': 'rotc', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=E&issuetype=R&industry_code=&Page=1&chklike=Y', 'suffix': '.TWO'},
    ]
    
    all_items = []
    log("📡 [方案 A] 正在從證交所 JSP 獲取清單...")
    
    for cfg in url_configs:
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
            resp = requests.get(cfg['url'], timeout=15, headers=headers)
            df_list = pd.read_html(StringIO(resp.text), header=0)
            if not df_list: continue
            df = df_list[0]
            for _, row in df.iterrows():
                code = str(row['有價證券代號']).strip()
                name = str(row['有價證券名稱']).strip()
                if code and '有價證券' not in code:
                    all_items.append(f"{code}{cfg['suffix']}&{name}")
        except Exception as e:
            continue

    # --- 方案 B: Akshare 備援 (當證交所失敗時) ---
    if len(all_items) < 500:
        log("📡 [方案 B] 證交所資料獲取不足，改用 Akshare 備援...")
        try:
            import akshare as ak
            # 獲取上市與上櫃清單
            df_tw_listed = ak.stock_tw_spot_em() # 台灣市場即時行情
            for _, row in df_tw_listed.iterrows():
                code = str(row['代码'])
                name = str(row['名称'])
                # Akshare 的代號通常需要判斷 .TW 或 .TWO
                # 這裡簡單處理：如果是上市公司通常是 .TW，其餘 .TWO
                suffix = ".TW" if len(code) == 4 and code.startswith(('2', '1', '3')) else ".TWO"
                all_items.append(f"{code}{suffix}&{name}")
        except Exception as e:
            log(f"❌ 備援方案亦失敗: {e}")

    final_res = list(set(all_items))
    log(f"✅ 台股清單獲取完成，共 {len(final_res)} 檔標的。")
    return final_res

def download_stock_data(item):
    """具備隨機延遲與自動重試的下載邏輯"""
    yf_tkr = "ParseError"
    try:
        parts = item.split('&', 1)
        if len(parts) < 2: return {"status": "error", "tkr": item}
        
        yf_tkr, name = parts
        safe_name = "".join([c for c in name if c.isalnum() or c in (' ', '_', '-')]).strip()
        out_path = os.path.join(DATA_DIR, f"{yf_tkr}_{safe_name}.csv")
        
        # 今日快取檢查
        if os.path.exists(out_path):
            mtime = datetime.fromtimestamp(os.path.getmtime(out_path)).date()
            if mtime == datetime.now().date() and os.path.getsize(out_path) > 1000:
                return {"status": "exists", "tkr": yf_tkr}

        time.sleep(random.uniform(0.5, 1.2))
        tk = yf.Ticker(yf_tkr)
        
        for attempt in range(2):
            try:
                hist = tk.history(period="2y", timeout=15)
                if hist is not None and not hist.empty:
                    hist.reset_index(inplace=True)
                    hist.columns = [c.lower() for c in hist.columns]
                    hist.to_csv(out_path, index=False, encoding='utf-8-sig')
                    return {"status": "success", "tkr": yf_tkr}
                if attempt == 1: return {"status": "empty", "tkr": yf_tkr}
            except:
                time.sleep(random.uniform(3, 7))

        return {"status": "empty", "tkr": yf_tkr}
    except:
        return {"status": "error", "tkr": yf_tkr}

from datetime import datetime

def main():
    items = get_full_stock_list()
    if not items:
        return {"total": 0, "success": 0, "fail": 0}
        
    log(f"🚀 啟動台股下載任務，目標總數: {len(items)}")
    
    stats = {"success": 0, "exists": 0, "empty": 0, "error": 0}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_stock_data, it): it for it in items}
        pbar = tqdm(total=len(items), desc="台股下載")
        
        for future in as_completed(futures):
            res = future.result()
            stats[res["status"]] += 1
            pbar.update(1)
            
            if pbar.n % 100 == 0:
                time.sleep(random.uniform(5, 10))
        pbar.close()
    
    # ✨ 重要：構建回傳給 main.py 的統計字典
    report_stats = {
        "total": len(items),
        "success": stats["success"] + stats["exists"],
        "fail": stats["error"] + stats["empty"]
    }
    
    print("\n" + "="*50)
    log(f"📊 台股下載完成報告: {report_stats}")
    print("="*50 + "\n")
    
    return report_stats # 👈 必須 Return 給 main.py

if __name__ == "__main__":
    main()
