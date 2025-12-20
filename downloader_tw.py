# -*- coding: utf-8 -*-
import os
import time
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

# 由於不跑權證，家數約在 2,500 檔上下，執行緒設為 5 較穩定
MAX_WORKERS = 5 
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

def get_full_stock_list():
    """
    抓取台股全市場標的（已排除權證）
    包含：上市、上櫃、興櫃、DR、ETF、創新板 A/C
    """
    url_configs = [
        {'name': 'listed', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?market=1&issuetype=1&Page=1&chklike=Y', 'suffix': '.TW'},
        {'name': 'dr', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=1&issuetype=J&industry_code=&Page=1&chklike=Y', 'suffix': '.TW'},
        {'name': 'otc', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?market=2&issuetype=4&Page=1&chklike=Y', 'suffix': '.TWO'},
        {'name': 'etf', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=1&issuetype=I&industry_code=&Page=1&chklike=Y', 'suffix': '.TW'},
        {'name': 'rotc', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=E&issuetype=R&industry_code=&Page=1&chklike=Y', 'suffix': '.TWO'},
        # ✅ 創新板 C 市場（TW）
        {'name': 'tw_innovation', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=C&issuetype=C&industry_code=&Page=1&chklike=Y', 'suffix': '.TW'},
        # ✅ 創新板 A 市場（TWO）
        {'name': 'otc_innovation', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?owncode=&stockname=&isincode=&market=A&issuetype=C&industry_code=&Page=1&chklike=Y', 'suffix': '.TWO'},
    ]

    all_items = []
    log("📡 正在獲取全市場清單 (排除權證，包含創新板與興櫃)...")
    
    for cfg in url_configs:
        try:
            resp = requests.get(cfg['url'], timeout=15)
            df_list = pd.read_html(StringIO(resp.text), header=0)
            if not df_list: continue
            df = df_list[0]
            
            for _, row in df.iterrows():
                code = str(row['有價證券代號']).strip()
                name = str(row['有價證券名稱']).strip()
                
                # 排除 HTML 表格中的標題行
                if code and '有價證券' not in code:
                    full_tkr = f"{code}{cfg['suffix']}"
                    all_items.append(f"{full_tkr}&{name}")
        except Exception as e:
            print(f"⚠️ 無法抓取 {cfg['name']}: {e}")
            
    final_list = list(set(all_items))
    log(f"✅ 清單獲取完畢：共計 {len(final_list)} 檔標的")
    return final_list

def download_stock_data(item):
    try:
        yf_tkr, name = item.split('&')
        # 移除檔名非法字元
        safe_name = name.replace('/', '_').replace('*', '_').replace('\\', '_').replace(':', '_')
        out_path = os.path.join(DATA_DIR, f"{yf_tkr}_{safe_name}.csv")
        
        # 檢查檔案是否已存在（若已下載過則跳過，加快更新速度）
        if os.path.exists(out_path) and os.path.getsize(out_path) > 1000:
            return True

        # 下載 2 年數據
        tk = yf.Ticker(yf_tkr)
        hist = tk.history(period="2y")
        
        if hist is not None and not hist.empty:
            hist.reset_index(inplace=True)
            hist.columns = [c.lower() for c in hist.columns]
            hist.to_csv(out_path, index=False, encoding='utf-8-sig')
            return True
        return False
    except:
        return False

def main():
    items = get_full_stock_list()
    
    log(f"🚀 開始下載數據，目標家數: {len(items)}")
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_stock_data, it): it for it in items}
        pbar = tqdm(total=len(items), desc="下載進度")
        success = 0
        for future in as_completed(futures):
            if future.result():
                success += 1
            pbar.update(1)
        pbar.close()
    
    log(f"📊 執行完畢！成功率: {success}/{len(items)}")

if __name__ == "__main__":
    main()
