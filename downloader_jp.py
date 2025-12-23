# -*- coding: utf-8 -*-
import os, sys, time, random, logging, warnings, subprocess, json
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import pandas as pd
import yfinance as yf

# ====== 自動安裝/匯入必要套件 ======
def ensure_pkg(pkg_install_name, import_name):
    try:
        __import__(import_name)
    except ImportError:
        print(f"🔧 正在安裝 {pkg_install_name}...")
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", pkg_install_name])

ensure_pkg("tokyo-stock-exchange", "tokyo_stock_exchange")
from tokyo_stock_exchange import tse

# ====== 降噪與環境設定 ======
warnings.filterwarnings("ignore")
logging.getLogger("yfinance").setLevel(logging.CRITICAL)

# 路徑定義
MARKET_CODE = "jp-share"
DATA_SUBDIR = "dayK"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data", MARKET_CODE, DATA_SUBDIR)
LIST_DIR = os.path.join(BASE_DIR, "data", MARKET_CODE, "lists")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LIST_DIR, exist_ok=True)

# 狀態管理檔案
MANIFEST_CSV = Path(LIST_DIR) / "jp_manifest.csv"
LIST_ALL_CSV = Path(LIST_DIR) / "jp_list_all.csv"
THREADS = 4 

# 💡 核心新增：數據過期時間 (3600 秒 = 1 小時)
DATA_EXPIRY_SECONDS = 3600

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

def get_tse_list():
    """獲取日股清單：具備門檻檢查與歷史快取備援"""
    threshold = 3800 
    log("📡 正在獲取東京交易所標的清單...")
    try:
        df = pd.read_csv(tse.csv_file_path)
        code_col = next((c for c in ['コード', 'Code', 'code', 'Local Code'] if c in df.columns), None)
        name_col = next((c for c in ['銘柄名', 'Name', 'name', 'Company Name'] if c in df.columns), None)

        if not code_col: raise KeyError("無法定位代碼欄位")

        res = []
        for _, row in df.iterrows():
            code = str(row[code_col]).strip()
            if len(code) >= 4 and code[:4].isdigit():
                res.append({
                    "code": code[:4], 
                    "name": str(row[name_col]) if name_col else code[:4], 
                    "board": "T"
                })
        
        final_df = pd.DataFrame(res).drop_duplicates(subset=['code'])
        
        if len(final_df) < threshold:
            log(f"⚠️ 數量異常 ({len(final_df)})，嘗試讀取歷史快取...")
            if LIST_ALL_CSV.exists(): return pd.read_csv(LIST_ALL_CSV)
        else:
            final_df.to_csv(LIST_ALL_CSV, index=False, encoding='utf-8-sig')
            log(f"✅ 成功獲取 {len(final_df)} 檔日股清單")
        return final_df

    except Exception as e:
        log(f"❌ 清單獲取失敗: {e}")
        return pd.read_csv(LIST_ALL_CSV) if LIST_ALL_CSV.exists() else pd.DataFrame()

def build_manifest(df_list):
    """建立續跑清單，偵測檔案時效性"""
    if MANIFEST_CSV.exists():
        mf = pd.read_csv(MANIFEST_CSV)
        new_codes = df_list[~df_list['code'].astype(str).isin(mf['code'].astype(str))]
        if not new_codes.empty:
            new_codes_df = new_codes.copy()
            new_codes_df['status'] = 'pending'
            mf = pd.concat([mf, new_codes_df], ignore_index=True)
    else:
        mf = df_list.copy()
        mf["status"] = "pending"

    # 💡 智慧時效檢查：若檔案太舊，則標記為 pending 重新抓取
    log("🔍 正在檢查日股數據時效性...")
    for idx, row in mf.iterrows():
        code_str = str(row['code']).zfill(4)
        out_path = os.path.join(DATA_DIR, f"{code_str}.T.csv")
        
        if os.path.exists(out_path):
            file_age = time.time() - os.path.getmtime(out_path)
            if file_age < DATA_EXPIRY_SECONDS and os.path.getsize(out_path) > 1000:
                mf.at[idx, "status"] = "done"
            else:
                mf.at[idx, "status"] = "pending" # 超過一小時，標記重新下載
        else:
            mf.at[idx, "status"] = "pending"

    mf.to_csv(MANIFEST_CSV, index=False)
    return mf

def download_one(row_tuple):
    """強化版下載：加入 3 次重試機制與動態延遲"""
    idx, row = row_tuple
    code = str(row['code']).zfill(4)
    symbol = f"{code}.T"
    out_path = os.path.join(DATA_DIR, f"{code}.T.csv")
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            time.sleep(random.uniform(0.5, 1.2)) 
            tk = yf.Ticker(symbol)
            df_raw = tk.history(period="2y", interval="1d", auto_adjust=True, timeout=20)
            
            if df_raw is not None and not df_raw.empty:
                df_raw.reset_index(inplace=True)
                df_raw.columns = [c.lower() for c in df_raw.columns]
                if 'date' in df_raw.columns:
                    df_raw['date'] = pd.to_datetime(df_raw['date'], utc=True).dt.tz_localize(None)
                
                cols = ['date','open','high','low','close','volume']
                df_final = df_raw[[c for c in cols if c in df_raw.columns]]
                df_final.to_csv(out_path, index=False, encoding='utf-8-sig')
                return idx, "done"
            
            if attempt == max_retries - 1: return idx, "empty"
        except Exception:
            if attempt == max_retries - 1: return idx, "failed"
            time.sleep(random.randint(3, 7))
            
    return idx, "failed"

def main():
    start_time = time.time()
    log("🇯🇵 日本股市同步器啟動 (時效檢查模式)")
    
    df_list = get_tse_list()
    if df_list.empty: 
        log("🚨 無法取得清單，結束程序。")
        return
    mf = build_manifest(df_list)

    todo = mf[~mf["status"].isin(["done", "empty"])]
    
    if not todo.empty:
        log(f"📝 待處理標的數：{len(todo)} 檔 (其餘 {len(mf)-len(todo)} 檔在效期內)")
        with ThreadPoolExecutor(max_workers=THREADS) as executor:
            futures = {executor.submit(download_one, item): item for item in todo.iterrows()}
            pbar = tqdm(total=len(todo), desc="日股下載進度")
            count = 0
            try:
                for f in as_completed(futures):
                    idx, status = f.result()
                    mf.at[idx, "status"] = status
                    count += 1
                    pbar.update(1)
                    if count % 100 == 0:
                        mf.to_csv(MANIFEST_CSV, index=False)
            except KeyboardInterrupt:
                log("🛑 中斷下載...")
            finally:
                mf.to_csv(MANIFEST_CSV, index=False)
                pbar.close()
    else:
        log("✅ 所有日股資料皆在 1 小時內更新過。")

    total_expected = len(mf)
    effective_success = len(mf[mf['status'] == 'done'])
    fail_count = total_expected - effective_success

    download_stats = {
        "total": total_expected,
        "success": effective_success,
        "fail": fail_count
    }

    duration = (time.time() - start_time) / 60
    log("="*30)
    log(f"📊 下載報告: 成功(含效期內)={effective_success}, 耗時={duration:.1f}分鐘")
    log(f"📈 數據完整度: {(effective_success/total_expected)*100:.2f}%")
    log("="*30)

    return download_stats

if __name__ == "__main__":
    main()
