# -*- coding: utf-8 -*-
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from tqdm import tqdm
import matplotlib

matplotlib.use('Agg')

plt.rcParams['font.sans-serif'] = ['Noto Sans CJK TC', 'Noto Sans CJK JP', 'Microsoft JhengHei', 'sans-serif']
plt.rcParams['axes.unicode_minus'] = False

BIN_SIZE = 10.0
X_MIN, X_MAX = -100, 100
BINS = np.arange(X_MIN, X_MAX + 11, BIN_SIZE)

def build_company_list(arr_pct, codes, names, bins):
    lines = [f"{'報酬區間':<12} | {'家數(比例)':<14} | 公司清單", "-"*80]
    total = len(arr_pct)
    clipped_arr = np.clip(arr_pct, -100, 100)
    counts, edges = np.histogram(clipped_arr, bins=bins)

    for i in range(len(edges)-1):
        lo, up = edges[i], edges[i+1]
        lab = f"{int(lo)}%~{int(up)}%"
        mask = (arr_pct >= lo) & (arr_pct < up)
        if i == len(edges) - 2: mask = (arr_pct >= lo) & (arr_pct <= up)
        cnt = int(mask.sum())
        if cnt == 0: continue
        picked_indices = np.where(mask)[0]
        links = [f'<a href="https://www.wantgoo.com/stock/{codes[idx]}/technical-chart" style="text-decoration:none; color:#0366d6;">{codes[idx]}({names[idx]})</a>' for idx in picked_indices]
        lines.append(f"{lab:<12} | {cnt:>4} ({(cnt/total*100):5.1f}%) | {', '.join(links)}")
    return "\n".join(lines)

def run_global_analysis(market_id="tw-share"):
    data_path = Path("./data") / market_id / "dayK"
    image_out_dir = Path("./output/images") / market_id
    image_out_dir.mkdir(parents=True, exist_ok=True)
    all_files = list(data_path.glob("*.csv"))
    if not all_files: return [], pd.DataFrame(), {}

    results = []
    for f in tqdm(all_files, desc="分析數據"):
        try:
            df = pd.read_csv(f)
            if len(df) < 20: continue
            df.columns = [c.lower() for c in df.columns]
            close, high, low = df['close'].values, df['high'].values, df['low'].values
            periods = [('Week', 5), ('Month', 20), ('Year', 250)]
            tkr, nm = f.stem.split('_', 1) if '_' in f.stem else (f.stem, f.stem)
            row = {'Ticker': tkr, 'Full_ID': nm}
            for p_name, days in periods:
                if len(close) <= days: continue
                prev_c = close[-(days+1)]
                if prev_c == 0: continue
                row[f'{p_name}_High'] = (max(high[-days:]) - prev_c) / prev_c * 100
                row[f'{p_name}_Close'] = (close[-1] - prev_c) / prev_c * 100
                row[f'{p_name}_Low'] = (min(low[-days:]) - prev_c) / prev_c * 100
            results.append(row)
        except: continue

    df_res = pd.DataFrame(results)
    images = []
    # 這裡調整顏色與標籤
    color_map = {'High': '#28a745', 'Close': '#007bff', 'Low': '#dc3545'}
    
    for p_n, p_z in [('Week', '週'), ('Month', '月'), ('Year', '年')]:
        for t_n, t_z in [('High', '最高-進攻'), ('Close', '收盤-實質'), ('Low', '最低-防禦')]:
            col = f"{p_n}_{t_n}"
            if col not in df_res.columns: continue
            data = df_res[col].dropna()
            fig, ax = plt.subplots(figsize=(11, 7))
            counts, edges = np.histogram(np.clip(data.values, X_MIN, X_MAX), bins=BINS)
            bars = ax.bar(edges[:-1], counts, width=9, align='edge', color=color_map[t_n], alpha=0.7, edgecolor='white')
            
            # ✅ 修復：加上數據標籤
            max_h = counts.max() if len(counts) > 0 else 1
            for bar in bars:
                h = bar.get_height()
                if h > 0:
                    ax.text(bar.get_x() + 4.5, h + (max_h * 0.01), f'{int(h)}\n({h/len(data)*100:.1f}%)', 
                            ha='center', va='bottom', fontsize=9, fontweight='bold')

            ax.set_ylim(0, max_h * 1.3) # 預留頂部空間
            ax.set_title(f"{p_z}K {t_z} 報酬分布", fontsize=18, fontweight='bold')
            ax.set_xticks(BINS)
            ax.set_xticklabels([f"{int(x)}%" for x in BINS], rotation=45)
            ax.grid(axis='y', linestyle='--', alpha=0.3)
            plt.tight_layout()
            
            img_path = image_out_dir / f"{col.lower()}.png"
            plt.savefig(img_path, dpi=120)
            plt.close()
            images.append({'id': col.lower(), 'path': str(img_path), 'label': f"{p_z}K {t_z}"})

    text_reports = {}
    for p_n in ['Week', 'Month', 'Year']:
        col = f'{p_n}_High'
        if col in df_res.columns:
            text_reports[p_n] = build_company_list(df_res[col].values, df_res['Ticker'].tolist(), df_res['Full_ID'].tolist(), BINS)
    
    return images, df_res, text_reports
