# -*- coding: utf-8 -*-
import os
import time
import argparse
import traceback
from datetime import datetime, timedelta

# --- 導入模組 ---
# 僅保留台灣下載器與核心分析模組
import downloader_tw
import analyzer
import notifier

def run_market_pipeline(market_id, market_name, emoji):
    """
    執行單一市場的完整管線：下載 -> 分析 -> 寄信
    """
    print("\n" + "="*60)
    print(f"{emoji} 啟動管線：{market_name} ({market_id})")
    print("="*60)

    # 初始化統計變數，預設為 0
    stats = {"total": 0, "success": 0, "fail": 0}
    
    # 建立通知器實例 (用於發送 Telegram 與 Resend 郵件)
    agent = notifier.StockNotifier()

    # --- Step 1: 數據獲取 ---
    print(f"【Step 1: 數據獲取】正在更新 {market_name} 原始 K 線資料...")
    try:
        res = None
        # 僅執行台灣股市下載
        if market_id == "tw-share":
            res = downloader_tw.main()
        else:
            print(f"⚠️ 未知的市場 ID: {market_id}")
            return

        # ✨ 數據標準化
        if isinstance(res, dict):
            stats = res
            print(f"📊 [下載報告] 總計: {stats.get('total', 0)} | 成功: {stats.get('success', 0)} | 失敗: {stats.get('fail', 0)}")
        elif res is not None and hasattr(res, '__len__'):
            # 相容舊版回傳 List 的格式
            stats = {"total": len(res), "success": len(res), "fail": 0}
            print(f"📊 [下載報告] 已獲取 {len(res)} 檔標的。")
        else:
            print(f"⚠️ {market_name} 下載器未回傳有效數據，報告可能顯示為 0。")

    except Exception as e:
        print(f"❌ {market_name} 數據下載過程發生嚴重異常: {e}")

    # --- Step 2: 數據分析 & 繪圖 ---
    print(f"\n【Step 2: 矩陣分析】正在計算 {market_name} 動能分布並生成圖表...")
    try:
        # 呼叫分析核心
        img_paths, report_df, text_reports = analyzer.run_global_analysis(market_id=market_id)
        
        if report_df is None or report_df.empty:
            print(f"⚠️ {market_name} 分析結果為空 (可能是 CSV 資料不足)，跳過寄信步驟。")
            return
        
        print(f"✅ 分析完成！成功處理 {len(report_df)} 檔有效數據。")

        # --- Step 3: 報表發送 ---
        print(f"\n【Step 3: 報表發送】正在透過 Resend 傳送郵件...")
        
        # 將下載統計 (stats) 與分析結果一併送出
        success_sent = agent.send_stock_report(
            market_name=market_name,
            img_data=img_paths,
            report_df=report_df,
            text_reports=text_reports,
            stats=stats
        )
        
        if success_sent:
            print(f"✅ {market_name} 監控報告已成功寄達！")
        else:
            print(f"❌ {market_name} 報告寄送失敗 (請檢查 API Key 或日誌)。")

    except Exception as e:
        print(f"❌ {market_name} 分析或寄信過程出錯:\n{traceback.format_exc()}")

def main():
    parser = argparse.ArgumentParser(description="Global Stock Monitor Orchestrator")
    # 將預設值改為 tw-share，並簡化選項
    parser.add_argument('--market', type=str, default='tw-share', 
                        choices=['tw-share'], help="僅支援台灣股市")
    args = parser.parse_args()

    start_time = time.time()
    
    # 獲取台北時間 (UTC+8) 供 Log 記錄
    now_utc8 = datetime.utcnow() + timedelta(hours=8)
    now_str = now_utc8.strftime("%Y-%m-%d %H:%M:%S")
    
    print("\n" + "🚀 " + "="*55)
    print(f"🚀 股市監控系統啟動 (台灣限定版)")
    print(f"🚀 啟動時間: {now_str} (UTC+8)")
    print("🚀 " + "="*55 + "\n")

    # 鎖定市場配置：只保留台灣
    markets_config = {
        "tw-share": {"name": "台灣股市", "emoji": "🇹🇼"}
    }

    # 直接執行台灣股市流程
    target_market = "tw-share"
    m_info = markets_config[target_market]
    
    run_market_pipeline(target_market, m_info["name"], m_info["emoji"])

    end_time = time.time()
    total_duration = (end_time - start_time) / 60
    print("\n" + "="*60)
    print(f"🎉 任務執行完畢！總耗時: {total_duration:.2f} 分鐘")
    print("="*60 + "\n")

if __name__ == "__main__":
    main()
