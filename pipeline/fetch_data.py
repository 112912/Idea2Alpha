import os
import requests
import pandas as pd
from datetime import datetime

# 1. 設定 FinMind API Token（從環境變數讀取）
FINMIND_TOKEN = os.getenv("FINMIND_TOKEN", "")
BASE_URL = "[suspicious link removed]"

# 2. 定義關卡二要求的 9 大類數據對應的 FinMind Dataset 名稱
DATASET_MAP = {
    "daily_k": "TaiwanStockPrice",                       # 1. 日 K 線
    "per_pbr_yield": "TaiwanStockPER",                   # 2. 本益比/淨值比/殖利率
    "institutional": "TaiwanStockInstitutionalInvestorsBuySell", # 3. 三大法人
    "margin_short": "TaiwanStockMarginPurchaseShortSale",# 4. 融資融券
    "monthly_revenue": "TaiwanStockMonthRevenue",        # 5. 月營收
    "financial_statements": "TaiwanStockFinancialStatements", # 6. 財報三表
    "dividends": "TaiwanStockDividend",                  # 7. 股利政策
    "shareholding": "TaiwanStockHoldingSharesPer",       # 8. 集保股權分散
    "futures_position": "TaiwanFuturesInstitutionalInvestors" # 9. 期貨法人多空比
}

DATA_DIR = "pipeline/data"
os.makedirs(DATA_DIR, exist_ok=True)

def fetch_finmind_data(dataset_name: str, start_date: str = "2019-01-01", stock_id: str = None):
    """通用 FinMind 資料抓取函式"""
    params = {
        "dataset": dataset_name,
        "start_date": start_date,
        "token": FINMIND_TOKEN
    }
    if stock_id:
        params["data_id"] = stock_id

    response = requests.get(BASE_URL, params=params)
    if response.status_code == 200:
        data = response.json().get("data", [])
        return pd.DataFrame(data)
    else:
        print(f"⚠️ 抓取 {dataset_name} 失敗：{response.status_code}")
        return pd.DataFrame()

def save_to_parquet(df: pd.DataFrame, file_name: str):
    """將資料存成 Parquet，若檔案已存在則進行增量追加 (Incremental Update)"""
    file_path = os.path.join(DATA_DIR, f"{file_name}.parquet")
    if os.path.exists(file_path):
        existing_df = pd.read_parquet(file_path)
        combined_df = pd.concat([existing_df, df]).drop_duplicates().reset_index(drop=True)
        combined_df.to_parquet(file_path, index=False)
        print(f"✅ 更新成功：{file_path} (總列數：{len(combined_df)})")
    else:
        df.to_parquet(file_path, index=False)
        print(f"✅ 初次建立成功：{file_path} (總列數：{len(df)})")

def generate_top300_stock_pool():
    """每週依成交值計算前 300 名股票池"""
    k_path = os.path.join(DATA_DIR, "daily_k.parquet")
    if not os.path.exists(k_path):
        return
    
    df = pd.read_parquet(k_path)
    if "Trading_money" in df.columns:
        # 計算近一週每檔股票的總成交金額排序
        df["date"] = pd.to_datetime(df["date"])
        recent_week = df[df["date"] >= df["date"].max() - pd.Timedelta(days=7)]
        top_300 = (
            recent_week.groupby("stock_id")["Trading_money"]
            .sum()
            .nlargest(300)
            .index.tolist()
        )
        pool_df = pd.DataFrame({"stock_id": top_300})
        pool_df.to_parquet(os.path.join(DATA_DIR, "top_300_pool.parquet"), index=False)
        print(f"✅ Top 300 股票池已更新！共 {len(top_300)} 檔股票。")

if __name__ == "__main__":
    print("🚀 開始執行資料管線（2019 年至今）...")
    
    # 範例：抓取全市場或指定個股數據
    for key, dataset in DATASET_MAP.items():
        print(f"📥 正在抓取：{key} ({dataset})...")
        df_data = fetch_finmind_data(dataset_name=dataset, start_date="2019-01-01")
        if not df_data.empty:
            save_to_parquet(df_data, key)
    
    # 計算每週前 300 名股票池
    generate_top300_stock_pool()
    print("🎉 關卡 2 資料下載完成！")
