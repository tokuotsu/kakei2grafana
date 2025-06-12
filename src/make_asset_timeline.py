import pandas as pd
import numpy as np
import json

def generate_final_balance_df(
    balance_csv_path,
    transfer_csv_path,
    record_csv_path,
    meta_json_path,
    output_csv_path=None
):
    # --- データ読み込み ---
    data1 = pd.read_csv(balance_csv_path, parse_dates=["日付"])
    transfer_df = pd.read_csv(transfer_csv_path, parse_dates=["withdrawal_date"])
    record_df = pd.read_csv(record_csv_path, parse_dates=["withdrawal_date"])
    data2 = pd.concat([transfer_df, record_df], axis=0)

    # --- タイムゾーン処理 ---
    jst = pd.Timestamp.now(tz='Asia/Tokyo').tz
    data1['日付'] = pd.to_datetime(data1['日付']).dt.tz_localize(
        'Asia/Tokyo', ambiguous='NaT', nonexistent='shift_forward'
    ).dt.normalize()
    data2['withdrawal_date'] = pd.to_datetime(
        data2['withdrawal_date'], format='mixed', errors='coerce'
    ).dt.tz_localize('Asia/Tokyo', ambiguous='NaT', nonexistent='shift_forward').dt.normalize()

    # --- 日付範囲 ---
    min_date = min(data1['日付'].min(), data2['withdrawal_date'].min())
    max_date = max(data1['日付'].max(), data2['withdrawal_date'].max())
    date_range = pd.date_range(start=min_date, end=max_date, freq='D', tz='Asia/Tokyo')
    final_df = pd.DataFrame({'date': date_range})

    # --- アカウント読み込み ---
    with open(meta_json_path, "r") as f:
        accounts = json.load(f)["accounts_ja_en"]

    for account in accounts.keys():
        # --- 入出金イベント ---
        df = data2[data2["withdrawal_account"] == account].copy()
        df['event_date'] = df['withdrawal_date']
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)
        df['収支'] = np.where(df['type'] == '収入', df['amount'], -df['amount'])
        df['is_balance_event'] = 0

        # --- 残高イベント ---
        balance_df = data1[data1["資産"] == account][["日付", "金額"]].rename(
            columns={'日付': 'event_date', '金額': 'balance'}
        ).sort_values("event_date")
        balance_df['収支'] = 0.0
        balance_df['is_balance_event'] = 1

        # --- イベント統合 ---
        all_events = pd.concat([
            df[['event_date', '収支', 'is_balance_event']],
            balance_df[['event_date', '収支', 'is_balance_event', 'balance']]
        ], ignore_index=True, sort=False).sort_values(['event_date', 'is_balance_event'], ascending=[True, False])
        all_events.reset_index(drop=True, inplace=True)

        # --- 残高計算 ---
        cur_balance = 0
        balances_by_datetime = []
        for _, row in all_events.iterrows():
            d = row['event_date']
            if row['is_balance_event'] == 1:
                cur_balance = row['balance']
            else:
                cur_balance += row['収支']
            balances_by_datetime.append((d, cur_balance))

        # --- 日別残高へ変換 ---
        balance_df_by_day = pd.DataFrame(balances_by_datetime, columns=['datetime', 'balance'])
        balance_df_by_day = balance_df_by_day.groupby(balance_df_by_day['datetime'].dt.normalize()).last()
        daily_balance_series = pd.Series(index=date_range, dtype='float64')
        daily_balance_series.update(balance_df_by_day['balance'])
        daily_balance_series = daily_balance_series.ffill()

        # --- 収支計算 ---
        income_ts = pd.Series(0, index=date_range)
        daily_income = df.groupby('event_date')['収支'].sum()
        income_ts.update(daily_income)

        temp_df = pd.DataFrame({
            'date': date_range,
            f'{account}_収支': income_ts.values,
            f'{account}_残高': daily_balance_series.values
        })
        final_df = final_df.merge(temp_df, on='date', how='left')

    # --- 出力（任意） ---
    if output_csv_path:
        final_df.to_csv(output_csv_path, index=False)

    return final_df
    
if __name__ == "__main__":
    final_balance_df = generate_final_balance_df(
        output_csv_path="../output/final_balance.csv"
    )
    print("✅ 最終残高データの生成が完了しました。")
    print(final_balance_df.head())