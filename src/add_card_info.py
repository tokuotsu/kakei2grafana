import calendar
import datetime
import json
import pandas as pd
from typing import Any

def load_and_process_csv(
        csv_file_path: str, 
        card_settings_path: str
    ) -> pd.DataFrame:
    """
    CSVファイルを読み込んでカラム名の変換、クレジットカード情報の追加処理を行う
    
    parameters:
        csv_file_path (str): 入力CSVファイルのパス
        card_settings_path (str): カード設定JSONファイルのパス
    returns:
        pd.DataFrame: 処理後のデータフレーム
    """
    # JSONファイルの読み込みとカード設定取得
    with open(card_settings_path, "r") as f:
        card_settings = json.load(f)["card_settings"]

    # CSV読み込み
    df = pd.read_csv(csv_file_path, encoding='utf-8', parse_dates=['日付'])
    # カラム名変換（日本語 → 英語）
    df.columns = [
        'date', 
        'type', 
        'category', 
        'subcategory', 
        'amount',
        'place', 
        'memo', 
        'payment_method', 
        'account', 
        'tag'
    ]

    # クレジットカード関連情報を付加
    df = add_credit_withdrawal_info(df, card_settings)

    return df


def add_credit_withdrawal_info(
        df: pd.DataFrame, 
        card_settings: dict[str, Any]
    ) -> pd.DataFrame:
    """
    クレジットカードの引き落とし情報を付与する関数
    この関数は、データフレームの各行に対して、クレジットカードの引き落とし日と引き落とし口座を計算し、追加のカラムを生成します。
    もしアカウントがクレジットカード設定に存在しない場合は、元の引き落とし日と口座をそのまま使用します。
    
    parameters:
        df (pd.DataFrame): 入力のデータフレーム
        card_settings (dict[str, Any]): カード設定情報の辞書
    returns:
        pd.DataFrame: クレジットカード引き落とし情報を追加したデータフレーム
"""
    def get_next_weekday(year, month, day):
        """土日なら次の平日に"""
        dt = datetime.date(year, month, day)
        while dt.weekday() >= 5:  # 土日
            dt += datetime.timedelta(days=1)
        return dt

    def calculate_withdrawal_info(row):
        account = row['account']
        tx_date = pd.to_datetime(row['date']).date()
        if account in card_settings:
            config = card_settings[account]
            closing_day = config['closing_day']
            payment_offset = config['payment_offset_months']
            payment_day = config['payment_day']
            withdrawal_account = config['withdrawal_account']

            # 月末締めの場合
            if closing_day == -1:
                closing_day = calendar.monthrange(tx_date.year, tx_date.month)[1]

            closing_date = datetime.date(tx_date.year, tx_date.month, closing_day)
            if tx_date > closing_date:
                next_month = tx_date.month + 1
                next_year = tx_date.year + (next_month - 1) // 12
                next_month = (next_month - 1) % 12 + 1
                last_day = calendar.monthrange(next_year, next_month)[1]
                real_closing_day = min(closing_day, last_day)
                closing_date = datetime.date(next_year, next_month, real_closing_day)

            payment_month = closing_date.month + payment_offset
            payment_year = closing_date.year + (payment_month - 1) // 12
            payment_month = (payment_month - 1) % 12 + 1

            last_day = calendar.monthrange(payment_year, payment_month)[1]
            raw_payment_day = min(payment_day, last_day)
            payment_date = get_next_weekday(payment_year, payment_month, raw_payment_day)

            return pd.Series([payment_date, withdrawal_account])
        else:
            return pd.Series([row['date'], row['account']])

    df[['withdrawal_date', 'withdrawal_account']] = df.apply(calculate_withdrawal_info, axis=1)
    return df

def process_and_save_kakeibo_data(
        input_record_path: str, 
        output_record_path: str, 
        card_settings_path: str, 
    ) -> None:
    """
    指定されたCSVファイルをクレジットカード情報を追加してデータベースに処理して保存
    
    parameters:
        input_record_path (str): 入力CSVファイルのパス
        output_record_path (str): 出力CSVファイルのパス
        card_settings_path (str): カード設定JSONファイルのパス
        conn_params (dict[str, str | None]): データベース接続情報
        table_name (str): データベースのテーブル名
        
    returns:
        None
    """
    # CSV読み込みと処理
    df = load_and_process_csv(input_record_path, card_settings_path)
    df.to_csv(output_record_path)

if __name__ == "__main__":
    pass