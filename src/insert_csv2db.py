import calendar
import datetime
import json
import pandas as pd
import psycopg2


def create_database_table(conn_params, table_name):
    """データベースにテーブルを作成する"""
    create_table_sql = f"""
    DROP TABLE IF EXISTS {table_name};
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        date TIMESTAMP,
        type VARCHAR(10),
        category VARCHAR(100),
        subcategory VARCHAR(100),
        amount NUMERIC,
        place VARCHAR(200),
        memo TEXT,
        payment_method VARCHAR(100),
        account VARCHAR(100),
        tag VARCHAR(100),
        withdrawal_date TIMESTAMP,
        withdrawal_account VARCHAR(100)
    );
    """
    with psycopg2.connect(**conn_params) as conn:
        with conn.cursor() as cur:
            cur.execute(create_table_sql)
            conn.commit()


def load_and_process_csv(csv_file, card_settings_file):
    """CSVファイルを読み込んで処理を行う"""
    # JSONファイルの読み込みとカード設定取得
    with open(card_settings_file, "r") as f:
        card_settings = json.load(f)["card_settings"]

    # CSV読み込み
    df = pd.read_csv(csv_file, encoding='utf-8', parse_dates=['日付'])
    # カラム名変換（日本語 → 英語）
    df.columns = [
        'date', 'type', 'category', 'subcategory', 'amount',
        'place', 'memo', 'payment_method', 'account', 'tag'
    ]

    # クレジットカード関連情報を付加
    df = add_credit_withdrawal_info(df, card_settings)

    return df


def add_credit_withdrawal_info(df, card_settings):
    """クレジットカード引き落とし情報を付与"""
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


def save_dataframe_to_database(df, conn_params, table_name):
    """データフレームをデータベースに保存"""
    with psycopg2.connect(**conn_params) as conn:
        with conn.cursor() as cur:
            for _, row in df.iterrows():
                insert_sql = f"""
                INSERT INTO {table_name} (
                    date, type, category, subcategory, amount, place,
                    memo, payment_method, account, tag, withdrawal_date, withdrawal_account
                )
                VALUES (
                    %(date)s, %(type)s, %(category)s, %(subcategory)s, %(amount)s, %(place)s,
                    %(memo)s, %(payment_method)s, %(account)s, %(tag)s, %(withdrawal_date)s, %(withdrawal_account)s
                );
                """
                cur.execute(insert_sql, row.to_dict())
            conn.commit()

def process_and_save_kakeibo_data(csv_file, card_settings_file, conn_params, table_name):
    """指定されたCSVファイルをデータベースに処理して保存"""
    # テーブル作成
    create_database_table(conn_params, table_name)

    # CSV読み込みと処理
    df = load_and_process_csv(csv_file, card_settings_file)

    # データベースへ保存
    save_dataframe_to_database(df, conn_params, table_name)

if __name__ == "__main__":
    process_and_save_kakeibo_data()