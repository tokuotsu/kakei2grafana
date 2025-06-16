import csv
import pandas as pd
import psycopg2
from psycopg2 import sql
import pytz

def insert_csv_to_postgres(db_config, csv_file_path):
    # 接続を確立
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        print("PostgreSQLに接続しました。")
    except Exception as e:
        print(f"データベース接続エラー: {e}")
        exit()
    # テーブル作成SQL
    create_table_sql = """
    DROP TABLE IF EXISTS kakeibo;
    CREATE TABLE IF NOT EXISTS kakeibo (
        id SERIAL PRIMARY KEY,
        date TIMESTAMP NOT NULL,
        income_or_expense VARCHAR(255),
        category VARCHAR(255),
        sub_category VARCHAR(255),
        amount INTEGER,
        location VARCHAR(255),
        memo TEXT,
        payment_method VARCHAR(255),
        bank_account_or_card VARCHAR(255),
        tag TEXT
    );
    """

    cursor.execute(create_table_sql)
    conn.commit()

    # CSVをDataFrameで読み込む
    df = pd.read_csv(csv_file_path, encoding='utf-8')

    # 日付列を datetime に強制変換（formatが合ってる場合）
    df['日付'] = pd.to_datetime(df['日付'], errors='coerce')

    # JST → UTC に変換
    jst = pytz.timezone('Asia/Tokyo')
    df['日付'] = df['日付'].apply(lambda d: jst.localize(d).astimezone(pytz.utc) if pd.isna(d.tzinfo) else d.astimezone(pytz.utc))

    # 金額を整数に変換（エラー処理付き）
    df['金額'] = pd.to_numeric(df['金額'], errors='coerce').fillna(0).astype(int)

    # 必要な列の順序でデータをタプルのリストに変換
    records = list(df[['日付', '収入/支出', 'カテゴリ', 'サブカテゴリ', '金額',
                    '店舗/場所', 'メモ', '入金/支払い方法', '銀行口座/カード等', 'タグ']].itertuples(index=False, name=None))


    # executemanyで一括INSERT
    insert_query = sql.SQL("""
        INSERT INTO kakeibo (
            date, income_or_expense, category, sub_category, amount,
            location, memo, payment_method, bank_account_or_card, tag
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """)

    try:
        cursor = conn.cursor()
        cursor.executemany(insert_query, records)
        conn.commit()
        print(f"{len(records)} 行を一括挿入しました。")
    except Exception as e:
        conn.rollback()
        print("エラーが発生:", e)
    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
    insert_csv_to_postgres()