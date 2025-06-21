import pandas as pd
import psycopg2
import psycopg2.extras
import json
from typing import Any

def insert_final_balance_to_db(
        db_config: dict[str, Any],
        csv_file_path: str,
        meta_json_path: str,
        table_name: str,
    ) -> None:
    """
    最終残高データをPostgreSQLに高速インサートする関数
    この関数は、指定されたCSVファイルから最終残高データを読み込み、PostgreSQLデータベースに高速でインサートします。
    テーブルが存在しない場合は新規作成し、既存のテーブルは削除してから作成します。
    Args:
        db_config (dict): データベース接続情報
        csv_file_path (str): 最終残高データのCSVファイルパス
        meta_json_path (str): meta.jsonのパス
        table_name (str): 挿入先のテーブル名
        
    Returns:
        None
    """
    # PostgreSQL接続
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()

    # assets情報読み込み
    with open(meta_json_path, "r") as f:
        assets = json.load(f)["accounts_ja_en"]

    # カラム名のSQL部分生成
    column_sql = ",\n".join(
        f"{account}_{flow_or_balance} NUMERIC" 
        for account in assets.values() 
        for flow_or_balance in ["flow", "balance"]
    )

    # テーブル作成SQL
    create_table_sql = f"""
    DROP TABLE IF EXISTS {table_name};
    CREATE TABLE {table_name} (
        id SERIAL PRIMARY KEY,
        date DATE,
        {column_sql}
    );
    """
    cur.execute(create_table_sql)
    conn.commit()

    # CSV読み込み
    df = pd.read_csv(csv_file_path, encoding='utf-8', parse_dates=['date'])

    # DataFrameカラム名の変換
    df.columns = ['date'] + [f"{account}_{flow_or_balance}" for account in assets.values() for flow_or_balance in ["flow", "balance"]]

    # バルクインサート用SQL
    insert_sql = f"""
        INSERT INTO {table_name} ({', '.join(df.columns)})
        VALUES %s
    """

    # 高速バルクインサート
    psycopg2.extras.execute_values(
        cur, insert_sql, df.values.tolist(), page_size=100
    )

    conn.commit()
    cur.close()
    conn.close()

    print("✅ 高速インサート完了！")
    
if __name__ == '__main__':
    pass
