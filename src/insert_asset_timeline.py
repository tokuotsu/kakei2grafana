import pandas as pd
import psycopg2
import psycopg2.extras
import json

def insert_final_balance_to_db(
    db_config,
    csv_file_path,
    meta_json_path,
    table_name,
):
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
    insert_final_balance_to_db(
        db_config=db_config,
        csv_file_path='../output/final_balance.csv',
        meta_json_path='../meta.json',
        table_name='kakeibo_2',
    )
