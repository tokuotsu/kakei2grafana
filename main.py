import os
from dotenv import load_dotenv

load_dotenv(".env")

from src import (
    process_transfer_csv,
    process_and_save_kakeibo_data,
    generate_final_balance_df,
    insert_final_balance_to_db,
    insert_csv_to_postgres
)

def main():
    # 振替CSVファイルの処理
    transfer_csv_file = './data/csvoutputs/transfer.csv'
    card_settings_file = './meta.json'
    output_file = './output/transfer.csv'
    process_transfer_csv(transfer_csv_file, card_settings_file, output_file)

    # 収支データの処理と保存
    db_config = {
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT'),
        'dbname': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD')
    }
    csv_file = './data/csvoutputs/record.csv'
    table_name = 'kakeibo'

    process_and_save_kakeibo_data(csv_file, card_settings_file, db_config, table_name)

    # 最終残高データの生成
    final_balance_df = generate_final_balance_df(
        balance_csv_path="./data/csvoutputs/balance.csv",
        transfer_csv_path=output_file,
        record_csv_path="./output/record.csv",
        meta_json_path=card_settings_file,
        output_csv_path="./output/final_balance.csv"
    )
    print("✅ 最終残高データの生成が完了しました。")

    insert_final_balance_to_db(
        db_config=db_config,
        csv_file_path='./output/final_balance.csv',
        meta_json_path='./meta.json',
        table_name='kakeibo_2',
    )

    insert_csv_to_postgres(db_config, csv_file_path=csv_file)


if __name__ == "__main__":
    main()