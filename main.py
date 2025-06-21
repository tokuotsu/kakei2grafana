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
    # input paths
    input_transfer_path = './data/csvoutputs/transfer.csv'
    input_record_path = './data/csvoutputs/record.csv'
    input_balance_path="./data/csvoutputs/balance.csv"

    # output paths
    output_transfer_path = './output/transfer.csv'
    output_record_path = './output/record.csv'
    output_final_balance_path = './output/final_balance.csv'

    # setting paths
    card_settings_path = './meta.json'
    table_name_record = 'kakeibo'
    table_name_final = 'kakeibo_2'

    db_config = {
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT'),
        'dbname': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD')
    }

    # record.csvをPostgreSQLにインサート
    insert_csv_to_postgres(
        db_config, 
        input_record_path=input_record_path,
        table_name=table_name_record,
    )
    
    # transfer(振替)データにクレジットカード情報を追加してCSV出力
    process_transfer_csv(
        input_transfer_path, 
        card_settings_path, 
        output_transfer_path
    )

    # record(収支)データにクレジットカード情報を追加し、CSV出力
    process_and_save_kakeibo_data(
        input_record_path, 
        output_record_path, 
        card_settings_path
    )

    # 最終残高データの生成
    final_balance_df = generate_final_balance_df(
        balance_csv_path=input_balance_path,
        transfer_csv_path=output_transfer_path,
        record_csv_path=output_record_path,
        meta_json_path=card_settings_path,
        output_csv_path=output_final_balance_path
    )
    print("✅ 最終残高データの生成が完了しました。")

    insert_final_balance_to_db(
        db_config=db_config,
        csv_file_path=output_final_balance_path,
        meta_json_path=card_settings_path,
        table_name=table_name_final,
    )



if __name__ == "__main__":
    main()