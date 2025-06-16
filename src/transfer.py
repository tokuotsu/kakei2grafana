import json
import pandas as pd
if __name__ == "__main__":
    from insert_csv2db import add_credit_withdrawal_info
else:
    from .insert_csv2db import add_credit_withdrawal_info

def transform_transfer_data(transfer_df):
    """
    振替データを収支に変換する
    
    Parameters:
        transfer_df (pd.DataFrame): 振替データのデータフレーム
    
    Returns:
        pd.DataFrame: 変換された収支データのデータフレーム
    """
    def transform_row(row):
        """
        各振替データの行を収支データに変換する
        """
        date = row['日付']
        amount = row['金額']
        from_account = row['出金']
        to_account = row['入金']
        memo = row.get('メモ', '')
        return [
            {
                'date': date,
                'type': '支出',
                'category': '振替',
                'subcategory': '',
                'amount': amount,
                'place': '',
                'memo': f'振替: {to_account}へ。{memo}',
                'payment_method': '振替',
                'account': from_account,
                'tag': ''
            },
            {
                'date': date,
                'type': '収入',
                'category': '振替',
                'subcategory': '',
                'amount': amount,
                'place': '',
                'memo': f'振替: {from_account}から。{memo}',
                'payment_method': '振替',
                'account': to_account,
                'tag': ''
            }
        ]
    
    # 各行を変換して flatten
    records_series = transfer_df.apply(transform_row, axis=1)
    flat_records = [item for sublist in records_series for item in sublist]
    
    # DataFrame化して返す
    return pd.DataFrame(flat_records)


def process_transfer_csv(
    transfer_csv_file_path, 
    card_settings_file_path, 
    output_file_path, 
    add_credit_info_func=add_credit_withdrawal_info
):
    """
    振替CSVデータを収支データに変換し、クレジットカードの情報を追加して保存
    
    Parameters:
        transfer_csv_file_path (str): 振替データのCSVファイルパス
        card_settings_file_path (str): meta.jsonのカード設定ファイルパス
        output_file_path (str): 変換後のデータを保存するファイルパス
        add_credit_info_func (function): クレジットカード情報を追加する処理関数
        
    Returns:
        None
    """
    # CSV読み込み
    transfer_df = pd.read_csv(transfer_csv_file_path, encoding='utf-8', parse_dates=['日付'])
    
    # 振替データを収支データに変換
    transformed_df = transform_transfer_data(transfer_df)
    with open(card_settings_file_path, "r") as f:
        card_settings = json.load(f)["card_settings"]
    
    # クレジットカード関連情報の付加
    df_with_credit_info = add_credit_info_func(transformed_df, card_settings)
    
    # CSVに保存
    df_with_credit_info.to_csv(output_file_path, index=False)
    print("✅ 振替データの変換と保存が完了しました。")

if __name__ == "__main__":
    from insert_csv2db import add_credit_withdrawal_info
    import json
    transfer_csv_file_path = '../data/csvoutputs/transfer.csv'
    card_settings_file = '../meta.json'
    output_file_path = '../output/transfer.csv'
    with open(card_settings_file, "r") as f:
        card_settings = json.load(f)["card_settings"]
    print("iD/Amazon" in card_settings)
    # exit()
    process_transfer_csv(
        transfer_csv_file_path, 
        card_settings_file, 
        output_file_path,
        add_credit_info_func=add_credit_withdrawal_info)