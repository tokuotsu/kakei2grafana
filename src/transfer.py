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
    transfer_csv_file, card_settings_file, output_file, add_credit_info_func=add_credit_withdrawal_info
):
    """
    振替CSVデータを収支データに変換し、クレジットカードの情報を追加して保存
    
    Parameters:
        transfer_csv_file (str): 振替データのCSVファイルパス
        card_settings_file (str): meta.jsonのカード設定ファイルパス
        output_file (str): 変換後のデータを保存するファイルパス
        add_credit_info_func (function): クレジットカード情報を追加する処理関数
        
    Returns:
        None
    """
    # CSV読み込み
    transfer_df = pd.read_csv(transfer_csv_file, encoding='utf-8', parse_dates=['日付'])
    
    # 振替データを収支データに変換
    transformed_df = transform_transfer_data(transfer_df)
    
    # クレジットカード関連情報の付加
    df_with_credit_info = add_credit_info_func(transformed_df, card_settings_file)
    
    # CSVに保存
    df_with_credit_info.to_csv(output_file, index=False)
    print("✅ 振替データの変換と保存が完了しました。")

if __name__ == "__main__":
    from insert_csv2db import add_credit_withdrawal_info
    transfer_csv_file = '../data/csvoutputs/transfer.csv'
    card_settings_file = '../meta.json'
    output_file = '../output/transfer.csv'
    
    process_transfer_csv(transfer_csv_file, card_settings_file, output_file)