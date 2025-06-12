import os
import subprocess

from flask import Flask, request
import requests

app = Flask(__name__)

@app.route('/drive-webhook', methods=['GET', 'POST'])
def drive_webhook():
    if request.method == 'GET':
        return "OK", 200  # ブラウザからアクセスした時に表示される

    # POST（Webhookからの通知）の処理
    data = request.json
    print("📥 新着ファイル通知:", data)

    for file in data.get("files", []):
        url = file["url"]
        filename = file["name"]

        # 共有リンク → ダウンロードURL変換（Google Driveの仕様）
        file_id = file["id"]
        download_url = f"https://drive.google.com/uc?export=download&id={file_id}"

        response = requests.get(download_url)
        with open(f"./data/csvoutputs/{filename}", "wb") as f:
            f.write(response.content)
            print(f"✅ {filename} を保存しました")
    
    subprocess.run(["/home/ttoku/Environments/grafana/bin/python", "main.py"])

    return "", 200

if __name__=="__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)