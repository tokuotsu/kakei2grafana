import os
import subprocess
import traceback

from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

@app.route('/drive-webhook', methods=['GET', 'POST'])
def drive_webhook():
    if request.method == 'GET':
        return "OK", 200  # ブラウザからアクセスした時に表示される

    try:
        data = request.json
        if not data:
            return jsonify({"error": "No JSON received"}), 400

        print("📥 新着ファイル通知:", data)

        for file in data.get("files", []):
            try:
                url = file["url"]
                filename = file["name"]
                file_id = file["id"]
                download_url = f"https://drive.google.com/uc?export=download&id={file_id}"

                print(f"⬇️ {filename} をダウンロード中...")

                response = requests.get(download_url)
                response.raise_for_status()

                save_path = f"./data/csvoutputs/{filename}"
                os.makedirs(os.path.dirname(save_path), exist_ok=True)

                with open(save_path, "wb") as f:
                    f.write(response.content)
                    print(f"✅ {filename} を保存しました")

            except KeyError as e:
                print(f"❌ ファイル情報に必要なキーがありません: {e}")
            except requests.RequestException as e:
                print(f"❌ ダウンロードに失敗しました: {e}")
            except Exception as e:
                print(f"❌ ファイル保存時に予期せぬエラー: {e}")
                traceback.print_exc()

        try:
            print("▶️ main.py を実行します")
            result = subprocess.run(
                ["/home/ttoku/Environments/grafana/bin/python", "main.py"],
                capture_output=True,
                text=True,
                check=True
            )
            print("✅ main.py 実行成功")
            print(result.stdout)
        except subprocess.CalledProcessError as e:
            print(f"❌ main.py 実行中にエラー: {e}")
            print(e.stderr)

        return "", 200

    except Exception as e:
        print(f"❌ 全体処理でエラー: {e}")
        traceback.print_exc()
        return jsonify({"error": "Internal server error"}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=False)
