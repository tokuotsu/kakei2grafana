# 概要
Androidアプリ「カケイ」をGrafanaで可視化するためのプロジェクトです。
# 構成
- `grafana/` - Grafanaの設定ファイルやダッシュボードのJSONファイルを格納するディレクトリ
- `docker-compose.yml` - GrafanaをDockerで起動するための設定ファイル
- `README.md` - このドキュメント
# Grafanaの起動方法
1. このリポジトリをクローンします。
    ```bash
    git clone
    cd grafana
    docker compose up -d
    ```
2. ブラウザで `http://localhost:3001` にアクセスします。
3. 初期ログイン情報は以下の通りです。
   - ユーザー名: `admin`
   - パスワード: `admin`
4. カケイのCSV出力データ(`balance.csv`, `record.csv`, `transfer.csv`)を`./data/csvoutputs`ディレクトリに配置します。
5. `main.py`を実行して、データをpostgresに保存します。
6. Grafanaで可視化します。

4,5.はGASで自動化する事も可能です。GAS側で`GAS.js`を参考にwebhookを設定し、webhook.pyで待ち受けることで、CSVデータをGoogleDriveにアップロードした後更新を自動化できます。
