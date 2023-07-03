"""
このスクリプトは、YFinanceのデータを取得してMySQLデータベースに挿入または更新することを目的とします。
スクリプトの処理フローは以下の通りです。

1. 必要なライブラリとモジュールをインポートします。
2. ログの設定を行います。ログファイルのディレクトリを作成し、ログのフォーマットとハンドラーを設定します。
3. yfinanceの設定を行います。
4. 処理を開始するログメッセージを記録します。
5. SQLファイルのパスを取得します。コマンドライン引数が指定されていない場合は、デフォルトのパスを使用します。
6. `process_data`関数を呼び出してデータの処理を実行します。
7. エラーが発生した場合は例外を処理し、エラーログを記録します。
8. 処理が完了したことを示すログメッセージを記録します。

"""

import os
import sys
import mysql.connector
import yfinance as yf
from datetime import datetime
from configparser import ConfigParser
import logging

# 関数用の定義ファイルをインクルード
from DB_INS_01_2_Utils_YFinDataTable import process_data

def main():
    # ロギングの設定
    log_directory = r"C:\Users\sabe2\OneDrive\デスクトップ\Python\06_DATABASE\06-03_SRC\LOG"
    os.makedirs(log_directory, exist_ok=True)
    log_file = os.path.join(log_directory, f"log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    file_handler = logging.FileHandler(log_file,encoding='utf-8)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # yfinanceの設定
    # logging.getLogger("yfinance").setLevel(logging.WARNING)

    # 処理開始
    logger.info("処理を開始します。")
    

    # SQLファイル処理
    # デフォルトのファイルパスを設定します
    default_file_path = "C:/Users/sabe2/OneDrive/デスクトップ/Python/06_DATABASE/06-02_OBJ/02-1_INS_YFinDataTable.txt"

    # コマンドライン引数からファイルパスとオプションを取得します
    if len(sys.argv) < 2:
        file_path = default_file_path  # デフォルトのファイルパスを使用します
    else:
        file_path = sys.argv[1]

    # データ処理を実行
    try:
        process_data(file_path, logger)

    except Exception as e:
        logger.exception("予期しないエラーが発生しました。")

        # 処理終了
    logger.info("処理が完了しました。")


if __name__ == "__main__":
    main()
