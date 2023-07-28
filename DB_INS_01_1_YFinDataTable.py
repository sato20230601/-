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
import glob

# 共通関数の読み込み
import DB_Common_Utils

# YFinDataTable用関数
from DB_INS_01_2_Utils_YFinDataTable import (
    process_data,
    load_symbols_from_csv
)

def main():
    # ロギングの設定
    log_directory = r"C:\Users\sabe2\OneDrive\デスクトップ\Python\06_DATABASE\06-03_SRC\LOG"
    os.makedirs(log_directory, exist_ok=True)
    log_file = os.path.join(log_directory, f"log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    file_handler = logging.FileHandler(log_file,encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 処理開始
    logger.info("処理を開始します。")

    # SQLファイル処理
    # デフォルトのファイルパスを設定します
    default_file_path = "C:/Users/sabe2/OneDrive/デスクトップ/Python/06_DATABASE/06-02_OBJ/02-1_INS_YFinDataTable.txt"

    # コマンドライン引数からファイルパスとオプションを取得します
    if len(sys.argv) < 2:
        sql_file_path = default_file_path  # デフォルトのファイルパスを使用します
    else:
        sql_file_path = sys.argv[1]

    # データ処理を実行
    try:

        # MySQLに接続
        cnx = DB_Common_Utils.get_mysql_connection()

        # カーソルを取得
        cursor = cnx.cursor()

        # ティッカーシンボルファイルからシンボルを読み込む
        config_path = r"C:\Users\sabe2\OneDrive\デスクトップ\Python\06_DATABASE\06-03_SRC\config.txt"
        config = DB_Common_Utils.read_config_file(config_path)
        csv_dir_path = config.get('symbols_csv_dir_path')  # ティッカーシンボルが記述されたファイルのパス

        search_pattern = f"*symbols.csv"

        # ファイルの検索パターン
        search_csv = os.path.join(csv_dir_path, search_pattern)

        # 検索パターンにマッチするファイルを取得
        csv_files = glob.glob(search_csv)

        symbols = []

        for csv_file in csv_files:
            symbols += load_symbols_from_csv(csv_file)

        # SQLファイル一覧からSQLのファイルの数分SQLデータを取得する。
        sql_file_info = DB_Common_Utils.get_sql_file_info(sql_file_path, logger)

        for symbol in symbols:
            symbol = symbol.strip()  # シンボルの両端の空白を削除

            # SQLファイルの対象テーブルのCSVファイルを抽出し、登録する。
            for sql_info in sql_file_info:

                is_successful = process_data(cursor, symbol, sql_info, logger)
                if is_successful == True:
                    cnx.commit()  # トランザクションをコミットする
                elif is_successful == "No Data":
                    logger.debug(f"データがないのでスキップします。:{symbol}")
                    continue

                elif is_successful == False:
                    logger.debug(f"データ取得時にエラーの為スキップします。:{symbol}")
                    continue

                else:
                    raise Exception("データの挿入に失敗しました。")
                
        # カーソルと接続を閉じる
        cursor.close()
        cnx.close()

    except Exception as e:
        logger.exception("予期しないエラーが発生しました。")

    finally:
        # カーソルと接続を閉じる
        if cursor:
            cursor.close()
        if cnx:
            cnx.close()

        logger.info("カーソルと接続を閉じました。")

        # 処理終了
    logger.info("処理が完了しました。")

if __name__ == "__main__":
    main()
