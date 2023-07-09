import os
import sys
import csv
from datetime import datetime
from configparser import ConfigParser
import logging
import mysql.connector
import shutil
import glob

# 共通関数の読み込み
import DB_Common_Utils

# INSERT/UPDATE 処理共通
from DB_INS_00_Utils import move_processed_csv

# SectorIndustrySymbol用関数
from DB_INS_09_2_Utils_SectorIndustrySymbol import (
    get_sql_file_info,
    insert_data,
    SectorIndustrySymbol_insert
)

# 主処理
def main():

    # デフォルトの文字エンコーディングを変更
    # sys.stderr.reconfigure(encoding='utf-8')

    # ロギングの設定
    log_directory = r"C:\Users\sabe2\OneDrive\デスクトップ\Python\06_DATABASE\06-03_SRC\LOG"
    os.makedirs(log_directory, exist_ok=True)
    log_file_name = f"log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    log_file_path = os.path.join(log_directory, log_file_name)

    # logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', encoding='utf-8')
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(funcName)s - %(message)s')

    file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
    # file_handler = logging.FileHandler(log_file_path)
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
    default_file_path = r"C:\Users\sabe2\OneDrive\デスクトップ\Python\06_DATABASE\06-02_OBJ\02-9_INS_SectorIndustrySymbol.txt"

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

        # SQLファイル一覧からSQLのファイルの数分SQLデータを取得する。
        sql_file_info = get_sql_file_info(sql_file_path, logger)

        # 「SectorIndustrySymbol_csv_dir_path」のパスをconfigファイルより取得
        config_path = r"C:\Users\sabe2\OneDrive\デスクトップ\Python\06_DATABASE\06-03_SRC\config.txt"
        config = DB_Common_Utils.read_config_file(config_path)

        SectorIndustrySymbol_csv_dir_path = config.get("SectorIndustrySymbol_csv_dir_path")

        # SQLファイルの対象テーブルのCSVファイルを抽出し、登録する。
        for sql_info in sql_file_info:
            # SQLファイル名の取得
            table_name = sql_info.table_name
            search_pattern = f"*{table_name}*.csv"

            # ファイルの検索パターン
            search_csv = os.path.join(SectorIndustrySymbol_csv_dir_path, search_pattern)

            # 検索パターンにマッチするファイルを取得
            csv_files = glob.glob(search_csv)

            for csv_file in csv_files:
                is_insertion_successful = SectorIndustrySymbol_insert(cursor, sql_info, csv_file, logger)
                cnx.commit()  # トランザクションをコミットする

                if is_insertion_successful:
                    # 取り込みの終わったCSVファイルのリネームを行う
                    logger.info("取り込みの終わったCSVファイルのリネームを行います。")
                    folder_name = "取込済"
                    move_processed_csv(csv_file, folder_name, logger)

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
