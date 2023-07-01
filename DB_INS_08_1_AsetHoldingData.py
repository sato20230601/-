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

# AsetHoldingData用関数
from DB_INS_08_2_Utils_AsetHoldingData import (
    AsetHoldingData_insert,
    validate_row,
    extract_date_from_csv_filename,
    parse_date_string,
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

    file_handler = logging.FileHandler(log_file_path,encoding='utf-8')
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
    default_file_path = r"C:\Users\sabe2\OneDrive\デスクトップ\Python\06_DATABASE\06-02_OBJ\02-8_INS_AsetHoldingData.txt"

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

        # 「AsetHoldingData_csv_dir_path」のパスをconfigファイルより取得
        config_path = r"C:\Users\sabe2\OneDrive\デスクトップ\Python\06_DATABASE\06-03_SRC\config.txt"
        config = DB_Common_Utils.read_config_file(config_path)

        download_folder = config.get("DownloadFolder")
        AsetHoldingData_csv_dir_path = config.get("AsetHoldingData_csv_dir_path")

        # ファイルの検索パターン
        search_pattern = os.path.join(download_folder, '*assetbalance(all)*.csv')

        # 検索パターンにマッチするファイルを取得
        download_files = glob.glob(search_pattern)

        # CSVファイルを取り込むフォルダに移動
        for download_file in download_files:
            destination_path = os.path.join(AsetHoldingData_csv_dir_path, os.path.basename(download_file))
            shutil.move(download_file, destination_path)

        csv_file_paths = glob.glob(os.path.join(AsetHoldingData_csv_dir_path, "*.csv"))

        if len(csv_file_paths) == 0:
            logger.info("CSVファイルが存在しません。処理をスキップします。")
            return

        for csv_file in csv_file_paths:

            logger.info(f"処理中のCSVファイル名: {csv_file}")

            logger.info("AsetHoldingData_insertを開始します。")
            is_insertion_successful = AsetHoldingData_insert(cursor,sql_file_path , csv_file,logger)
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
