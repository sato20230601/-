import os
import sys
import csv
from datetime import datetime
from configparser import ConfigParser
import logging
import mysql.connector

from DB_INS_03_2_Utils_sp500DataTable import sp500_process_data

def main():
    # デフォルトの文字エンコーディングを変更
    # sys.stdout.reconfigure(encoding='utf-8')
    # sys.stderr.reconfigure(encoding='utf-8')

    # ロギングの設定
    log_directory = r"C:\Users\sabe2\OneDrive\デスクトップ\Python\06_DATABASE\06-03_SRC\LOG"
    os.makedirs(log_directory, exist_ok=True)
    log_file_name = f"log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    log_file_path = os.path.join(log_directory, log_file_name)

    # logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', encoding='utf-8')
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

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
    default_file_path = r"C:\Users\sabe2\OneDrive\デスクトップ\Python\06_DATABASE\06-02_OBJ\02-3_INS_sp500DataTable.txt"

    # コマンドライン引数からファイルパスとオプションを取得します
    if len(sys.argv) < 2:
        file_path = default_file_path  # デフォルトのファイルパスを使用します
    else:
        file_path = sys.argv[1]

    # データ処理を実行
    try:
        config_key = "sp500_csv_file_path"
        sp500_process_data(file_path,config_key,logger)
    except Exception as e:
        logger.exception("予期しないエラーが発生しました。")

    # 処理終了
    logger.info("処理が完了しました。")

if __name__ == "__main__":
    main()
