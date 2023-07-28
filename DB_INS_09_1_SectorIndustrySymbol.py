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
    get_sector_industry_symbol_sp500_data,
    get_sector_industry_sp500_data,
    write_data_to_csv,
    insert_data,
    SectorIndustrySymbol_insert
)

# 主処理
def main():

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

        # 「SectorIndustrySymbol_csv_dir_path」のパスをconfigファイルより取得
        config_path = r"C:\Users\sabe2\OneDrive\デスクトップ\Python\06_DATABASE\06-03_SRC\config.txt"
        config = DB_Common_Utils.read_config_file(config_path)
        csv_dir_path = config.get("SectorIndustrySymbol_csv_dir_path")

        timestamp = datetime.now().strftime('%Y%m%d')

        # sp500のセクター:業種:Symbolの情報を取得
        select_data = get_sector_industry_symbol_sp500_data(cursor)

        # 取得したsp500のセクター:業種:Symbolの情報をCSVに出力
        csv_file = f"{timestamp}_sec_ind_symbol.csv"

        csv_path = os.path.join(csv_dir_path, csv_file)
        write_data_to_csv(select_data, csv_path)

        logger.info(f"sp500のセクター:業種:Symbolの情報をCSVファイルに出力しました: {csv_path}")

        # SQLファイル一覧からSQLのファイルの数分SQLデータを取得する。
        sql_file_info = DB_Common_Utils.get_sql_file_info(sql_file_path, logger)

        # SQLファイルの対象テーブルのCSVファイルを抽出し、登録する。
        for sql_info in sql_file_info:
            # SQLファイル名の取得
            table_name = sql_info.table_name
            search_pattern = f"*{table_name}.csv"

            # ファイルの検索パターン
            search_csv = os.path.join(csv_dir_path, search_pattern)

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
        logger.exception("セクターや業種の情報の取得およびCSV出力中にエラーが発生しました。")

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
