import os
import sys
import csv
from datetime import datetime
from configparser import ConfigParser
import logging
import mysql.connector

# 共通関数の読み込み
import DB_Common_Utils

# CREATE/DELETE 処理共通
from DB_CRE_00_Utils import create_table, check_table_existence, read_create_statements, process_sql_files,drop_table,clear_table

# INSERT/UPDATE 処理共通
from DB_INS_00_Utils import generate_insert_sql

# Trading_History用関数
from DB_INS_07_2_Utils_Trading_History import Trading_History_insert_tmp_data,Trading_History_insert_data

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
    console_handler.setLevel(logging.WARN) 
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 処理開始
    logger.info("処理を開始します。")

    # SQLファイル処理
    # デフォルトのファイルパスを設定します
    default_file_path = r"C:\Users\sabe2\OneDrive\デスクトップ\Python\06_DATABASE\06-02_OBJ\02-7_INS_Trading_History.txt"

    # コマンドライン引数からファイルパスとオプションを取得します
    if len(sys.argv) < 2:
        file_path = default_file_path  # デフォルトのファイルパスを使用します

    else:
        file_path = sys.argv[1]

    # データ処理を実行
    try:

        # MySQLに接続
        cnx = DB_Common_Utils.get_mysql_connection()

        # カーソルを取得
        cursor = cnx.cursor()

        config_key = "Trading_History_csv_file_path"

        # 1.一時テーブルの作成 
        Temp_file_path = r"C:\Users\sabe2\OneDrive\デスクトップ\Python\06_DATABASE\06-02_OBJ\01-7_CRE_Trading_History.txt"
        flg = 1
        logger.info("process_sql_filesを開始します。")
        tmp_table_name = process_sql_files(cursor,Temp_file_path,logger,flg)

        # 2.一時テーブルへのCSVの登録 
        logger.info("Trading_History_process_dataを開始します。")
        table_name,members =Trading_History_insert_tmp_data(cursor, file_path,config_key,tmp_table_name,logger)

        # 3.重複しないデータを抽出し、主テーブルへの登録を行う。
        logger.info("generate_insert_sqlを開始します。")
        insert_sql = generate_insert_sql(tmp_table_name,table_name,members,logger)

        logger.info("Trading_History_process_dataを開始します。")
        Trading_History_insert_data(cursor, insert_sql,logger)
        cnx.commit()  # トランザクションをコミットする

        # 4.一時テーブルの削除
        logger.info("一時テーブルの削除を開始します。")
        drop_table(cursor, tmp_table_name, logger)
        logger.info("一時テーブルの削除が完了しました。")

        # カーソルと接続を閉じる
        cursor.close()
        cnx.commit()  # トランザクションをコミットする
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
