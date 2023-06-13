#import traceback
#traceback.print_exc()

import os
import sys
import logging
from datetime import datetime
from configparser import ConfigParser

# 関数用の定義ファイルをインクルード
from DB_CRE_00_Utils import create_table, check_table_existence, read_create_statements, process_sql_files

def main():

    # ロギングの設定
    # 一般的なログレベルの階層 CRITICAL > ERROR > WARNING > INFO > DEBUG

    log_directory = r"C:\Users\sabe2\OneDrive\デスクトップ\Python\06_DATABASE\06-03_SRC\LOG"
    os.makedirs(log_directory, exist_ok=True)
    log_file = os.path.join(log_directory, f"log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # コンソール設定
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 処理開始
    logger.info("処理を開始します。")

    # SQLファイル処理
    # デフォルトのファイルパスを設定します。※注\ではなく/で作成すること
    default_file_path = r"C:\Users\sabe2\OneDrive\デスクトップ\Python\06_DATABASE\06-02_OBJ\01-6_CRE_EcoCalender.txt"

    # コマンドライン引数からファイルパスとオプションを取得します
    if len(sys.argv) < 2:
        file_path = default_file_path  # デフォルトのファイルパスを使用します
    else:
        file_path = sys.argv[1]

    try:
        process_sql_files(file_path, logger)
    except Exception as e:
        logger.exception("予期しないエラーが発生しました。")

    # 処理終了
    logger.info("処理が完了しました。")

if __name__ == "__main__":
    main()
