import os
import mysql.connector
import csv
import yfinance as yf
import sys
from datetime import datetime
from configparser import ConfigParser
import logging
import traceback

import requests
from bs4 import BeautifulSoup

# 登録更新処理共通
import DB_INS_00_Utils

# 全処理共通
import DB_Common_Utils

def process_data(file_path, logger):
    try:
        # ファイルからディレクトリパスとSQLファイル名を読み込む
        with open(file_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()

        if len(lines) < 2:
            logger.error("ファイルの内容が正しくありません。")
            return

        directory_path = lines[0].strip()
        sql_files = [line.strip() for line in lines[1:]]

        # ティッカーシンボルファイルからシンボルを読み込む
        config_path = r"C:\Users\sabe2\OneDrive\デスクトップ\Python\06_DATABASE\06-03_SRC\config.txt"
        config = DB_Common_Utils.read_config_file(config_path)
        symbol_file_path = config.get('symbol_file_path')  # ティッカーシンボルが記述されたファイルのパス
        with open(symbol_file_path, 'r') as symbol_file:
            symbols = [symbol.strip() for symbol in symbol_file.readlines()]

        # MySQLに接続
        cnx = DB_Common_Utils.get_mysql_connection()

        # カーソルを取得
        cursor = cnx.cursor()

        # ループの外でエラーフラグを初期化
        has_error = False

        # 各SQLファイルとシンボルを組み合わせて実行
        for sql_file in sql_files:
            sql_file_path = f"{directory_path}/{sql_file}"
            with open(sql_file_path, 'r') as file:
                insert_query = file.read()

            # INSERT文からメンバー名を取得
            start_index = insert_query.index('(') + 1
            end_index = insert_query.index(')')
            members = [m.strip() for m in insert_query[start_index:end_index].split(',')]
            table_name = insert_query.split()[2]  # INSERT文からテーブル名を抽出

            for symbol in symbols:
                symbol = symbol.strip()  # シンボルの両端の空白を削除

                # データを取得
                stock = yf.Ticker(symbol)
                fundamental_data = stock.info

                # データをテーブルに挿入または更新
                insert_values = []
                update_values = []
                for member in members:
                    if member == 'INS_DATE':
                        value = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        insert_values.append(value)

                    elif member == 'UPD_DATE':
                        value = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        insert_values.append(None)
                        update_values.append(value)
                    else:
                        value = fundamental_data.get(member, None)
                        insert_values.append(value)
                        update_values.append(value)

                # SELECT文を実行してデータの存在有無を確認
                select_query = f"SELECT * FROM {table_name} WHERE symbol = %s"
                select_values = [symbol]
                result = DB_Common_Utils.execute_sql_query(cursor, select_query, select_values, logger)

                if result:
                    # データが存在する場合はUPDATE文を実行
                    update_columns = [f"{member} = %s" for member in members if member != 'INS_DATE']

                    # UPDATE文を実行
                    DB_INS_00_Utils.update_data_in_table(cursor, table_name, update_columns, update_values, 'symbol', symbol, logger)
                    logger.info(f"{sql_file} が正常に更新されました。:{table_name}:{symbol}")

                else:
                    # データが存在しない場合はINSERT文を実行
                    # INSERT文を実行
                    DB_INS_00_Utils.insert_data_into_table(cursor, table_name, members, insert_values, logger)
                    logger.info(f"{sql_file} が正常に登録されました。:{table_name}:{symbol}")

        # カーソルと接続を閉じる
        cursor.close()
        cnx.commit()  # トランザクションをコミットする
        cnx.close()
        logger.info(f"処理が完了しました。SQLファイル: {sql_file}")

    except Exception as e:
        logger.error("処理中にエラーが発生しました。")
        logger.error(str(e))
        logger.error(traceback.format_exc())  # 修正: トレースバック情報をログに出力
        has_error = True  # エラーフラグを設定

    # エラーフラグをチェックして処理を終了
    if has_error:
        logger.error("エラーが発生したため、処理を終了します。")
        return
