"""
このスクリプトは、YFinanceのAPIを使用してティッカーシンボルからYFinanceのデータを取得し、
そのデータをMySQLデータベースに挿入または更新するPythonスクリプトです。以下にスクリプトの概要を説明します。

1. ファイルを読み込んでディレクトリパスとSQLファイル名を取得します。
2. 「ティッカーシンボルファイル」を読み込んで「ティッカーシンボル」のリストを取得します。
3. MySQLデータベースに接続します。
4. SQLクエリを実行するためのカーソルを取得します。
5. エラーフラグを初期化します。
6. 実行日付を取得します。
7.「SQLファイル(INSERT文)」から「テーブル名」、「メンバー名」、「条件部」を取得する。
8.「ティッカーシンボル」の数分、「YFinanceのAPI」より「ティッカーシンボル」を指定し、YFinanceのデータを取得する。
9. 各ティッカーシンボルに対してINSERTクエリまたはUPDATEクエリを実行します。
10. SELECTクエリを使用してデータの存在を確認します。
11. データが存在する場合はUPDATEクエリを実行します。
12. データが存在しない場合はINSERTクエリを実行します。
13. 変更をコミットし、カーソルとデータベース接続をクローズします。
14. 例外を処理し、エラーをログに記録します。

`DB_INS_00_Utils.get_table_name_and_members`、
`DB_Common_Utils.read_config_file`、
`DB_Common_Utils.get_mysql_connection`、
`DB_Common_Utils.execute_sql_query`、
`DB_INS_00_Utils.update_data_in_table`、
`DB_INS_00_Utils.insert_data_into_table`
などのコードスニペットは、おそらく別のファイルで定義されたカスタムユーティリティ関数やメソッドであることに注意してください。

"""

import os
import mysql.connector
import csv
import yfinance as yf
import sys
from datetime import datetime
from datetime import date

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

        # 実行日付を取得
        execution_date = date.today()

        # 各SQLファイルとシンボルを組み合わせて実行
        for sql_file in sql_files:
            sql_file_path = f"{directory_path}/{sql_file}"

            logger.info(f"処理を開始します。SQLファイル: {sql_file}")
            logger.debug(sql_file_path)

            # INSERT文からテーブル名、メンバー名、条件部を取得
            table_name, members, additional_statement = DB_INS_00_Utils.get_table_name_and_members(sql_file_path)
            logger.debug(table_name)
            logger.debug(members)
            logger.debug(additional_statement)

            with open(sql_file_path, 'r') as file:
                insert_query = file.read()

            for symbol in symbols:
                symbol = symbol.strip()  # シンボルの両端の空白を削除

                # YFinanceのAPIより「ティッカーシンボル」を指定し、YFinanceのデータを取得
                stock = yf.Ticker(symbol)
                fundamental_data = stock.info

                # データをテーブルに挿入または更新
                insert_values = []
                update_values = []
                for member in members:
                    if member == 'INS_DATE':
                        value = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        logger.info(f"1:{member}:{value}")
                        insert_values.append(value)

                    elif member == 'UPD_DATE':
                        value = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        logger.info(f"2:{member}:{value}")
                        insert_values.append(None)
                        update_values.append(value)

                    elif member == 'Date_YYYYMMDD':

                        insert_values.append(execution_date)
                        update_values.append(execution_date)

                    else:
                        value = fundamental_data.get(member, None)
                        logger.info(f"3:{member}:{value}")
                        insert_values.append(value)
                        update_values.append(value)

                # SELECT文を実行してデータの存在有無を確認
                # テーブル名がFinancials,MarketData,RiskAssessmentの場合のみ実行日付を条件に追加
                if table_name == "Financials" or table_name == "MarketData" or table_name == "RiskAssessment":

                    select_query = f"SELECT * FROM {table_name} WHERE symbol = %s AND Date_YYYYMMDD = %s "
                    select_values = (symbol, execution_date)

                else:
                    select_query = f"SELECT * FROM {table_name} WHERE symbol = %s"
                    select_values = (symbol,)

                # SQL文を表示またはログファイルに書き込み
                logger.debug("実行するSQL文:")
                logger.info(select_query % tuple(select_values))  # 値をセットしたSQL文を表示

                result = DB_Common_Utils.execute_sql_query(cursor, select_query, select_values, logger)
                logger.info(f"SELECTの結果: {result}")

                if result:
                    # データが存在する場合はUPDATE文を実行
                    update_columns = [f"{member} = %s" for member in members if member != 'INS_DATE']

                    # SQL文を表示またはログファイルに書き込み
                    logger.info(update_values)  # 値をセットしたSQL文を表示

                    # テーブル名がFinancials,MarketData,RiskAssessmentの場合のみ実行日付を条件に追加
                    if table_name == "Financials" or table_name == "MarketData" or table_name == "RiskAssessment":
                        update_conditions = ['symbol', 'Date_YYYYMMDD']
                        update_conditions_values = (symbol, execution_date)
                    else:
                        update_conditions = ['symbol']
                        update_conditions_values = (symbol,)

                    # UPDATE文を実行
                    DB_INS_00_Utils.update_data_in_table(cursor, table_name, update_columns, tuple(update_values), update_conditions, update_conditions_values, logger)
                    logger.info(f"{sql_file} が正常に更新されました。:{table_name}:{update_values}:{update_conditions_values}")

                else:
                    # データが存在しない場合はINSERT文を実行

                    # SQL文を表示またはログファイルに書き込み
                    logger.info(insert_values)  # 値をセットしたSQL文を表示

                    # INSERT文を実行
                    DB_INS_00_Utils.insert_data_into_table(cursor, table_name, members, tuple(insert_values), logger)
                    logger.info(f"{sql_file} が正常に登録されました。:{table_name}:{members}:{insert_values}")

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
