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
# import DB_INS_00_Utils

# 共通関数の読み込み
import DB_Common_Utils

def csv_process_data(file_path, config_key, logger):
    try:
        # ファイルからディレクトリパスとSQLファイル名を読み込む
        with open(file_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()

        if len(lines) < 2:
            logger.error("ファイルの内容が正しくありません。")
            return

        directory_path = lines[0].strip()
        sql_files = [line.strip() for line in lines[1:]]

        # CSVデータファイルのパスをconfigファイルより取得
        config = DB_Common_Utils.read_config_file('config.txt')
        csv_file_path = config.get(config_key)

        # MySQLに接続
        cnx = DB_Common_Utils.get_mysql_connection()

        # カーソルを取得
        cursor = cnx.cursor()

        # ループの外でエラーフラグを初期化
        has_error = False

        # 各SQLファイルとシンボルを組み合わせて実行
        for sql_file in sql_files:

            sql_file_path = f"{directory_path}/{sql_file}"
            table_name, members, additional_statement = DB_Common_Utils.get_table_name_and_members(sql_file_path)

            # CSVファイルのデータをテーブルに登録
            with open(csv_file_path, 'r', newline='', encoding='utf-8') as csvfile:
                csv_reader = csv.reader(csvfile, delimiter='\t')
                next(csv_reader)  # ヘッダー行をスキップする場合はコメントアウト

                for row_index, row in enumerate(csv_reader, start=1):
                    # 空白を除外してデータを整形
                    row = [value.strip() for value in row]

                    # SELECT文を実行してデータの存在有無を確認
                    select_query = f"SELECT * FROM {table_name} WHERE symbol = %s AND transaction_date = %s AND transaction_type = %s AND market = %s AND settlement_currency = %s AND exchange_rate = %s AND unit_price = %s AND quantity = %s"
                    select_values = [row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7]]

                    # SQL文を表示またはログファイルに書き込み
                    logger.debug("データの存在有無 SQL文:[%d]",row_index)
                    logger.info(select_query % tuple(select_values))  # 値をセットしたSQL文を表示
                    logger.debug("VALUES:")
                    logger.debug(str(select_values))

                    cursor.execute(select_query, select_values)
                    result = cursor.fetchone()  # データが存在するかどうかを取得

                    # resultの内容を確認してログ出力
                    logger.info("データの存在有無 件数:[%s]", str(result))

                    if result:
                        # データが存在する場合はスキップ
                        logger.info("データが既に存在します。スキップします。")
                        logger.debug("スキップしたデータ:[%d][table_name:%s] [symbol:%s] [transaction_date:%s][transaction_type:%s][unit_price:%s][quantity:%s]",
                                     row_index, table_name, row[0], row[1], row[2], row[6], row[7])
                        continue

                    if row[4] == '-' or row[6] == '0':
                        # '-'のデータは登録対象外として別ファイルに出力
                        logger.info("登録対象外のデータです。")
                        logger.info("データ:[%d][table_name:%s] [symbol:%s] [transaction_date:%s][transaction_type:%s][unit_price:%s][quantity:%s]",
                                    row_index, table_name, row[0], row[1], row[2], row[6], row[7])
                        excluded_data_path = f"./excluded_data.txt"
                        with open(excluded_data_path, 'w', encoding='utf-8') as excluded_file:
                            excluded_file.write('\t'.join(row) + '\n')
                        continue

                    # データが存在しない場合はINSERT文を実行
                    insert_values = []
                    for member, value in zip(members, row):
                        if member == 'INS_DATE':
                            insert_values.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                        elif member == 'UPD_DATE':
                            insert_values.append(None)
                        elif member in ['unit_price', 'exchange_rate']:
                            # カンマを削除して数値として扱う
                            value = value.replace(',', '')
                            insert_values.append(value)
                        else:
                            insert_values.append(value)

                    # INSERT文を実行
                    insert_query = f"""
                    INSERT INTO {table_name}
                        ({', '.join(members)})
                    VALUES
                        ({', '.join(['%s'] * len(members))})
                    """
                    insert_values.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))  # INS_DATEの値を追加
                    insert_values.append(None)  # UPT_DATEの値を追加

                    # SQL文を表示またはログファイルに書き込み
                    logger.debug("実行するSQL文:")
                    logger.info(insert_query % tuple(insert_values))  # 値をセットしたSQL文を表示
                    logger.debug("VALUES:")
                    logger.debug(str(insert_values))

                    try:
                        # INSERT文を実行
                        cursor.execute(insert_query, insert_values)
                        cnx.commit()
                        logger.info("データを挿入しました。:[%d]", row_index)
                    except mysql.connector.Error as error:
                        logger.error("INSERT文の実行中にエラーが発生しました:", error)
                        logger.error("対象行番号:[%d]", row_index)  # 対象行番号をログに出力
                        raise  # エラーを再度発生させて処理を終了

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

