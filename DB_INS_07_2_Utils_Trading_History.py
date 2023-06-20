import os
import csv
from datetime import datetime, date, timedelta

import logging
import traceback
import requests

import mysql.connector
from configparser import ConfigParser
from collections import defaultdict

import sys

import xml.etree.ElementTree as ET

# 作成削除処理共通
import DB_CRE_00_Utils

# 登録更新処理共通
import DB_INS_00_Utils

# 共通関数の読み込み
import DB_Common_Utils

def Trading_History_insert_tmp_data(file_path, config_key ,tmp_table_name ,logger):
    try:
        # ファイルからディレクトリパスとSQLファイル名を読み込む
        with open(file_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()

        if len(lines) < 2:
            logger.error("ファイルの内容が正しくありません。")
            return

        directory_path = lines[0].strip()
        sql_files = [line.strip() for line in lines[1:]]

        # 「Trading_History_csv_file_path」のパスをconfigファイルより取得
        config_path = r"C:\Users\sabe2\OneDrive\デスクトップ\Python\06_DATABASE\06-03_SRC\config.txt"
        config = DB_Common_Utils.read_config_file(config_path)
        Trading_History_csv_file_path = config.get('Trading_History_csv_file_path')

        # MySQLに接続
        cnx = DB_Common_Utils.get_mysql_connection()

        # カーソルを取得
        cursor = cnx.cursor()

        # ループの外でエラーフラグを初期化
        has_error = False

        # 各SQLファイルとシンボルを組み合わせて実行
        for sql_file in sql_files:
            sql_file_path = f"{directory_path}/{sql_file}"
            logger.debug(sql_file_path)

            table_name, members, additional_statement = DB_INS_00_Utils.get_table_name_and_members(sql_file_path)

            logger.debug(table_name)
            logger.debug(members)
            logger.debug(additional_statement)

            # グループごとのカウントを管理する辞書
            group_counts = defaultdict(int)

            # CSVファイルのデータをテーブルに登録
            with open(Trading_History_csv_file_path, 'r', newline='', encoding='utf-8') as csvfile:
                csv_reader = csv.reader(csvfile, delimiter=',', quotechar='"')
                next(csv_reader)  # ヘッダー行をスキップする場合はコメントアウト

                for row_index, row in enumerate(csv_reader, start=1):

                    # 毎回初期化を行う。
                    csv_data = {}

                    processed_row = []
                    for value in row:

                        # カンマが含まれていたら除外する。
                        value = value.strip().replace(',', '')

                        # NULLまたは"-"の場合、Noneに変換する
                        value = None if value in ('', '-') else value
                        processed_row.append(value)

                    # グループのキーを作成
                    group_key = (
                        processed_row[0],  # trade_date
                        processed_row[1],  # settlement_date
                        processed_row[2],  # ticker
                        processed_row[6]   # buy_sell_type
                    )

                    # グループ内のカウントを取得
                    group_count = group_counts[group_key]

                    # Noにカウント値をセット
                    csv_data['No'] = group_count + 1
                    
                    # グループのカウントを更新
                    group_counts[group_key] = csv_data['No']

                    # Trading_Historyのカラムに合わせてデータの作成を行う。
                    csv_data['trade_date'] = processed_row[0]
                    csv_data['settlement_date'] = processed_row[1]
                    csv_data['ticker'] = processed_row[2]
                    csv_data['buy_sell_type'] = processed_row[6]
                    csv_data['security_name'] = processed_row[3]
                    csv_data['account'] = processed_row[4]
                    csv_data['transaction_type'] = processed_row[5]
                    csv_data['credit_type'] = processed_row[7]
                    csv_data['settlement_deadline'] = processed_row[8]
                    csv_data['settlement_currency'] = processed_row[9]
                    csv_data['quantity'] = processed_row[10]
                    csv_data['unit_price'] = processed_row[11]
                    csv_data['execution_price'] = processed_row[12]
                    csv_data['exchange_rate'] = processed_row[13]
                    csv_data['commission'] = processed_row[14]
                    csv_data['tax'] = processed_row[15]
                    csv_data['settlement_amount_usd'] = processed_row[16]
                    csv_data['settlement_amount_jpy'] = processed_row[17]

                    # INSERT文を作成
                    insert_values = []
                    for member in members:
                        logger.debug(f"member:{member}")

                        if member == 'INS_DATE':
                            insdate = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            insert_values.append(insdate)

                        elif member == 'UPD_DATE':
                            insert_values.append(None)

                        else:
                            insert_values.append(csv_data.get(member))
                            logger.debug(f"insert_values:{insert_values}")

                    # INSERT文を実行
                    insert_query = f"""
                    INSERT INTO {tmp_table_name}
                        ({', '.join(members)})
                    VALUES
                        ({', '.join(['%s'] * len(members))})
                    """
                    # insert_query += additional_statement

                    # SQL文を表示またはログファイルに書き込み
                    logger.debug("実行するSQL文:値セット")
                    logger.info(insert_query % tuple(insert_values))  # 値をセットしたSQL文を表示
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
        return table_name, members
        logger.info(f"処理が完了しました。SQLファイル: {sql_file}")

    except Exception as e:
        logger.error("処理中にエラーが発生しました。")
        logger.error(str(e))
        logger.error(traceback.format_exc())  # 修正: トレースバック情報をログに出力
        has_error = True  # エラーフラグを設定

    # エラーフラグをチェックして処理を終了
    if has_error:
        logger.error("エラーが発生したため、処理を終了します。")
        sys.exit(1)  # 修正: エラーが発生した場合にプログラムを終了する
    else:
        logger.info("処理が正常に終了しました。")

def Trading_History_insert_data( insert_sql,logger ):
    try:

        # MySQLに接続
        cnx = DB_Common_Utils.get_mysql_connection()

        # カーソルを取得
        cursor = cnx.cursor()

        # エラーフラグを初期化
        has_error = False

        # 各SQLファイルとシンボルを組み合わせて実行
        try:
            # INSERT文を実行
            cursor.execute(insert_sql)

            cnx.commit()
            logger.info("データを挿入しました。")
            
        except mysql.connector.Error as error:
            logger.error("INSERT文の実行中にエラーが発生しました:", error)
            logger.error("対象行番号:[%d]", row_index)  # 対象行番号をログに出力
            raise  # エラーを再度発生させて処理を終了

        # カーソルと接続を閉じる
        cursor.close()
        cnx.commit()  # トランザクションをコミットする
        cnx.close()
        logger.info(f"処理が完了しました。SQLファイル: {insert_sql}")

    except Exception as e:
        logger.error("処理中にエラーが発生しました。")
        logger.error(str(e))
        logger.error(traceback.format_exc())  # 修正: トレースバック情報をログに出力
        has_error = True  # エラーフラグを設定

    # エラーフラグをチェックして処理を終了
    if has_error:
        logger.error("エラーが発生したため、処理を終了します。")
        sys.exit(1)  # 修正: エラーが発生した場合にプログラムを終了する
    else:
        logger.info("処理が正常に終了しました。")

# テーブルの削除を実行する。
def drop_table(table_name, logger):
    try:
        logger.info("テーブルの削除処理を開始します。")

        # MySQLに接続
        cnx = DB_Common_Utils.get_mysql_connection()

        # カーソルを取得
        cursor = cnx.cursor()

        # テーブルの存在を確認
        if DB_CRE_00_Utils.check_table_existence(table_name, cursor, logger):

            # テーブル削除のSQL文を生成
            query = f"DROP TABLE {table_name}"

            # SQLクエリを実行
            logger.debug(query)
            cursor.execute(query)

            logger.info("テーブルの削除が完了しました。")
        else:
            logger.info("削除対象のテーブルは存在しません。")

    except mysql.connector.Error as err:
        logger.error("テーブルの削除に問題がありました。")
        logger.error("エラーコード: %s", err.errno)
        logger.error("エラーメッセージ: %s", err.msg)
        logger.error("トレースバック情報: %s", traceback.format_exc())  # 修正: トレースバック情報をログに出力

    finally:
        # カーソルと接続を閉じる
        if cursor:
            cursor.close()
        if cnx:
            cnx.close()

        logger.info("カーソルと接続を閉じました。")

