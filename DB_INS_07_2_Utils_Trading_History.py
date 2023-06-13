import os
import csv
from datetime import datetime, date, timedelta

import logging
import traceback
import requests

import mysql.connector
from configparser import ConfigParser

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

            # CSVファイルのデータをテーブルに登録
            with open(Trading_History_csv_file_path, 'r', newline='', encoding='utf-8') as csvfile:
                csv_reader = csv.reader(csvfile, delimiter=',', quotechar='"')
                next(csv_reader)  # ヘッダー行をスキップする場合はコメントアウト

                for row_index, row in enumerate(csv_reader, start=1):
                    # 空白を除外してデータを整形
                    row = [value.strip() for value in row]
                    logger.debug(f"row: {row}")

                    # INSERT文を実行
                    insert_values = []
                    for member, value in zip(members, row):
                        logger.debug(f"member:{member}")
                        logger.debug(f"value:{value}")

                         # カンマが含まれていたら除外する。
                        value = value.replace(',', '')

                        if member == 'INS_DATE':
                            insdate = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            insert_values.append(insdate)
                        elif member == 'UPD_DATE':
                            insert_values.append(None)

                        else:
                            # NULLまたは"-"の場合、Noneに変換する
                            insert_values.append(None if value in ('', '-') else value)
                            logger.debug(f"insert_values:{insert_values}")

                    # INSERT文を実行
                    insert_query = f"""
                    INSERT INTO {tmp_table_name}
                        ({', '.join(members)})
                    VALUES
                        ({', '.join(['%s'] * len(members))})
                    """
                    # insert_query += additional_statement
                    insert_values.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))  # INS_DATEの値を追加
                    insert_values.append(None)  # UPT_DATEの値を追加

                    # SQL文を表示またはログファイルに書き込み
                    logger.debug("実行するSQL文:insert_query")
                    logger.debug(insert_query)
                    logger.debug("実行するSQL文:insert_values")
                    logger.debug(insert_values)
                    logger.debug("実行するSQL文:insert_query2")
                    logger.debug(insert_query)
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

