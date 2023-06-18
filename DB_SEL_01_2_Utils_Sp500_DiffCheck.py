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

# 共通関数の読み込み
import DB_Common_Utils

# SP500から取得年月日を取得する
def get_unique_dates(cursor,logger):

    logger.info(f"SP500から取得年月日の取得処理を開始します。")

    query = """
    SELECT DISTINCT Date_YYYYMMDD
    FROM sp500
    ORDER BY Date_YYYYMMDD
    """

    logger.debug(f"SP500から取得年月日取得クエリ: {query}")
    cursor.execute(query)
    results = cursor.fetchall()

    unique_dates = [row[0] for row in results]

    # 取得年月日の内容をログに出力
    for date in unique_dates:
        logger.debug(f"取得年月日: {date}")

    logger.info(f"SP500から取得年月日の取得処理を終了しました。")
    return unique_dates

# 差分チェックを実行して結果を取得
def get_diff_records(cursor, old_date, new_date,logger):

    logger.info(f"差分チェック処理を開始します。")
    has_error = False  # エラーフラグを初期化
    try:
        diff_check_query = """
        SELECT
          NEW_SP500.Date_YYYYMMDD,
          NEW_SP500.Symbol
        FROM
          (SELECT
            Date_YYYYMMDD,
            Symbol
           FROM sp500
           WHERE
             Date_YYYYMMDD = %s
           ) NEW_SP500
        LEFT JOIN
          (SELECT
            Date_YYYYMMDD,
            Symbol
          FROM sp500
          WHERE
            Date_YYYYMMDD = %s
          ) OLD_SP500
        ON
          NEW_SP500.Symbol = OLD_SP500.Symbol
        WHERE
          OLD_SP500.Symbol IS NULL
        """

        # SQL文をログに出力
        logger.debug(f"差分チェッククエリ: {diff_check_query % (new_date, old_date)}")

        cursor.execute(diff_check_query, (new_date, old_date))
        diff_results = cursor.fetchall()

        logger.info(f"差分チェック処理を終了しました。")

        return diff_results

    except Exception as e:
        logger.error("処理中にエラーが発生しました。")
        logger.error(str(e))
        logger.error(traceback.format_exc())  # トレースバック情報をログに出力
        has_error = True  # エラーフラグを設定
        return []

# 差分チェック結果を「sp500_diff_record」に登録を行う。
def insert_diff_records(cursor, diff_records, logger):
    has_error = False  # エラーフラグを初期化
    try:
        logger.info(f"差分結果の登録処理を開始します。")
        for record in diff_records:
            insert_query = """
            INSERT INTO `sp500_diff_record` (`Date_YYYYMMDD`, `Symbol`, `Action`, `INS_DATE`)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE `Action` = VALUES(`Action`), `UPD_DATE` = VALUES(`UPD_DATE`)
                """

            # INSERT文と値をログに出力
            logger.debug(f"INSERTクエリ: {insert_query % (record['Date_YYYYMMDD'], record['Symbol'], record['Action'], datetime.now())}")

            cursor.execute(insert_query, (record['Date_YYYYMMDD'], record['Symbol'], record['Action'], datetime.now()))

        logger.info(f"差分結果の登録処理が終了しました。")

    except Exception as e:
        logger.error("差分結果の登録処理中にエラーが発生しました。")
        logger.error(str(e))
        logger.error(traceback.format_exc())  # トレースバック情報をログに出力
        has_error = True  # エラーフラグを設定
        return []

    return []

def Sp500_DiffCheck_process_data(logger):

    has_error = False  # エラーフラグを初期化
    try:
        logger.info(f"SP500の取得年月毎の差分チェックを開始します。")

        # MySQLに接続
        cnx = DB_Common_Utils.get_mysql_connection()

        # カーソルを取得
        cursor = cnx.cursor()

        # SP500から取得年月日を取得する
        unique_dates = get_unique_dates(cursor, logger)

        # 取得日付が1つ以下の場合はスキップして処理を終了
        if len(unique_dates) <= 1:
            cursor.close()
            cnx.close()
            return

        # 差分チェック結果のデータを格納するリスト
        diff_records = []

        # 取得日付ごとに差分チェックを行う
        for i in range(1, len(unique_dates)):
            old_date = unique_dates[i-1]
            new_date = unique_dates[i]

            # 差分チェックを実行して結果を取得
            diff_results = get_diff_records(cursor, old_date, new_date,logger)
            diff_results2 = get_diff_records(cursor, new_date, old_date,logger)

            # 結果を変数に格納
            for diff in diff_results:
                diff_records.append({
                'Date_YYYYMMDD': new_date,
                'Symbol': diff[1],
                'Action': '削除'
                })

            for diff2 in diff_results2:
                diff_records.append({
                    'Date_YYYYMMDD': new_date,
                    'Symbol': diff2[1],
                    'Action': '追加'
                })

        # 差分チェック処理結果を表示
        for record in diff_records:
            logger.debug(f"差分チェック結果: {record}")

        # 差分チェック処理結果を「sp500_diff_record」に登録
        insert_diff_records(cursor,diff_records ,logger)

        # カーソルと接続を閉じる
        cursor.close()
        cnx.commit()  # トランザクションをコミットする
        cnx.close()
        logger.info(f"SP500の取得年月毎の差分チェック処理が完了しました。")

    except Exception as e:
        logger.error("処理中にエラーが発生しました。")
        logger.error(str(e))
        logger.error(traceback.format_exc())  # 修正: トレースバック情報をログに出力
        has_error = True  # エラーフラグを設定

    # エラーフラグをチェックして処理を終了
    if has_error:
        logger.error("エラーが発生したため、処理を終了します。")
        cursor.close()
        cnx.close()
        return

