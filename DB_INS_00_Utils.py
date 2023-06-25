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

# import DB_Common_Utils
import DB_Common_Utils

def read_csv_data(csv_file_path, logger=None):
    csv_data = []

    with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile, delimiter='\t')

        # ヘッダー行をスキップ
        next(reader)

        # データ行を読み込む
        for row in reader:
            csv_data.append(row)

    # ログ出力: 開始メッセージと入力データ
    if logger:
        logger.info(f"--- 関数 read_csv_data 開始 ---")
        logger.info(f"csv_file_path: {csv_file_path}")

    # ログ出力: 出力データ
    if logger:
        logger.info(f"csv_data: {csv_data}")

    return csv_data

def get_recent_data(cursor,table_name,logger):

    # ログ出力: 開始メッセージと入力変数
    if logger:
        logger.info(f"--- 関数 get_recent_data 開始 ---")
        logger.info(f"table_name: {table_name}")

    # 直近の「取得年月日」とティッカーシンボルを取得するSQL文
    query = """
    SELECT `Date_YYYYMMDD`, `Symbol` FROM %s
    """
    query = query % table_name

    # SQL文を実行
    cursor.execute(query)

    # 結果を取得
    result = cursor.fetchall()

    # ログ出力: 終了メッセージと戻り値（正常終了の場合）
    if logger:
        logger.info(f"--- 関数 get_recent_data 正常終了 ---")
#        logger.info(f"戻り値: {result}")

    return result

# CSVのデータとDBの直近のデータの比較を行い、差分があれば差分チェックテーブルに登録を行いTRUEを返す。
# なければFALSEを返す。
def check_diff(cursor, table_name, csv_data, csv_no, recent_data, recent_no, logger=None):
    if logger:
        logger.info(f"--- 関数 check_diff 開始 ---")
        logger.info(f"table_name: {table_name}")
        logger.info(f"csv_data: {csv_data}")
        logger.info(f"recent_data: {recent_data}")

    if not csv_data:
        if logger:
            logger.info("--- 関数 check_diff 終了 FALSE ---")
            logger.info("CSVデータが空です。処理をスキップします。")
        return False

    if not recent_data:
        if logger:
            logger.info("--- 関数 check_diff 終了 TRUE ---")
            logger.info("DBの直近データが空です。処理をスキップし、csv_dataの内容をテーブルに登録します。")
        return True

    diff_flag = False
    csv_symbols = [row[csv_no] for row in csv_data]
    recent_symbols = [row[recent_no] for row in recent_data]
    execution_date = datetime.now().strftime("%Y-%m-%d")

    if logger:
        logger.info(f"csv_symbols: {csv_symbols}")
        logger.info(f"recent_symbols: {recent_symbols}")

    for symbol in csv_symbols:
        if symbol not in recent_symbols:
            diff_flag = True
            insert_query = """
            INSERT INTO `{table_name}` (`Date_YYYYMMDD`, `Symbol`, `Action`, `UPD_DATE`)
            VALUES (%s, %s, '追加', %s)
            ON DUPLICATE KEY UPDATE `Action` = VALUES(`Action`), `UPD_DATE` = VALUES(`UPD_DATE`)
            """
            insert_query = insert_query.format(table_name=table_name)
            insert_values = (execution_date, symbol, datetime.now())
            cursor.execute(insert_query, insert_values)

    for symbol in recent_symbols:
        if symbol not in csv_symbols:
            diff_flag = True
            insert_query = """
            INSERT INTO `{table_name}` (`Date_YYYYMMDD`, `Symbol`, `Action`, `UPD_DATE`)
            VALUES (%s, %s, '削除', %s)
            ON DUPLICATE KEY UPDATE `Action` = VALUES(`Action`), `UPD_DATE` = VALUES(`UPD_DATE`)
            """
            insert_query = insert_query.format(table_name=table_name)
            insert_values = (execution_date, symbol, datetime.now())
            cursor.execute(insert_query, insert_values)

    if logger:
        logger.info(f"--- 関数 check_diff 終了 ---")
        logger.info(f"diff_flag: {diff_flag}")

    return diff_flag

# SQLファイル(INSERT文)読み込み、テーブル名、カラム名、条件部分(ON句以降)を取得する。
def get_table_name_and_members(sql_file_path):
    try:
        with open(sql_file_path, 'r') as file:
            insert_query = file.read()

        start_index = insert_query.index('(') + 1
        end_index = insert_query.index(')', start_index)
        members = [m.strip() for m in insert_query[start_index:end_index].split(',')]
        table_name = insert_query.split()[2]

        # VALUESの次の行にある追加のステートメントを取得
        values_index = insert_query.index('VALUES')
        next_line_index = insert_query.index('\n', values_index)
        additional_statement = insert_query[next_line_index:]

        # on_index = insert_query.index('ON')
        # additional_statement = insert_query[on_index:]

        return table_name, members, additional_statement

    except FileNotFoundError:
        logger.error(f"ファイルが存在しません: {sql_file_path}")
        return ""

def insert_data_into_table(cursor, table_name, columns, values, logger):

    # ログ出力: 開始メッセージと入力変数
    if logger:
        logger.info(f"--- 関数 insert_data_into_table 開始 ---")
        logger.info(f"table_name: {table_name}")
        logger.info(f"columns: {columns}")
        logger.info(f"values: {values}")

    # INSERT文を構築
    placeholders = ', '.join(['%s'] * len(columns))
    columns_str = ', '.join(columns)
    insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"

    # ログ出力: INSERT文
    if logger:
        logger.info(f"生成されたINSERT文: {insert_query}")

    # データを挿入
    DB_Common_Utils.execute_sql_query(cursor, insert_query, values, logger)

def update_data_in_table(cursor, table_name, update_columns, values, condition_columns, condition_values, logger):
    # UPDATE文を構築
    update_columns_str = ', '.join(update_columns)
    condition_str = ' AND '.join([f"{column} = %s" for column in condition_columns])
    update_query = f"UPDATE {table_name} SET {update_columns_str} WHERE {condition_str}"

    # データを更新
    DB_Common_Utils.execute_sql_query(cursor, update_query, values + condition_values, logger)

# 重複しないレコードの登録用INSERT文の作成
# 呼び出し元関数
# DB_CRE_07_Trading_History
def generate_insert_sql(tmp_table_name, table_name, members, logger=None):
    # ログ出力: 開始メッセージと入力変数
    if logger:
        logger.info(f"--- 関数 generate_insert_sql 開始 ---")
        logger.info(f"tmp_table_name: {tmp_table_name}")
        logger.info(f"table_name: {table_name}")
        logger.info(f"members: {members}")

    # membersの数を取得
    num_members = len(members)

    # SELECT文の作成
    select_columns = []
    insert_columns = []
    for member in members:

        # INSERT句の作成
        insert_columns.append(member)

        # SELECT句の作成
        if member == 'INS_DATE':
            select_columns.append('CURRENT_TIMESTAMP')
        elif member == 'UPD_DATE':
            select_columns.append('NULL')
        else:
            select_columns.append(f"TMP_TABLE.{member}")

    select_sql = (
        f"SELECT {', '.join(select_columns)} FROM "
        f"{tmp_table_name} TMP_TABLE "
        f"LEFT JOIN {table_name} REAL_TABLE "
        "ON "
        "REAL_TABLE.trade_date = TMP_TABLE.trade_date AND "
        "REAL_TABLE.settlement_date = TMP_TABLE.settlement_date AND "
        "REAL_TABLE.ticker = TMP_TABLE.ticker AND "
        "REAL_TABLE.buy_sell_type = TMP_TABLE.buy_sell_type AND "
        "REAL_TABLE.No = TMP_TABLE.No"
    )

    # WHERE句を追加してREAL_TABLEにデータが存在しないレコードを抽出
    select_sql += (
        " WHERE REAL_TABLE.No is NULL OR "
        "REAL_TABLE.No < TMP_TABLE.No OR "
        "REAL_TABLE.quantity <> TMP_TABLE.quantity"
        " ORDER BY 1,2,3,4,5"
    )
    logger.info(f"select_sql文: {select_sql}")

    # INSERT文の作成
    insert_sql = f"INSERT INTO {table_name} ({', '.join(insert_columns)}) {select_sql}"

    # ログ出力: INSERT文
    if logger:
        logger.info(f"生成されたINSERT文: {insert_sql}")

    # ログ出力: 終了メッセージと戻り値（正常終了の場合）
    if logger:
        logger.info(f"--- 関数 generate_insert_sql 正常終了 ---")
        logger.info(f"戻り値: {insert_sql}")

    return insert_sql
