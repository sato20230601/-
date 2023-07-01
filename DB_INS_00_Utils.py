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
import shutil

import requests
from bs4 import BeautifulSoup

# import DB_Common_Utils
import DB_Common_Utils

#
# CSVファイルからデータを読み込み、リストとして返す関数
#     Parameters:
#       csv_file_path (str): CSVファイルのパス
#       logger (Logger): ロガーオブジェクト（デフォルトはNone）
#     Returns:
#        list: CSVデータを格納したリスト
#
def read_csv_data(csv_file_path, logger=None):

    # ログ出力: 開始メッセージと入力データ
    if logger:
        logger.info(f"--- 関数 read_csv_data 開始 ---")
        logger.debug(f"csv_file_path: {csv_file_path}")

    csv_data = []

    with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile, delimiter='\t')

        # ヘッダー行をスキップ
        next(reader)

        # データ行を読み込む
        for row in reader:
            csv_data.append(row)

    # ログ出力: 出力データ
    if logger:
        logger.info(f"--- 関数 read_csv_data 終了 ---")
        logger.debug(f"csv_data: {csv_data}")

    return csv_data

#
# データベースから直近の「取得年月日」とティッカーシンボルを取得する関数
# Parameters:
#     cursor (Cursor): データベースカーソルオブジェクト
#     table_name (str): テーブル名
#     logger (Logger): ロガーオブジェクト
# Returns:
#     list: 直近の「取得年月日」とティッカーシンボルを格納したリスト
#
def get_recent_data(cursor,table_name,logger):

    # ログ出力: 開始メッセージと入力変数
    if logger:
        logger.info(f"--- 関数 get_recent_data 開始 ---")
        logger.debug(f"table_name: {table_name}")

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
        logger.debug(f"戻り値: {result}")

    return result
"""
CSVデータとDBの直近のデータの差分をチェックし、差分があれば差分チェックテーブルに登録し、Trueを返す関数

Parameters:
    cursor (Cursor): データベースカーソルオブジェクト
    table_name (str): テーブル名
    csv_data (list): CSVデータを格納したリスト
    csv_no (int): CSVデータのシンボルのインデックス
    recent_data (list): 直近のデータを格納したリスト
    recent_no (int): 直近のデータのシンボルのインデックス
    logger (Logger): ロガーオブジェクト（デフォルトはNone）

Returns:
    bool: 差分がある場合はTrue、差分がない場合はFalse
"""
def check_diff(cursor, table_name, csv_data, csv_no, recent_data, recent_no, logger=None):
    if logger:
        logger.info(f"--- 関数 check_diff 開始 ---")
        logger.debug(f"table_name: {table_name}")
        logger.debug(f"csv_data: {csv_data}")
        logger.debug(f"recent_data: {recent_data}")

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
        logger.debug(f"csv_symbols: {csv_symbols}")
        logger.debug(f"recent_symbols: {recent_symbols}")

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
        logger.debug(f"diff_flag: {diff_flag}")

    return diff_flag

"""
SQLファイルからテーブル名とメンバーを取得する関数

Parameters:
    sql_file_path (str): SQLファイルのパス

Returns:
    tuple: テーブル名とメンバーのリスト、追加のステートメント
"""
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
"""
テーブルにデータを挿入する関数

Parameters:
    cursor (Cursor): データベースカーソルオブジェクト
    table_name (str): テーブル名
    columns (list): カラム名のリスト
    values (list): 挿入するデータのリスト
    logger (Logger): ロガーオブジェクト
"""
def insert_data_into_table(cursor, table_name, columns, values, logger):

    # ログ出力: 開始メッセージと入力変数
    if logger:
        logger.info(f"--- 関数 insert_data_into_table 開始 ---")
        logger.debug(f"table_name: {table_name}")
        logger.debug(f"columns: {columns}")
        logger.debug(f"values: {values}")

    # INSERT文を構築
    placeholders = ', '.join(['%s'] * len(columns))
    columns_str = ', '.join(columns)
    insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"

    # ログ出力: INSERT文
    if logger:
        logger.debug(f"生成されたINSERT文: {insert_query}")

    # データを挿入
    DB_Common_Utils.execute_sql_query(cursor, insert_query, values, logger)

    if logger:
        logger.info(f"--- 関数 insert_data_into_table 終了 ---")

def update_data_in_table(cursor, table_name, update_columns, values, condition_columns, condition_values, logger):

    # ログ出力: 開始メッセージと入力変数
    if logger:
        logger.info(f"--- 関数 update_data_in_table 開始 ---")
        logger.debug(f"table_name: {table_name}")
        logger.debug(f"update_columns: {update_columns}")
        logger.debug(f"values: {values}")
        logger.debug(f"condition_columns: {condition_columns}")
        logger.debug(f"condition_values: {condition_values}")

    # UPDATE文を構築
    update_columns_str = ', '.join(update_columns)
    condition_str = ' AND '.join([f"{column} = %s" for column in condition_columns])
    update_query = f"UPDATE {table_name} SET {update_columns_str} WHERE {condition_str}"

    # データを更新
    DB_Common_Utils.execute_sql_query(cursor, update_query, values + condition_values, logger)

    if logger:
        logger.info(f"--- 関数 update_data_in_table 終了 ---")

"""
一時テーブルから実テーブルへのINSERT文を生成する関数

Parameters:
    tmp_table_name (str): 一時テーブル名
    table_name (str): 実テーブル名
    members (list): メンバーのリスト
    logger (Logger): ロガーオブジェクト

Returns:
    str: 生成されたINSERT文
"""
def generate_insert_sql(tmp_table_name, table_name, members, logger=None):
    # ログ出力: 開始メッセージと入力変数
    if logger:
        logger.info(f"--- 関数 generate_insert_sql 開始 ---")
        logger.debug(f"tmp_table_name: {tmp_table_name}")
        logger.debug(f"table_name: {table_name}")
        logger.debug(f"members: {members}")

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
    logger.debug(f"select_sql文: {select_sql}")

    # INSERT文の作成
    insert_sql = f"INSERT INTO {table_name} ({', '.join(insert_columns)}) {select_sql}"

    # ログ出力: INSERT文
    if logger:
        logger.debug(f"生成されたINSERT文: {insert_sql}")

    # ログ出力: 終了メッセージと戻り値（正常終了の場合）
    if logger:
        logger.info(f"--- 関数 generate_insert_sql 正常終了 ---")
        logger.debug(f"戻り値: {insert_sql}")

    return insert_sql

def move_processed_csv(csv_file_path,folder_name,logger):

    # 開始ログを出力
    logger.info("CSVファイルの移動処理を開始します。")
    
    # 引数の値をログに出力
    logger.info(f"入力引数 csv_file_path: {csv_file_path}")
    
    # ファイルの存在するフォルダのパスを取得
    csv_directory = os.path.dirname(csv_file_path)
    
    # 「取込済」フォルダのパスを作成
    processed_directory = os.path.join(csv_directory, folder_name)
    
    # 「取込済」フォルダが存在しない場合は作成
    os.makedirs(processed_directory, exist_ok=True)

    # 移動先のファイルパスを作成
    new_file_name = f"{os.path.basename(csv_file_path)}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    new_file_path = os.path.join(processed_directory, new_file_name)
    
    # ファイルの移動
    shutil.move(csv_file_path, new_file_path)
    
    # 終了ログを出力
    logger.info(f"CSVファイルを移動しました。移動先: {new_file_path}")

