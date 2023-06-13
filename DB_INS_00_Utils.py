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

# import DB_Common_Utils
import DB_Common_Utils

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

    # INSERT文を構築
    placeholders = ', '.join(['%s'] * len(columns))
    columns_str = ', '.join(columns)
    insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"

    # データを挿入
    DB_Common_Utils.execute_sql_query(cursor, insert_query, values, logger)

def update_data_in_table(cursor, table_name, update_columns, values, condition_column, condition_value, logger):

    # UPDATE文を構築
    update_columns_str = ', '.join(update_columns)
    update_query = f"UPDATE {table_name} SET {update_columns_str} WHERE {condition_column} = %s"

    # データを更新
    DB_Common_Utils.execute_sql_query(cursor, update_query, values + [condition_value], logger)

# 重複しないレコードの登録用INSERT文の作成
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
    for member in members:
        if member == 'id':
            continue
        elif member == 'INS_DATE':
            select_columns.append('CURRENT_TIMESTAMP')
        elif member == 'UPD_DATE':
            select_columns.append('NULL')
        else:
            select_columns.append(f"TMP_TABLE.{member}")

    select_sql = f"SELECT {', '.join(select_columns)} FROM {tmp_table_name} AS TMP_TABLE LEFT JOIN {table_name} AS REAL_TABLE ON "

    # ON句の条件を追加
    for i in range(num_members -2 ):
        if members[i] not in ['id', 'credit_type', 'settlement_deadline', 'tax', 'commission', 'settlement_amount_usd']:
            if i > 0:
                select_sql += " AND "
            select_sql += f"TMP_TABLE.{members[i]} = REAL_TABLE.{members[i]}"

    # WHERE句を追加してREAL_TABLEにデータが存在しないレコードを抽出
    select_sql += f" WHERE REAL_TABLE.id IS NULL"

    # INSERT文の作成
    insert_sql = f"INSERT INTO {table_name} ({', '.join(members)}) {select_sql}"

    # ログ出力: INSERT文
    if logger:
        logger.info(f"生成されたINSERT文: {insert_sql}")

    # ログ出力: 終了メッセージと戻り値（正常終了の場合）
    if logger:
        logger.info(f"--- 関数 generate_insert_sql 正常終了 ---")
        logger.info(f"戻り値: {insert_sql}")

    return insert_sql

