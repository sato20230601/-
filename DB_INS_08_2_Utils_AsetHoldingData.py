import os
import csv
import codecs
import shutil
import datetime

from datetime import datetime, date, timedelta

import logging
import traceback
import requests

import mysql.connector
from configparser import ConfigParser
from collections import defaultdict

import sys
import re

import xml.etree.ElementTree as ET

# 登録更新処理共通
import DB_INS_00_Utils

# 共通関数の読み込み
import DB_Common_Utils

# SQLファイル情報のクラス
class SQLFileInfo:
    def __init__(self, table_name, members, additional_statement, sql_file_path):
        self.table_name = table_name
        self.members = members
        self.additional_statement = additional_statement
        self.sql_file_path = sql_file_path
        self.data = None  # データを保持するプロパティ

def validate_row(row, current_table, logger):

    logger.info(f"関数 validate_row の実行開始")
    logger.debug(f"入力引数: row={row}, current_table={current_table}")

    # 空行のチェック
    if not row:
        logger.debug("Empty row")
        return False

    if current_table == 'AssetData':
        # AssetDataテーブルのカラムチェックを実装する
        # カラム数が12であることを確認
        if len(row) != 12:
            logger.debug("Invalid number of columns in AssetData row")
            return False

    elif current_table == 'HoldingDetails':
        # HoldingDetailsテーブルのカラムチェックを実装する
        # カラム数が18であることを確認
        if len(row) != 18:
            logger.debug("Invalid number of columns in HoldingDetails row")
            return False

    elif current_table == 'ExchangeRate':
        # ExchangeRateテーブルのカラムチェックを実装する
        # カラム数が4であることを確認
        if len(row) != 4:
            logger.debug("Invalid number of columns in ExchangeRate row")
            return False

    logger.info(f"関数 validate_row 正常終了")

    return True

def extract_date_from_csv_filename(csv_file_path,logger):
    # ファイル名から年月日を抽出する正規表現パターンを定義します
    pattern = r"\d{8}"  # 8桁の数字

    # ファイル名から年月日を抽出します
    match = re.search(pattern, csv_file_path)
    if match:
        return match.group()
    else:
        return ""

def parse_date_string(date_string,logger):
    # 年月日の文字列を解析してDate型に変換します
    try:
        logger.info("関数 parse_date_string の実行開始")
        logger.debug(f"入力引数: date_string={date_string}")

        year = int(date_string[0:4])
        month = int(date_string[4:6])
        day = int(date_string[6:8])

        logger.info("関数 parse_date_string の正常終了")
        logger.debug(f"戻り値: year={year}:month={month}:day={day}")

        return date(year, month, day)

    except ValueError:
        return None

def insert_data(cursor, table_name, members, additional_statement, table_data, logger):
    logger.info("関数 insert_data の実行開始")
    logger.debug(f"入力引数: cursor={cursor}, table_name={table_name}, members={members}, additional_statement={additional_statement}, table_data={table_data}")

    # INSERT文のカラム部分を動的に生成
    column_names = ", ".join(members)

    # INSERT文のVALUES部分を動的に生成
    value_placeholders = ", ".join(["%s"] * len(members))

    # INSERT文を作成
    insert_statement = f"INSERT INTO {table_name} ({column_names}) VALUES ({value_placeholders}) {additional_statement}"
    logger.debug(f"insert_statement={insert_statement}")

    # データの挿入
    for row_index, row in enumerate(table_data):

        # 最初の行をスキップ
        if row_index == 0 and (table_name == "AssetData" or table_name == "HoldingDetails"):
            continue

        # 登録日時を取得
        ins_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # データの挿入値を作成
        row_values = []
        for value in row:
            if value == "" or value == "-":
                row_values.append(None)
            else:
                # 文字列に含まれるカンマを除外する
                if isinstance(value, str):
                    value = value.replace(",", "")

                # カラム値を追加する
                row_values.append(value)

        # INS_DATEとUPD_DATEの値を追加
        row_values.append(ins_date)
        row_values.append(None)  # UPD_DATEはNULL

        try:
            # データの挿入
            logger.debug(f"insert_statement={insert_statement}")
            logger.debug(f"row_values={tuple(row_values)}")  # row_valuesをタプルとしてログ出力

            logger.debug(insert_statement.format(*row_values))
            cursor.execute(insert_statement, tuple(row_values))

        except Exception as e:
            logger.error(f"Failed to insert data into {table_name} table: {str(e)}")
            logger.error(f"エラーログ: エラー内容: {traceback.format_exc()}")
            return False  # 挿入失敗した場合はFalseを返す

    logger.info("関数 insert_data の正常終了")
    return True  # 挿入成功した場合はTrueを返す

def AsetHoldingData_insert(cursor, sql_file_path, AsetHoldingData_csv_file_path, logger):
    try:
        logger.info(f"関数 AsetHoldingData_insert の実行開始")
        logger.debug(f"入力引数: cursor={cursor}, file_path={sql_file_path}, AsetHoldingData_csv_file_path={AsetHoldingData_csv_file_path}")

        # SQLファイルパスからSQLディレクトリパスとSQLファイル名を読み込む
        with open(sql_file_path, 'r', encoding='utf-8') as sql_file:
            lines = sql_file.readlines()

        if len(lines) < 2:
            logger.error("ファイルの内容が正しくありません。")
            return

        # SQLディレクトリパスの取得
        sql_directory_path = lines[0].strip()

        # SQLファイル名の取得
        sql_files = [line.strip() for line in lines[1:]]

        # ループの外でエラーフラグを初期化
        has_error = False

        # 各SQLファイルの情報を保持するリスト
        sql_file_info = []

        # 各SQLファイルとシンボルを組み合わせて実行
        for sql_file in sql_files:
            sql_file_path = f"{sql_directory_path}/{sql_file}"

            logger.debug(f"sql_file_path={sql_file_path}")

            table_name, members, additional_statement = DB_Common_Utils.get_table_name_and_members(sql_file_path)

            logger.debug(f"table_name={table_name}")
            logger.debug(f"members={members}")
            logger.debug(f"additional_statement={additional_statement}")

            # 各SQLファイルとシンボルを組み合わせて情報を取得し、リストに追加
            sql_file_info.append(SQLFileInfo(table_name, members, additional_statement, sql_file_path))

        logger.debug("sql_file_infoのデータ確認")
        for sql_info in sql_file_info:
            logger.debug(f"Table Name: {sql_info.table_name}")
            logger.debug(f"Members: {sql_info.members}")
            logger.debug(f"additional_statement: {sql_info.additional_statement}")
            logger.debug(f"sql_file_path: {sql_info.sql_file_path}")

        # CSVファイル名から年月日を取得してDate型に変換する。
        date_string = extract_date_from_csv_filename(AsetHoldingData_csv_file_path,logger)
        date_value = parse_date_string(date_string,logger)

        logger.debug(f"date_value={date_value}")

        # CSVファイルのデータを「AssetData」「HoldingDetails」「ExchangeRate」の順に登録
        with codecs.open(AsetHoldingData_csv_file_path, 'r', 'sjis', 'utf-8') as utf_file:
            csv_reader = csv.reader(utf_file, delimiter=',', quotechar='"')
            # next(csv_reader)  # ヘッダー行をスキップしない場合はコメントアウト

            asset_data = []
            holding_details = []
            exchange_rate = []

            current_table = None
            for row in csv_reader:

                logger.debug(f"csv_row: {row}")
                if row:
                    logger.debug(f"row[0]: {row[0]}")
                    if '資産合計欄' in row[0]:
                        current_table = 'AssetData'
                        continue
                    elif '保有商品詳細'in row[0]:
                        current_table = 'HoldingDetails'
                        continue
                    elif '参考為替レート'in row[0]:
                        current_table = 'ExchangeRate'
                        continue

                # CSVファイルのテーブル毎の仕分けを行う。
                if validate_row(row, current_table,logger):

                    # CSVファイル名から年月日を取得してDate_YYYYMMDD"カラムにセット
                    row.insert(0, date_value)

                    if current_table == 'AssetData':
                        del row[9]  # 9番目のカラムを削除
                        asset_data.append(row)
                    elif current_table == 'HoldingDetails':
                        holding_details.append(row)
                    elif current_table == 'ExchangeRate':

                        # LastUpdateのDatetime型への変換処理を追加
                        last_update_str = row[4]  # LastUpdateの文字列を取得
                        last_update_str = last_update_str.replace('"', '')  # 不要なダブルクォートを削除
                        last_update = datetime.strptime(last_update_str, "(%m/%d  %H:%M)")

                        # LastUpdateを追加
                        row[4] = last_update
                        exchange_rate.append(row)

            logger.debug("asset_data:")
            for data in asset_data:
                logger.debug(data)

            logger.debug("holding_details:")
            for data in holding_details:
                logger.debug(data)

            logger.debug("exchange_rate:")
            for data in exchange_rate:
                logger.debug(data)

        # テーブルごとのINSERT文作成とデータの登録
        for sql_info in sql_file_info:

            table_name = sql_info.table_name
            members = sql_info.members
            additional_statement = sql_info.additional_statement

            if table_name == 'AssetData':
                table_data = asset_data

            elif table_name == 'HoldingDetails':
                table_data = holding_details

            elif table_name == 'ExchangeRate':
                table_data = exchange_rate

            else:
                logger.error(f"テーブル名が不正です。table_name={table_name}")
                has_error = True
                break

           # insert_data関数を呼び出し、戻り値をチェックする
            if not insert_data(cursor, table_name, members, additional_statement, table_data, logger):
                logger.error(f"{table_name}テーブルへのデータの挿入に失敗しました。")
                has_error = True
                break

       # エラーフラグの処理
        if has_error:
            logger.info("関数 insert_data の異状終了")
            return False
        else:
            logger.info("関数 insert_data の正常終了")
            return True

    except FileNotFoundError:
        logger.error(f"ファイルが存在しません: {sql_file_path}")
        return False

    except ValueError:
        logger.error(f"SQLファイルの形式が無効です: {sql_file_path}")
        return False
