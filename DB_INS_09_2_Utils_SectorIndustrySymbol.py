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

# 新たな関数: SP500からセクターや業種の情報を取得する関数
def get_sector_industry_sp500_data(cursor):
    select_statement = """
    SELECT
        SP.GICS_Sector as "sector",
        SP.GICS_Sub_Industry as "industry"
    FROM sp500 SP
    GROUP BY
        SP.GICS_Sector,
        SP.GICS_Sub_Industry
    ORDER BY
        1, 2
    """
    cursor.execute(select_statement)
    return cursor.fetchall()

# 新たな関数: SP500からセクターや業種の情報を取得する関数
def get_sector_industry_symbol_sp500_data(cursor):
    select_statement = """
    SELECT
        SP.GICS_Sector as "sector",
        SP.GICS_Sub_Industry as "industry",
        SP.Symbol as "Symbol"
    FROM sp500 SP
    GROUP BY
        SP.GICS_Sector,
        SP.GICS_Sub_Industry,
        SP.Symbol
    ORDER BY
        1, 2, 3
    """
    cursor.execute(select_statement)
    return cursor.fetchall()

# 新たな関数: 取得したセクターや業種の情報をCSVに出力する関数
def write_data_to_csv(data, csv_file):
    with open(csv_file, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file, delimiter='\t')  # タブ区切りの設定
#        writer.writerow(['Sector', 'Industry'])  # ヘッダー行の書き込み
        writer.writerows(data)  # データの書き込み

# SQLファイル情報のクラス
class SQLFileInfo:
    def __init__(self, table_name, members, additional_statement, sql_file_path):
        self.table_name = table_name
        self.members = members
        self.additional_statement = additional_statement
        self.sql_file_path = sql_file_path
        self.data = None  # データを保持するプロパティ

def insert_data(cursor, table_name, members, additional_statement, csv_data, logger):
    try:
        logger.info(f"関数 insert_data の実行開始")
        logger.debug(f"table_name: {table_name}")
        logger.debug(f"members: {members}")
        logger.debug(f"additional_statement: {additional_statement}")

        # テーブルのカラム数を取得
        num_columns = len(members)

        # INSERT文の作成
        insert_statement = f"INSERT INTO {table_name} ({', '.join(members)}) VALUES ({', '.join(['%s']*num_columns)})"

        # 追加のステートメントがある場合は結合
        if additional_statement:
            insert_statement += f" {additional_statement}"

        logger.debug(f"insert_statement: {insert_statement}")

        # データの挿入

        # 登録日時を取得
        ins_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for row_num, row in enumerate(csv_data, start=1):

            # INS_DATEとUPD_DATEの値を追加
            row.append(ins_date)
            row.append(None)  # UPD_DATEはNULL

            # レコードの長さがカラム数に一致しない場合はスキップ
            if len(row) != (num_columns):
                logger.warning("カラム数が一致しないため、データの挿入をスキップしました。")
                continue

            # データの挿入
            logger.debug(f"行番号: {row_num}, データを挿入します: {row}")

            cursor.execute(insert_statement, row)

        logger.info(f"{table_name}テーブルへのデータの挿入が完了しました。")

        return True

    except Exception as e:
        logger.exception(f"{table_name}テーブルへのデータの挿入中にエラーが発生しました。")
        return False

def get_sql_file_info(sql_file_path,logger):
    """
    SQLファイルの情報を取得します。

    Args:
        sql_file_path (str): SQLファイルのパス

    Returns:
        SQLFileInfo: SQLファイルの情報を保持するオブジェクト
    """
    with open(sql_file_path, 'r', encoding='utf-8') as sql_file:
        lines = sql_file.readlines()

    if len(lines) < 2:
        logger.error("ファイルの内容が正しくありません。")
        return None

    # SQLディレクトリパスの取得
    sql_directory_path = lines[0].strip()

    # SQLファイル名の取得
    sql_files = [line.strip() for line in lines[1:]]

    # 各SQLファイルの情報を保持するリスト
    sql_file_info = []

    # 各SQLファイルとシンボルを組み合わせて実行
    for sql_file in sql_files:
        sql_file_path = f"{sql_directory_path}/{sql_file}"

        logger.debug(f"sql_file_path={sql_file_path}")

        table_name, members, additional_statement = DB_INS_00_Utils.get_table_name_and_members(sql_file_path)

        logger.debug(f"table_name={table_name}")
        logger.debug(f"members={members}")
        logger.debug(f"additional_statement={additional_statement}")

        # 各SQLファイルとシンボルを組み合わせて情報を取得し、リストに追加
        sql_file_info.append(SQLFileInfo(table_name, members, additional_statement, sql_file_path))

    return sql_file_info

def get_en_words(cursor):
    # en_wordsテーブルから登録されている英語の一覧を取得する関数
    select_statement = "SELECT english FROM en_words"
    cursor.execute(select_statement)
    return [row[0] for row in cursor.fetchall()]

def insert_en_word(cursor, english_text):
    # en_wordsテーブルに英語の情報を登録する関数
    insert_statement = "INSERT INTO en_words (english, INS_DATE) VALUES (%s, %s) ON DUPLICATE KEY UPDATE UPD_DATE = %s"
    current_date = datetime.now()
    cursor.execute(insert_statement, (english_text, current_date, current_date))
    en_id = get_en_id(cursor, english_text)
    return en_id

def get_en_id(cursor, english_text):
    # en_wordsテーブルから指定された英語の情報に対応するen_idを取得する関数
    select_statement = "SELECT en_id FROM en_words WHERE english = %s"
    cursor.execute(select_statement, (english_text,))
    return cursor.fetchone()[0]

def get_jp_words(cursor):
    # jp_wordsテーブルから登録されている英語の一覧を取得する関数
    select_statement = "SELECT japanese FROM jp_words"
    cursor.execute(select_statement)
    return [row[0] for row in cursor.fetchall()]

def insert_jp_word(cursor, japanese_text):
    # jp_wordsテーブルに日本語の情報を登録する関数
    insert_statement = "INSERT INTO jp_words (japanese, INS_DATE) VALUES (%s, %s) ON DUPLICATE KEY UPDATE UPD_DATE = %s"
    current_date = datetime.now()
    cursor.execute(insert_statement, (japanese_text, current_date, current_date))
    jp_id = get_jp_id(cursor, japanese_text)
    return jp_id

def get_jp_id(cursor, japanese_text):
    # jp_wordsテーブルから指定された英語の情報に対応するjp_idを取得する関数
    select_statement = "SELECT jp_id FROM jp_words WHERE japanese = %s"
    cursor.execute(select_statement, (japanese_text,))
    return cursor.fetchone()[0]

def insert_en_jp_translation_data(cursor, csv_file, logger):
    try:
        logger.info(f"関数 insert_en_jp_translation_data の実行開始")

        # en_words,jp_wordsテーブルに登録されている一覧を取得 
        en_words_set = set(get_en_words(cursor))
        jp_words_set = set(get_jp_words(cursor))

        # csvファイルの読み込み
        with open(csv_file, 'r', encoding='utf-8') as file:
            csv_data = csv.reader(file, delimiter='\t')

            # 英語と日本語の情報を登録および取得し、英語-日本語を登録
            for row in csv_data:
                en_text = row[0]
                jp_text = row[1]

                logger.debug(f"英語: {en_text}, 日本語: {jp_text}")

                # 英語の情報を登録および取得
                if en_text not in en_words_set:
                    # en_wordsテーブルにセクターの情報を登録
                    logger.debug(f"英語を登録します: {en_text}")
                    en_id =  insert_en_word(cursor,en_text)
                    logger.debug(f"英語の登録が完了しました。en_id: {en_id}")

                else:
                    # en_wordsテーブルからen_idを取得
                    logger.debug(f"英語の情報が既に存在します: {en_text}")
                    en_id = get_en_id(cursor, en_text)

                # 日本語の情報を登録および取得
                if jp_text not in jp_words_set:
                    # jp_wordsテーブルに業種の情報を登録
                    logger.debug(f"日本語を登録します: {jp_text}")
                    jp_id =  insert_jp_word(cursor,jp_text)
                    logger.debug(f"日本語の登録が完了しました。jp_id: {jp_id}")

                else:
                    # jp_wordsテーブルからjp_idを取得
                    logger.debug(f"日本語の情報が既に存在します: {jp_text}")
                    jp_id = get_jp_id(cursor, jp_text)

                # en_jpテーブルに英語-日本語を登録
                logger.debug(f"英語-日本語を登録します: en_id={en_id},jp_id={jp_id}")
                insert_en_jp_translation(cursor, en_id, jp_id)
                logger.debug("英語-日本語の登録が完了しました。")

        logger.info(f"英語-日本語の登録が完了しました。")
        return True

    except Exception as e:
        logger.exception("英語-日本語データの登録中にエラーが発生しました。")
        return False

def insert_en_jp_translation(cursor, en_id, jp_id):
    # en_jpテーブルに英語-日本語データを登録する関数
    insert_statement = "INSERT INTO en_jp_translation (en_id, jp_id, INS_DATE) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE UPD_DATE = %s"
    current_date = datetime.now()
    cursor.execute(insert_statement, (en_id, jp_id, current_date, current_date))

def get_sector(cursor):
    # sectorテーブルから指定されたセクター名の一覧を取得する関数
    select_statement = "SELECT en_words.english FROM sector JOIN en_words ON sector.en_id = en_words.en_id"
    cursor.execute(select_statement)
    return [row[0] for row in cursor.fetchall()]

def insert_sector_data(cursor, csv_file, logger):
    try:
        logger.info(f"関数 insert_sector_data の実行開始")

        # sectorテーブルに登録されている英語の一覧を取得
        sector_set = set(get_sector(cursor))

        # csvファイルの読み込み
        with open(csv_file, 'r', encoding='utf-8') as file:
            csv_data = csv.reader(file)
            
            # 英語の情報を登録および取得し、セクターを登録
            for row in csv_data:
                sector_text = row[0]
                if sector_text not in sector_set:
                    # sectorテーブルに英語の情報を登録
                    sector_id = insert_sector(cursor, sector_text)
                else:
                    # sectorテーブルからsector_idを取得
                    sector_id = get_sector_id(cursor, sector_text)

        logger.info(f"セクターデータの登録が完了しました。")
        return sector_id

    except Exception as e:
        logger.exception("セクターデータの登録中にエラーが発生しました。")
        return False

def insert_sector(cursor, sector_text):

    # en_wordsテーブルに登録されている英語の一覧を取得
    en_words_set = set(get_en_words(cursor))

    # en_wordsテーブルに登録されていない場合登録を行う。
    if sector_text not in en_words_set:
        en_id = insert_en_word(cursor, sector_text)
    else:
        en_id = get_en_id(cursor, sector_text)

    # sectorテーブルにセクターを登録する関数
    insert_statement = "INSERT INTO sector (en_id, INS_DATE) VALUES (%s, %s) ON DUPLICATE KEY UPDATE UPD_DATE = %s"
    current_date = datetime.now()
    cursor.execute(insert_statement, (en_id, current_date, current_date))
    sector_id = cursor.lastrowid
    return sector_id

def get_sector_id(cursor, sector_text):
    # sectorテーブルから指定されたセクターのsector_idを取得する関数
    select_statement = "SELECT sector_id FROM sector JOIN en_words ON sector.en_id = en_words.en_id WHERE english = %s"
    cursor.execute(select_statement, (sector_text,))
    result = cursor.fetchone()
    if result:
        sector_id = result[0]
        return sector_id
    else:
        return None

def get_industry(cursor):
    # sectorテーブルから指定されたセクター名の一覧を取得する関数
    select_statement = "SELECT en_words.english FROM industry JOIN en_words ON industry.en_id = en_words.en_id"
    cursor.execute(select_statement)
    return [row[0] for row in cursor.fetchall()]

def insert_industry_data(cursor, csv_file, logger):
    try:
        logger.info(f"関数 insert_industry_data の実行開始")

        # industryテーブルに登録されている英語の一覧を取得
        industry_set = set(get_industry(cursor))

        # csvファイルの読み込み
        with open(csv_file, 'r', encoding='utf-8') as file:
            csv_data = csv.reader(file)

            # 英語の情報を登録および取得し、業種を登録
            for row in csv_data:
                industry_text = row[0]
                if industry_text not in industry_set:
                    # industryテーブルに英語の情報を登録
                    industry_id = insert_industry(cursor, industry_text)
                else:
                    # industryテーブルからindustry_idを取得
                    industry_id = get_industry_id(cursor, industry_text)

        logger.info(f"業種データの登録が完了しました。")
        return industry_id

    except Exception as e:
        logger.exception("業種データの登録中にエラーが発生しました。")
        return False

def insert_industry(cursor, industry_text):

    # en_wordsテーブルに登録されている英語の一覧を取得
    en_words_set = set(get_en_words(cursor))

    # en_wordsテーブルに登録されていない場合登録を行う。
    if industry_text not in en_words_set:
        en_id = insert_en_word(cursor, industry_text)
    else:
        en_id = get_en_id(cursor, industry_text)

    # industryテーブルに業種を登録する関数
    insert_statement = "INSERT INTO industry (en_id, INS_DATE) VALUES (%s, %s) ON DUPLICATE KEY UPDATE UPD_DATE = %s"
    current_date = datetime.now()
    cursor.execute(insert_statement, (en_id, current_date, current_date))
    industry_id = cursor.lastrowid
    return industry_id

def get_industry_id(cursor, industry_text):
    # industryテーブルから指定された業種のindustry_idを取得する関数
    select_statement = "SELECT industry_id FROM industry JOIN en_words ON industry.en_id = en_words.en_id WHERE english = %s"
    cursor.execute(select_statement, (industry_text,))
    result = cursor.fetchone()
    if result:
        industry_id = result[0]
        return industry_id
    else:
        return None

def get_sec_ind(cursor):
    # sec_indテーブルからセクター業種の情報を取得する関数
    select_statement = """
    SELECT
        sector_id,
        industry_id
    FROM sec_ind
    """
    cursor.execute(select_statement)
    return cursor.fetchall()

def insert_sec_ind_data(cursor, csv_file, logger):
    try:
        logger.info(f"関数 insert_sec_ind_data の実行開始")

        # csvファイルの読み込み
        with open(csv_file, 'r', encoding='utf-8') as file:
            csv_data = csv.reader(file, delimiter='\t')

            # セクターと業種の情報を登録および取得し、セクター業種を登録
            for row in csv_data:
                sector_text = row[0]
                industry_text = row[1]

                logger.debug(f"セクター: {sector_text}, 業種: {industry_text}")
                sec_ind_id = insert_sec_ind(cursor, sector_text, industry_text,logger)

        logger.info(f"セクター業種データの登録が完了しました。")
        return True

    except Exception as e:
        logger.exception("セクター業種データの登録中にエラーが発生しました。")
        return False

def get_sec_ind_id(cursor, sector_id, industry_id):
    # sec_indテーブルから指定されたセクターIDと業種IDに対応するsec_ind_idを取得する関数
    select_statement = "SELECT sec_ind_id FROM sec_ind WHERE sector_id = %s AND industry_id = %s"
    cursor.execute(select_statement, (sector_id, industry_id))
    result = cursor.fetchone()
    if result:
        sec_ind_id = result[0]
        return sec_ind_id
    else:
        return None

def insert_sec_ind(cursor, sector_text, industry_text,logger):

    # sector,industryテーブルに登録されている一覧を取得 
    sector_set = set(get_sector(cursor))
    industry_set = set(get_industry(cursor))
    sec_ind_set = set(get_sec_ind(cursor))

    # セクターの情報を登録および取得
    if sector_text not in sector_set:

        # en_wordsテーブルにセクターの情報を登録
        logger.debug(f"セクターを登録します: {sector_text}")
        sector_id =  insert_sector(cursor,sector_text)
        logger.debug(f"セクターの登録が完了しました。sector_id: {sector_id}")

    else:
        # en_wordsテーブルからsector_idを取得
        logger.debug(f"セクターの情報が既に存在します: {sector_text}")
        sector_id = get_sector_id(cursor, sector_text)

    # 業種の情報を登録および取得
    if industry_text not in industry_set:
        # en_wordsテーブルに業種の情報を登録
        logger.debug(f"業種を登録します: {industry_text}")
        industry_id =  insert_industry(cursor,industry_text)
        logger.debug(f"業種の登録が完了しました。industry_id: {industry_id}")

    else:
        # en_wordsテーブルからindustry_idを取得
        logger.debug(f"業種の情報が既に存在します: {industry_text}")
        industry_id = get_industry_id(cursor, industry_text)

    # sector_industryテーブルにセクター：業種を登録
    logger.debug(f"セクター：業種を登録します: セクターID={sector_id}:{sector_text}:業種ID={industry_id}:{industry_text}")
    if (sector_id, industry_id) not in sec_ind_set:

        # sector_industryテーブルにセクター業種を登録する関数
        insert_statement = "INSERT INTO sec_ind (sector_id, industry_id, INS_DATE) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE UPD_DATE = %s"
        current_date = datetime.now()
        cursor.execute(insert_statement, (sector_id, industry_id, current_date, current_date))
        sec_ind_id = get_sec_ind_id(cursor, sector_id, industry_id)
        logger.debug("セクター：業種の登録が完了しました。")
    else:
        logger.debug(f"セクター:業種の情報が既に存在します:セクターID={sector_id}:{sector_text}:業種ID={industry_id}:{industry_text}")
        sec_ind_id = get_sec_ind_id(cursor, sector_id, industry_id)

    return sec_ind_id

def insert_sec_ind_symbol_data(cursor, csv_file, logger):
    try:
        logger.info(f"関数 insert_sec_ind_symbol_data の実行開始")

        # csvファイルの読み込み
        with open(csv_file, 'r', encoding='utf-8') as file:
            csv_data = csv.reader(file, delimiter='\t')

            # セクターと業種とシンボルの情報を登録および取得し、セクター業種シンボルを登録
            for row in csv_data:
                sector_text = row[0]
                industry_text = row[1]
                symbol_text = row[2]

                logger.debug(f"セクター: {sector_text}, 業種: {industry_text}, symbol: {symbol_text}")
                insert_sec_ind_symbol(cursor, sector_text, industry_text, symbol_text,logger)

        logger.info(f"セクター:業種:シンボルデータの登録が完了しました。")
        return True

    except Exception as e:
        logger.exception("セクター:業種:シンボルデータの登録中にエラーが発生しました。")
        return False

def insert_sec_ind_symbol(cursor, sector_text, industry_text, symbol, logger):

    sec_ind_id = insert_sec_ind(cursor, sector_text, industry_text, logger)

    # sec_ind_symbolテーブルにセクター業種とシンボルを登録
    insert_statement = "INSERT INTO sec_ind_symbol (sec_ind_id, symbol, INS_DATE) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE UPD_DATE = %s"
    current_date = datetime.now()
    cursor.execute(insert_statement, (sec_ind_id, symbol, current_date, current_date))

    logger.debug("セクター業種とシンボルの登録が完了しました。")

def SectorIndustrySymbol_insert(cursor, sql_info, csv_file, logger):
    try:
        logger.info("関数 SectorIndustrySymbol_insert の実行開始")

        # SQLファイルのデータ取得
        table_name = sql_info.table_name
        members = sql_info.members
        additional_statement = sql_info.additional_statement

        logger.debug(f"table_name: {table_name}")
        logger.debug(f"members: {members}")
        logger.debug(f"additional_statement: {additional_statement}")

        # ループの外でエラーフラグを初期化
        has_error = False

        if table_name == "sector":
            # "sector"テーブルの処理
            if not insert_sector_data(cursor, csv_file, logger):
                logger.error(f"{table_name}テーブルへのデータの挿入に失敗しました。")
                has_error = True

        elif table_name == "industry":
            # "industry"テーブルの処理
            if not insert_industry_data(cursor, csv_file, logger):
                logger.error(f"{table_name}テーブルへのデータの挿入に失敗しました。")
                has_error = True

        elif table_name == "sec_ind":
            # "sec_ind"テーブルの処理
            if not insert_sec_ind_data(cursor, csv_file, logger):
                logger.error(f"{table_name}テーブルへのデータの挿入に失敗しました。")
                has_error = True

        elif table_name == "sec_ind_symbol":
            # "sec_ind"テーブルの処理
            if not insert_sec_ind_symbol_data(cursor, csv_file, logger):
                logger.error(f"{table_name}テーブルへのデータの挿入に失敗しました。")
                has_error = True

        elif table_name == "en_jp_translation":
            # "en_jp_translation"テーブルの処理
            if not insert_en_jp_translation_data(cursor, csv_file, logger):
                logger.error(f"{table_name}テーブルへのデータの挿入に失敗しました。")
                has_error = True

        else:
            # その他のテーブルの処理
            with open(csv_file, 'r', encoding='utf-8') as utf_file:
                csv_data = csv.reader(utf_file, delimiter='\t')

                # insert_data関数を呼び出し、戻り値をチェックする
                if not insert_data(cursor, table_name, members, additional_statement, csv_data, logger):
                    logger.error(f"{table_name}テーブルへのデータの挿入に失敗しました。")
                    has_error = True

        # エラーフラグの処理
        if has_error:
            logger.info("関数 insert_data の異状終了")
            return False
        else:
            logger.info("関数 insert_data の正常終了")
            return True

    except FileNotFoundError:
        logger.error(f"ファイルが存在しません: {csv_file}")
        return False

    except ValueError:
        logger.error(f"SQLファイルの形式が無効です: {csv_file}")
        return False

