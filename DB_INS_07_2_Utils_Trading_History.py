import os
import csv
import codecs
import shutil
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

def Trading_History_insert_tmp_data(cursor, file_path, Trading_History_csv_file_path ,tmp_table_name ,logger):
    try:
        logger.info(f"関数 Trading_History_insert_tmp_data の実行開始。入力引数: cursor={cursor}, file_path={file_path}, Trading_History_csv_file_path={Trading_History_csv_file_path}, tmp_table_name={tmp_table_name}")
        # ファイルからディレクトリパスとSQLファイル名を読み込む
        with open(file_path, 'r',  encoding='utf-8') as file:
            lines = file.readlines()

        if len(lines) < 2:
            logger.error("ファイルの内容が正しくありません。")
            return

        directory_path = lines[0].strip()
        sql_files = [line.strip() for line in lines[1:]]

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
            with codecs.open(Trading_History_csv_file_path, 'r', 'sjis', 'utf-8') as utf_file:
                csv_reader = csv.reader(utf_file, delimiter=',', quotechar='"')
                next(csv_reader)  # ヘッダー行をスキップする場合はコメントアウト

                for row_index, row in enumerate(csv_reader, start=1):

                    logger.debug(f"row_index:row:{row_index}:{row}")

                    # 毎回初期化を行う。
                    csv_data = {}

                    processed_row = []
                    for value in row:

                        # カンマが含まれていたら除外する。
                        value = value.strip().replace(',', '')

                        # NULLまたは"-"の場合、Noneに変換する
                        value = None if value in ('', '-') else value
                        processed_row.append(value)

                    # buy_sell_typeは売買以外の株式分割などがあった場合のイベントの際はNoneが設定されるので「売買以外」という文字列の設定を行う。
                    if processed_row[6] == None:
                        processed_row[6] = "売買以外"

                    logger.debug(f"processed_row: {processed_row}")

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
                    # 例 processed_row: [0]:'2021/8/19',[1]:'2021/8/24', [2]:'TLT', [3]:'ISHARES 20YR TR', [4]:'特定',[5]:'買付',
                    #                   [6]:'円',[7]:'2',[8]:'150.2900', [9]:'300.58', [10]:'110.06', [11]:'1.35',[12]:'0.13',[13]:None,[14]:'33577.00']

                    csv_data['trade_date'] = processed_row[0]         # [0]:'2021/8/19'
                    csv_data['settlement_date'] = processed_row[1]    # [1]:'2021/8/24'
                    csv_data['ticker'] = processed_row[2]             # [2]:'TLT'
                    csv_data['buy_sell_type'] = processed_row[6]      # [6]:'円'
                    csv_data['security_name'] = processed_row[3]      # [3]:'ISHARES 20YR TR'
                    csv_data['account'] = processed_row[4]            # [4]:'特定'
                    csv_data['transaction_type'] = processed_row[5]   # [5]:'買付'
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
                        logger.info("データを挿入しました。:[%d]", row_index)
                    except mysql.connector.Error as error:
                        logger.error("INSERT文の実行中にエラーが発生しました:", error)
                        logger.error("対象行番号:[%d]", row_index)  # 対象行番号をログに出力
                        logger.error("処理中のファイル名:[%s]", Trading_History_csv_file_path)  # 処理中のファイル名をログに出力
                        raise  # エラーを再度発生させて処理を終了

        logger.info(f"処理が完了しました。SQLファイル: {sql_file}")
        return table_name, members,Trading_History_csv_file_path

    except Exception as e:
        logger.error("処理中にエラーが発生しました。")
        logger.error(str(e))
        logger.error(traceback.format_exc())  # 修正: トレースバック情報をログに出力
        has_error = True  # エラーフラグを設定
        raise  # エラーを再度発生させて処理を終了

    finally:
        logger.info("関数 Trading_History_insert_tmp_data の実行終了。")
        logger.info("処理中のファイル名:[%s]", Trading_History_csv_file_path)  # 処理中のファイル名をログに出力

    # エラーフラグをチェックして処理を終了
    if has_error:
        logger.error("エラーが発生したため、処理を終了します。")
        sys.exit(1)  # 修正: エラーが発生した場合にプログラムを終了する

def Trading_History_insert_data( cursor,insert_sql,logger ):
    try:

        # エラーフラグを初期化
        has_error = False

        # 各SQLファイルとシンボルを組み合わせて実行
        try:
            # INSERT文を実行
            cursor.execute(insert_sql)

            logger.info("データを挿入しました。")
            
        except mysql.connector.Error as error:
            logger.error("INSERT文の実行中にエラーが発生しました:", error)
            logger.error("対象行番号:[%d]", row_index)  # 対象行番号をログに出力
            raise  # エラーを再度発生させて処理を終了

        # カーソルと接続を閉じる
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

def check_csv_header(csv_file_path, expected_headers, logger):
    logger.info(f"CSVファイルのヘッダーをチェックします。ファイルパス: {csv_file_path}")
    with open(csv_file_path, 'r') as file:
        csv_reader = csv.reader(file)
        headers = next(csv_reader)
        if len(headers) != len(expected_headers) or set(headers) != set(expected_headers):
            logger.error(f"CSVファイルのヘッダーが不正です。")
            logger.info(f"期待するヘッダー: {expected_headers}")
            logger.info(f"実際のヘッダー: {headers}")
            return False  # ヘッダー不一致のステータスを返す
    logger.info("CSVファイルのヘッダーが正常です。")
    return True  # OKのステータスを返す
