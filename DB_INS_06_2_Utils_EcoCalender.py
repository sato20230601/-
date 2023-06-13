import os
import csv
from datetime import datetime, date, timedelta

import logging
import traceback
import requests
from bs4 import BeautifulSoup

import mysql.connector
from configparser import ConfigParser

import sys

import xml.etree.ElementTree as ET

# 登録更新処理共通
import DB_INS_00_Utils

# 共通関数の読み込み
import DB_Common_Utils

def scrape_data(url, logger):
    # リクエストを送信
    response = requests.get(url)

    # レスポンスのステータスコードを表示
    logger.info('Status Code:')
    logger.info(response.status_code)

    xml_data = response.content

    # XMLデータを解析
    root = ET.fromstring(xml_data)

    calendar_data = []

    # XMLからデータ取得
    for row in root.iter('ROW'):
        data = [
            row.find('NO').text,
            row.find('DATE').text,
            row.find('NAME').text,
            row.find('CUR').text,
            row.find('WEEKDAY').text,
            row.find('TIME').text,
            row.find('COMP').text,
            row.find('MON').text,
            row.find('LAST').text,
            row.find('CHANGE').text,
            row.find('ESTIMATE').text,
            row.find('RESULT').text,
            row.find('IMPORTANCE').text,
            row.find('DISPLAY').text
        ]
        calendar_data.append(data)

    # データを表示
    if len(calendar_data) == 0:
        logger.info('No data found.')

    return calendar_data

def EcoCalender_process_data(file_path, config_key, logger):
    try:
        # ファイルからディレクトリパスとSQLファイル名を読み込む
        with open(file_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()

        if len(lines) < 2:
            logger.error("ファイルの内容が正しくありません。")
            return

        directory_path = lines[0].strip()
        sql_files = [line.strip() for line in lines[1:]]

        # 「EcoCalender_url」と「EcoCalender_csv_file_path」のパスをconfigファイルより取得
        config_path = r"C:\Users\sabe2\OneDrive\デスクトップ\Python\06_DATABASE\06-03_SRC\config.txt"
        config = DB_Common_Utils.read_config_file(config_path)
        EcoCalender_url = config.get('EcoCalender_url')
        EcoCalender_csv_file_path = config.get('EcoCalender_csv_file_path')

        # 現在の年月を取得
        current_date = datetime.now()
        year_month = current_date.strftime("%Y%m")

        # URLを置き換える
        EcoCalender_url = EcoCalender_url.replace("YYYYMM", year_month)

        # 経済カレンダー情報を取得
        all_data = scrape_data(EcoCalender_url, logger)

        # CSVファイルにデータを書き込む
        with open(EcoCalender_csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile, delimiter='\t')

            # ヘッダー行を書き込む
            writer.writerow(['NO','DATE','EVENT_NAME','CUR','WEEKDAY','TIME','COMP','MON','LAST','CHANGE','ESTIMATE','RESULT','IMPORTANCE','DISPLAY'])  # ヘッダー行の書き込み

            # データ行を書き込む
            writer.writerows(all_data)  # データ行の書き込み

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
            with open(EcoCalender_csv_file_path, 'r', newline='', encoding='utf-8') as csvfile:
                csv_reader = csv.reader(csvfile, delimiter='\t')
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

                        if member == 'INS_DATE':
                            insdate = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            insert_values.append(insdate)
                        elif member == 'UPD_DATE':
                            insert_values.append(None)
                        else:
                            # NULLの場合、Noneに変換する
                            insert_values.append(value if value != '' else None)
                            logger.debug(f"insert_values:{insert_values}")

                    # INSERT文を実行
                    insert_query = f"""
                    INSERT INTO {table_name}
                        ({', '.join(members)})
                    VALUES
                        ({', '.join(['%s'] * len(members))})
                    """
                    insert_query += additional_statement
                    insert_values.append(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))  # INS_DATEの値を追加
                    insert_values.append(None)  # UPT_DATEの値を追加

                    # SQL文を表示またはログファイルに書き込み
                    logger.debug("実行するSQL文:")
                    logger.debug(insert_query)
                    logger.debug(insert_values)
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


