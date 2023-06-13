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

def sp500_process_data(file_path, config_key, logger):
    try:

        # ファイルからディレクトリパスとSQLファイル名を読み込む
        with open(file_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()

        if len(lines) < 2:
            logger.error("ファイルの内容が正しくありません。")
            return

        directory_path = lines[0].strip()
        sql_files = [line.strip() for line in lines[1:]]

        # 「sp500_url」と「500_csv_file_path」のパスをconfigファイルより取得
        config = DB_Common_Utils.read_config_file('config.txt')
        sp500_url = config.get('sp500_url')
        sp500_csv_file_path = config.get('sp500_csv_file_path')

        response = requests.get(sp500_url)
        soup = BeautifulSoup(response.text, 'html.parser')
	
        # テーブルの要素を取得
        table = soup.find('table', {'class': 'wikitable sortable'})

        # テーブルの行を取得
        rows = table.find_all('tr')

        # CSVファイルにデータを書き込む
        with open( sp500_csv_file_path , 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile, delimiter='\t')

            # ヘッダー行を書き込む
            header_row = rows[0].find_all('th')
            header_data = [cell.text.strip() for cell in header_row]
            writer.writerow(header_data)

            # データ行を書き込む
            for row in rows[1:]:
                cells = row.find_all('td')
                data = [cell.text.strip() for cell in cells]
                writer.writerow(data)

        # MySQLに接続
        cnx = DB_Common_Utils.get_mysql_connection()

        # カーソルを取得
        cursor = cnx.cursor()

        # ループの外でエラーフラグを初期化
        has_error = False

        # 各SQLファイルとシンボルを組み合わせて実行
        for sql_file in sql_files:

            sql_file_path = f"{directory_path}/{sql_file}"
            table_name, members, additional_statement = DB_INS_00_Utils.get_table_name_and_members(sql_file_path)
            logger.debug(table_name)
            logger.debug(members)
            logger.debug(additional_statement)

            # CSVファイルのデータをテーブルに登録
            with open(sp500_csv_file_path, 'r', newline='', encoding='utf-8') as csvfile:
                csv_reader = csv.reader(csvfile, delimiter='\t')
                next(csv_reader)  # ヘッダー行をスキップする場合はコメントアウト

                for row_index, row in enumerate(csv_reader, start=1):

                    # 空白を除外してデータを整形
                    row = [value.strip() for value in row]
                    logger.debug(f"row: {row}")

                    # 「Date_YYYYMMDD」のデータを追加
                    date_yyyymmdd = datetime.now().strftime("%Y-%m-%d")
                    row.insert(0, date_yyyymmdd)

                    # INSERT文を実行
                    insert_values = []
                    for member, value in zip(members, row):
                        logger.debug(f"member:{member}")
                        logger.debug(f"value:{value}")
                        
                        if member == 'Date_YYYYMMDD':
                            insert_values.append(f'{value}')
                        elif member == 'INS_DATE':
                            insdate = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            insert_values.append(f'{insdate}')
                        elif member == 'UPD_DATE':
                            insert_values.append(None)
                        elif member == "Date_added":

                            logger.debug(f"チェック1:[{member}]:[{value}]")
                            size = len(value)
                            logger.debug(f"サイズ:{size}")
                            if size > 0:  # データが存在する場合
                                logger.debug(f"Date added:データが存在する場合")
                                logger.debug(f"チェック2:[{member}]:[{value}]")

                                insert_values.append(f'{value}')
                                # logger.info({insert_values})

                            else:  # データが存在しない場合
                                logger.debug(f"Date added:データが存在しない場合")
                                logger.debug(f"チェック3:[{member}]:[{value}]")
                                insert_values.append(None)
                                # logger.info({insert_values})
                        else:
                            insert_values.append(f'{value}')
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
        return
