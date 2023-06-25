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

def get_headers_from_cookies(url):

    # リクエストを送信し、レスポンスを取得
    response = requests.get(url)

    # レスポンスヘッダーからクッキー情報を取得
    cookies = response.cookies

    # クッキー情報をヘッダー形式に変換
    headers = {
        'Cookie': '; '.join([f"{cookie.name}={cookie.value}" for cookie in cookies])
    }
    
    return headers

def scrape_data(headers,page):
    base_url = 'https://nikkeiyosoku.com/russell2000/json'
    next_page_url = base_url + "?start=20"

    payload = {
        'type': 'holdings',
        'page': str(page)
    }

    # リクエストを送信
    response = requests.post(next_page_url, headers=headers, data=payload)

    # レスポンスを取得
    data = response.json()

    html_content = data['table']

    soup = BeautifulSoup(html_content, 'html.parser')
    table_rows = soup.find_all('tr')

    # データをリストとして準備
    data_list = []

    for row in table_rows:
        hedders = row.find_all('th')
        if hedders:
            hedder = hedders[0].text.strip()

        cells = row.find_all('td')
        if cells:
            stock_code = cells[0].text.strip()
            stock_name = cells[1].text.strip()
            sector = cells[2].text.strip()
            industry = cells[3].text.strip()

            # リストにデータを追加
            data_list.append([hedder, stock_code, stock_name, sector, industry])

    return data_list

def rus2000_process_data(file_path, config_key, logger):
    try:

        # ファイルからディレクトリパスとSQLファイル名を読み込む
        with open(file_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()

        if len(lines) < 2:
            logger.error("ファイルの内容が正しくありません。")
            return

        directory_path = lines[0].strip()
        sql_files = [line.strip() for line in lines[1:]]

        # 「rus2000_url」と「rus2000_csv_file_path」のパスをconfigファイルより取得
        config_path = r"C:\Users\sabe2\OneDrive\デスクトップ\Python\06_DATABASE\06-03_SRC\config.txt"
        config = DB_Common_Utils.read_config_file(config_path)
        rus2000_url = config.get('rus2000_url')
        rus2000_csv_file_path = config.get('rus2000_csv_file_path')

        headers = DB_Common_Utils.get_headers_from_cookies(rus2000_url)

        # ページ1から100までのデータを取得
        all_data = []
        for page in range(1, 100):
            page_data = scrape_data(headers,page)
            all_data.extend(page_data)
	
        # CSVファイルにデータを書き込む
        with open( rus2000_csv_file_path , 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile, delimiter='\t')

            # ヘッダー行を書き込む
            writer.writerow(['No','Symbol', 'StockName', 'Sector', 'Industry'])  # ヘッダー行の書き込み

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

            rus2000_data = DB_INS_00_Utils.get_recent_data(cursor,table_name,logger)
            csv_data = DB_INS_00_Utils.read_csv_data(rus2000_csv_file_path,logger)

            csv_no = 1
            rus2000_no = 1
            if DB_INS_00_Utils.check_diff(cursor,"rus2000_diff_record",csv_data,csv_no, rus2000_data, rus2000_no, logger):

                # TRUEの場合差分があるので登録を行う。
                # CSVファイルのデータをテーブルに登録
                with open(rus2000_csv_file_path, 'r', newline='', encoding='utf-8') as csvfile:
                    csv_reader = csv.reader(csvfile, delimiter='\t')
                    next(csv_reader)  # ヘッダー行をスキップする場合はコメントアウト

                    for row_index, row in enumerate(csv_reader, start=1):

                        # 空白を除外してデータを整形
                        row = row[1:]  # Noを除外するためにスライスを使用
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
                        logger.debug(insert_query)
                        logger.debug(insert_values)
                        logger.debug(insert_query % tuple(insert_values))  # 値をセットしたSQL文を表示
                        try:
                            # INSERT文を実行
                            cursor.execute(insert_query, insert_values)
                            cnx.commit()
                            logger.info("データを挿入しました。:[%d]", row_index)
                        except mysql.connector.Error as error:
                            logger.error("INSERT文の実行中にエラーが発生しました:", error)
                            logger.error("対象行番号:[%d]", row_index)  # 対象行番号をログに出力
                            raise  # エラーを再度発生させて処理を終了

            else:
                logger.info("データに差分はありませんでした。スキップします。")

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

# CSVのデータとDBの直近のデータの比較を行い、差分があれば差分チェックテーブルに登録を行いTRUEを返す。
# なければFALSEを返す。
def check_diff_rus2000(cursor, table_name, csv_data, recent_data, logger=None):
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
    csv_symbols = [row[1] for row in csv_data]
    recent_symbols = [row[1] for row in recent_data]
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
