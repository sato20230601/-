import mysql.connector
import os
from configparser import ConfigParser

import requests

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

# MySQLへの接続を行う。
def get_mysql_connection():

    # MySQL接続情報の取得方法
    username = os.getenv('DB_USERNAME')
    password = os.getenv('DB_PASSWORD')
    host = os.getenv('DB_HOST')
    database = os.getenv('DB_DATABASE')

    # MySQLに接続
    cnx = mysql.connector.connect(user=username, password=password, host=host, database=database)
    return cnx

# Configファイルから定義ファイルに登録されているファイルパスを辞書形式で取得する関数
def read_config_file(file_path):
    config = {}
    with open(file_path, 'r', encoding='utf-8') as file:  # ファイルのエンコーディングを指定
        lines = file.readlines()
        for line in lines:
            line = line.strip()
            if line and '=' in line:  # 行に等号が含まれているか確認
                key, value = line.split('=')
                config[key.strip()] = value.strip()
    return config

# SQLクエリを実行
def execute_sql_query(cursor, query, values, logger):
    try:
        # SQLクエリを実行
        logger.debug(query)
        logger.debug(values)
        cursor.execute(query, values)
        result = cursor.fetchall()
        return result
    except mysql.connector.Error as err:
        logger.error("SQLクエリの実行に問題がありました。")
        logger.error("エラーコード: %s", err.errno)
        logger.error("エラーメッセージ: %s", err.msg)
        logger.error("トレースバック情報: %s", traceback.format_exc())  # 修正: トレースバック情報をログに出力
        return None

# テーブルの削除を実行する。
def drop_table(cursor, table_name, logger):
    try:
        # テーブルの存在を確認
        if check_table_existence(cursor, table_name):
            # テーブル削除のSQL文を生成
            query = f"DROP TABLE {table_name}"

            # SQLクエリを実行
            logger.debug(query)
            cursor.execute(query)

            logger.info("テーブルの削除が完了しました。")
        else:
            logger.info("削除対象のテーブルは存在しません。")

    except mysql.connector.Error as err:
        logger.error("テーブルの削除に問題がありました。")
        logger.error("エラーコード: %s", err.errno)
        logger.error("エラーメッセージ: %s", err.msg)
        logger.error("トレースバック情報: %s", traceback.format_exc())  # 修正: トレースバック情報をログに出力


