import traceback
import mysql.connector
import os
from configparser import ConfigParser
import requests

# 指定したURLからクッキー情報を取得し、ヘッダー形式に変換して返す関数
# Parameters:
#     url (str): クッキー情報を取得するURL
# Returns:
#     dict: クッキー情報を格納したヘッダー
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

# MySQLに接続するためのコネクションを取得する関数
# Returns:
#     MySQLConnection: MySQLへの接続コネクション
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
# Parameters:
#     file_path (str): Configファイルのパス
# Returns:
#     dict: ファイルパスを格納した辞書
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

# SQLクエリを実行する関数
# Parameters:
#     cursor (MySQLCursor): MySQLのカーソルオブジェクト
#     query (str): 実行するSQLクエリ
#     values (tuple): クエリに渡すパラメータ値のタプル
#     logger (Logger): ロガーオブジェクト
# Returns:
#     list: クエリの結果を格納したリスト
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
        raise  # エラーを呼び出し元に伝える

"""
SQLファイルからテーブル名とメンバーを取得する関数

Parameters:
    sql_file_path (str): SQLファイルのパス

Returns:
    tuple: テーブル名とメンバーのリスト、追加のステートメント
"""
def get_table_name_and_members(sql_file_path):
    try:
        with open(sql_file_path, 'r', encoding='utf-8') as file:
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

# SQLファイル情報のクラス
class SQLFileInfo:
    def __init__(self, table_name, members, additional_statement, sql_file_path):
        self.table_name = table_name
        self.members = members
        self.additional_statement = additional_statement
        self.sql_file_path = sql_file_path
        self.data = None  # データを保持するプロパティ

# SQLファイル情報の取得
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

        table_name, members, additional_statement = get_table_name_and_members(sql_file_path)

        logger.debug(f"table_name={table_name}")
        logger.debug(f"members={members}")
        logger.debug(f"additional_statement={additional_statement}")

        # 各SQLファイルとシンボルを組み合わせて情報を取得し、リストに追加
        sql_file_info.append(SQLFileInfo(table_name, members, additional_statement, sql_file_path))

    return sql_file_info
