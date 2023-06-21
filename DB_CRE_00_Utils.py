import os
import mysql.connector
from configparser import ConfigParser
import logging
from datetime import datetime
import DB_Common_Utils

def read_create_statements(sql_file_path, logger):
    try:
        logger.info("ファイルからクリエイト文を読み込みを行います。")

        with open(sql_file_path, 'r', encoding='utf-8') as file:
            content = file.read()
            # 改行文字を適切な区切り文字に置換する（例えばセミコロン）
            content = content.replace('\r\n', ';')
            create_statements = content.split(';')

            logger.debug(f"create_statements：{create_statements}")
            table_name = "Trading_History"
            table_name_aft = "Trading_History_20230610"
            new_statements = create_statements[0].replace( table_name , table_name_aft)
            logger.debug(f"new_statements：{new_statements}")

            logger.info("ファイルからクリエイト文を読み込みを終了します。")
        return create_statements
    except FileNotFoundError:
        logger.error(f"ファイルが存在しません: {sql_file_path}")
        return ""

def get_create_table_name_and_columns(create_query, logger):
    try:
        logger.info("SQLファイルからテーブル名、カラムの取得を行います。")
        logger.debug(f"create_query：{create_query}")

        create_query = create_query[0]  # リストから文字列を取り出す

        # テーブル名の取得
        table_name_start = create_query.index('CREATE TABLE') + len('CREATE TABLE')
        logger.debug(f"table_name_start：{table_name_start}")

        table_name_end = create_query.index('(', table_name_start)
        logger.debug(f"table_name_end：{table_name_end}")

        table_name = create_query[table_name_start:table_name_end].strip()
        logger.debug(f"table_name：{table_name}")

        # カラムの取得
        columns_start = create_query.index('(', table_name_end) + 1
        columns_end = create_query.index(')', columns_start)
        columns = [c.strip() for c in create_query[columns_start:columns_end].split(',')]

        logger.debug(f"columns：{columns}")

        logger.info("SQLファイルからテーブル名、カラムの取得を行いました。")

        return table_name, columns

    except ValueError:
        logger.info("SQLファイルからテーブル名、カラムの取得に失敗しました")

        # クエリの解析に失敗した場合のエラーハンドリング
        return None, None

# テーブルを作成する関数 
def create_table(create_statements, connection, logger ):
    try:
        logger.info("テーブルの生成を行います。")
        logger.debug(f"create_statements：{create_statements}")

        # カーソルを取得
        logger.info("カーソルの取得を行います。")
        cursor = connection.cursor()

        # CREATE TABLE ステートメントの取得と実行
        logger.info("CREATE TABLE ステートメントの取得と実行を行います。")
        create_table_start = create_statements.index('CREATE TABLE')
        create_table_end = len(create_statements)
        if 'COMMENT' in create_statements:
            create_table_end = create_statements.index('COMMENT')
        create_table_statement = create_statements[create_table_start:create_table_end+1]

        # テーブル作成処理の実行
        logger.info("テーブル作成処理の実行を行います。")
        logger.debug(create_table_statement)
        cursor.execute(create_table_statement)

        # CREATE TRIGGER ステートメントの取得と実行
        if 'CREATE TRIGGER' in create_statements:
            create_trigger_start = create_statements.index('CREATE TRIGGER')
            create_trigger_end = create_statements.index('END', create_trigger_start) + 3
            create_trigger_statement = create_statements[create_trigger_start:create_trigger_end]

            # トリガー作成処理の実行
            logger.debug(create_trigger_statement)
            cursor.execute(create_trigger_statement)

        # カーソルを閉じる
        logger.info("カーソルを閉じます。")
        cursor.close()

        logger.info("テーブルが正しく生成されました。")

        # テーブルの作成が成功した場合はTrueを返す
        return True

    except mysql.connector.Error as err:
        logger.error("テーブルの生成に問題がありました。")
        logger.error("エラーコード: %s", err.errno)
        logger.error("エラーメッセージ: %s", err.msg)
        return False


# テーブルの存在をチェックする関数
def check_table_existence(table_name, connection, logger):
    try:
        logger.info("テーブルの存在をチェックを開始します。")
        logger.debug(f"table_name：{table_name}")

        # MySQLに接続
        cnx = DB_Common_Utils.get_mysql_connection()

        # カーソルを取得
        cursor = cnx.cursor()

        # テーブルの存在をチェックするクエリ
        check_table_query = f"SHOW TABLES LIKE '{table_name}'"

        logger.debug(f"check_table_query：{check_table_query}")

        # テーブルの存在をチェック
        cursor.execute(check_table_query)

        exists = bool(cursor.fetchone())

        # カーソルと接続を閉じる
        cursor.close()
        cnx.close()

        logger.debug(f"exists：{exists}")
        logger.info("テーブルの存在をチェックを終了します。")
        return exists

    except mysql.connector.Error as err:
        logger.error("テーブルの存在チェックに問題がありました。")
        logger.error("エラーコード: %s", err.errno)
        logger.error("エラーメッセージ: %s", err.msg)
        return False

def process_sql_files(file_path, logger, flg=0):
    try:
        logger.debug("process_sql_filesを開始します")

        # ファイルからディレクトリパスとSQLファイル名を読み込む
        with open(file_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()

        if len(lines) < 2:
            logger.error("ファイルの内容が正しくありません。")
            return

        directory_path = lines[0].strip()
        logger.debug(f"directory_path：{directory_path}")

        sql_files = [line.strip() for line in lines[1:]]

        # MySQL接続情報を取得
        connection = DB_Common_Utils.get_mysql_connection()

        # 主処理
        # 定義ファイルに記載されているSQLファイル数だけCREATE処理実行
        table_name = []
        for sql_file in sql_files:
            sql_file_path = os.path.join(directory_path, sql_file)

            # ファイルが存在しない場合はスキップする
            if not os.path.exists(sql_file_path):
                logger.error(f"SQLファイルが存在しません: {sql_file}")
                continue

            # ファイルからクリエイト文を読み込む
            logger.debug(f"sql_file_path：{sql_file_path}")
            create_statements = read_create_statements(sql_file_path, logger)

            # create_statements の中身を改行を含めて表示
            create_statements_str = "\n".join(create_statements)
            logger.debug(create_statements_str)

            logger.debug(f"create_statements[0]：{create_statements[0]}")

            # CREATE文からテーブル名とカラムを取得
            table_name, columns = get_create_table_name_and_columns(create_statements, logger)
            if not table_name:
                logger.error(f"{sql_file} の解析に失敗しました。次のファイルに進みます。")
                continue

            logger.debug(f"table_name：{table_name}")

            # フラグ「1」の場合、一時テーブルの作成を行う。
            new_statements = None  # 初期値を設定
            if flg == 1:
                logger.debug("フラグ「1」の為、一時テーブルの作成を行う。")

                current_date = datetime.now().strftime("%Y%m%d")
                tmp_table_name = f"{table_name}_{current_date}"
                logger.debug(f"tmp_table_name：{tmp_table_name}")

                new_statements = create_statements[0].replace(table_name, tmp_table_name)
                table_name = tmp_table_name

                logger.debug(f"create_statements：{create_statements}")
                logger.debug(f"new_statements：{new_statements}")

            # テーブルが既に存在する場合はスキップする
            logger.debug("テーブル有無チェック")
            if check_table_existence(table_name, connection, logger):
                logger.info(f"テーブル '{table_name}' は既に存在します。スキップします。")
                continue

            # テーブルを作成する
            logger.debug("テーブル作成")
            if create_table(new_statements or create_statements[0], connection, logger):  # new_statementsがNoneの場合はcreate_statements[0]を使用
                logger.info(f"テーブル '{table_name}' を作成しました。")
            else:
                logger.error(f"テーブル '{table_name}' の作成に失敗しました。")

        logger.info("処理が完了しました。")
        return table_name

    except Exception as e:
        logger.error("処理中にエラーが発生しました。")
        logger.error(str(e))

