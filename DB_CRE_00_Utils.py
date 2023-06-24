import os
import mysql.connector
from configparser import ConfigParser
import logging
from datetime import datetime
import DB_Common_Utils

def read_create_statements(sql_file_path, logger):
    try:
        logger.debug("ファイルからクリエイト文を読み込みを行います。")

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

            logger.debug("ファイルからクリエイト文を読み込みを終了します。")
        return create_statements
    except FileNotFoundError:
        logger.error(f"ファイルが存在しません: {sql_file_path}")
        return ""

def get_create_table_name_and_columns(create_query, logger):
    try:
        logger.debug("SQLファイルからテーブル名、カラムの取得を行います。")
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

        logger.debug("SQLファイルからテーブル名、カラムの取得を行いました。")

        return table_name, columns

    except ValueError:
        logger.debug("SQLファイルからテーブル名、カラムの取得に失敗しました")

        # クエリの解析に失敗した場合のエラーハンドリング
        return None, None

# テーブルを作成する関数 
def create_table(cursor,create_statements, logger ):
    try:
        logger.debug("テーブルの生成を行います。")
        logger.debug(f"create_statements：{create_statements}")

        # CREATE TABLE ステートメントの取得と実行
        logger.debug("CREATE TABLE ステートメントの取得と実行を行います。")
        create_table_start = create_statements.index('CREATE TABLE')
        create_table_end = len(create_statements)
        if 'COMMENT' in create_statements:
            create_table_end = create_statements.index('COMMENT')
        create_table_statement = create_statements[create_table_start:create_table_end+1]

        # テーブル作成処理の実行
        logger.debug("テーブル作成処理の実行を行います。")
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

        logger.debug("テーブルが正しく生成されました。")

        # テーブルの作成が成功した場合はTrueを返す
        return True

    except mysql.connector.Error as err:
        logger.error("テーブルの生成に問題がありました。")
        logger.error("エラーコード: %s", err.errno)
        logger.error("エラーメッセージ: %s", err.msg)
        return False


# テーブルの存在をチェックする関数
def check_table_existence(cursor,table_name, logger):
    try:
        logger.debug("テーブルの存在をチェックを開始します。")
        logger.debug(f"table_name：{table_name}")

        # テーブルの存在をチェックするクエリ
        check_table_query = f"SHOW TABLES LIKE '{table_name}'"

        logger.debug(f"check_table_query：{check_table_query}")

        # テーブルの存在をチェック
        cursor.execute(check_table_query)

        exists = bool(cursor.fetchone())

        logger.debug(f"exists：{exists}")
        logger.debug("テーブルの存在をチェックを終了します。")
        return exists

    except mysql.connector.Error as err:
        logger.error("テーブルの存在チェックに問題がありました。")
        logger.error("エラーコード: %s", err.errno)
        logger.error("エラーメッセージ: %s", err.msg)
        return False

# テーブルの削除を実行する。
def drop_table(cursor, table_name, logger):
    try:
        # テーブルの存在を確認
        if check_table_existence(cursor, table_name, logger):
            # テーブル削除のSQL文を生成
            query = f"DROP TABLE {table_name}"

            # SQLクエリを実行
            logger.debug(query)
            cursor.execute(query)

            logger.debug("テーブルの削除が完了しました。")
        else:
            logger.debug("削除対象のテーブルは存在しません。")

    except mysql.connector.Error as err:
        logger.error("テーブルの削除に問題がありました。")
        logger.error("エラーコード: %s", err.errno)
        logger.error("エラーメッセージ: %s", err.msg)
        logger.error("トレースバック情報: %s", traceback.format_exc())  # 修正: トレースバック情報をログに出力

# テーブルのクリアを実行する。
def clear_table(cursor, table_name, logger):
    try:
        query = f"DELETE FROM {table_name};"

        # SQLクエリを実行
        logger.debug(query)
        cursor.execute(query)
        cursor.connection.commit()

        logger.info(f"テーブル '{table_name}' のデータをクリアしました。")

    except mysql.connector.Error as err:
        logger.error("テーブルのクリアに問題がありました。")
        logger.error("エラーコード: %s", err.errno)
        logger.error("エラーメッセージ: %s", err.msg)
        logger.error("トレースバック情報: %s", traceback.format_exc())  # 修正: トレースバック情報をログに出力

def process_sql_files(cursor,file_path, logger, flg=0):
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
            if flg == 1:
                # 一時テーブルの作成を行う
                tmp_table_name = f"TMP_{table_name}"
                logger.debug(f"tmp_table_name：{tmp_table_name}")

                # 一時テーブルの存在チェック
                if check_table_existence(cursor, tmp_table_name, logger):
                    logger.info(f"一時テーブル '{tmp_table_name}' が既に存在します。削除します。")
                    drop_table(cursor, tmp_table_name, logger)

                # 一時テーブルを作成する
                if create_table(cursor, create_statements[0].replace(table_name, tmp_table_name), logger):
                    logger.info(f"一時テーブル '{tmp_table_name}' を作成しました。")
                    table_name = tmp_table_name
                else:
                    logger.error(f"一時テーブル '{tmp_table_name}' の作成に失敗しました。")

            else:
                # テーブルの存在チェック
                if check_table_existence(cursor,table_name, logger):
                    logger.info(f"テーブル '{table_name}' が既に存在します。処理を選択してください。")
                    choice = input("1. テーブルを削除して新規に作成\n2. テーブルのデータをクリア\n3. スキップしてCREATEを実行しない\n選択してください（1, 2, 3）: ")

                    if choice == "1":
                        # テーブルを削除して新規に作成する
                        logger.info(f"テーブル '{table_name}' を削除します。")
                        drop_table(cursor,table_name, logger)
                        if create_table(cursor,create_statements[0], logger):
                            logger.info(f"テーブル '{table_name}' を作成しました。")
                        else:
                            logger.error(f"テーブル '{table_name}' の作成に失敗しました。")
                    elif choice == "2":
                        # テーブルのデータをクリアする
                        logger.info(f"テーブル '{table_name}' のデータをクリアします。")
                        clear_table(cursor,table_name, logger)

                    elif choice == "3":
                        # スキップしてCREATEを実行しない
                        logger.info(f"テーブル '{table_name}' のCREATEをスキップします。")
                    else:
                        logger.info("無効な選択です。スキップします。")
                        continue

                else:
                    # テーブルを作成する
                    if create_table(cursor,create_statements[0],logger):
                        logger.info(f"テーブル '{table_name}' を作成しました。")
                    else:
                        logger.error(f"テーブル '{table_name}' の作成に失敗しました。")

        logger.info("処理が完了しました。")

        return table_name

    except Exception as e:
        logger.error("処理中にエラーが発生しました。")
        logger.error(str(e))

