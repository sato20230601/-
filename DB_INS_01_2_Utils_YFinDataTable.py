"""
このスクリプトは、YFinanceのAPIを使用してティッカーシンボルからYFinanceのデータを取得し、
そのデータをMySQLデータベースに挿入または更新するPythonスクリプトです。以下にスクリプトの概要を説明します。

1. ファイルを読み込んでディレクトリパスとSQLファイル名を取得します。
2. 「ティッカーシンボルファイル」を読み込んで「ティッカーシンボル」のリストを取得します。
3. MySQLデータベースに接続します。
4. SQLクエリを実行するためのカーソルを取得します。
5. エラーフラグを初期化します。
6. 実行日付を取得します。
7.「SQLファイル(INSERT文)」から「テーブル名」、「メンバー名」、「条件部」を取得する。
8.「ティッカーシンボル」の数分、「YFinanceのAPI」より「ティッカーシンボル」を指定し、YFinanceのデータを取得する。
9. 各ティッカーシンボルに対してINSERTクエリまたはUPDATEクエリを実行します。
10. SELECTクエリを使用してデータの存在を確認します。
11. データが存在する場合はUPDATEクエリを実行します。
12. データが存在しない場合はINSERTクエリを実行します。
13. 変更をコミットし、カーソルとデータベース接続をクローズします。
14. 例外を処理し、エラーをログに記録します。

`DB_Common_Utils.get_table_name_and_members`、
`DB_Common_Utils.read_config_file`、
`DB_Common_Utils.get_mysql_connection`、
`DB_Common_Utils.execute_sql_query`、
`DB_INS_00_Utils.update_data_in_table`、
`DB_INS_00_Utils.insert_data_into_table`
などのコードスニペットは、おそらく別のファイルで定義されたカスタムユーティリティ関数やメソッドであることに注意してください。

"""

import os
import mysql.connector
import csv
import yfinance as yf
import sys
from datetime import datetime
from datetime import date
import glob
import pandas as pd

from configparser import ConfigParser
import logging
import traceback

import requests
from bs4 import BeautifulSoup

# 登録更新処理共通
import DB_INS_00_Utils

# 全処理共通
import DB_Common_Utils

def convert_unix_timestamp(timestamp):
    return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d")

def load_symbols_from_csv(csv_file):
    with open(csv_file, 'r') as symbol_file:
        symbols = [symbol.strip() for symbol in symbol_file.readlines()]

    # 重複を削除してソート
    unique_symbols = sorted(set(symbols))

    return unique_symbols

def get_metadata(table_name,stock,logger):

    is_successful = False  # エラーフラグを初期化

    yf_metadata = None  # yf_metadataの初期値を設定
    try:
        if table_name == "yf_history_metadata":

            yf_metadata = stock.get_history_metadata()

        elif table_name == "yf_stock_info":

            yf_metadata = stock.get_info()

        elif table_name == "yf_financials":

            yf_metadata = stock.get_financials()

        elif table_name == "yf_cash_flow":

            yf_metadata = stock.get_cash_flow()

        elif table_name == "yf_income_statement":

            yf_metadata = stock.get_income_stmt()

        elif table_name == "yf_balancesheet":

            yf_metadata = stock.get_balancesheet()

        elif table_name == "yf_actions":

            yf_metadata = stock.get_actions()

        elif table_name == "yf_major_holders":

            yf_metadata = stock.get_major_holders()
            logger.debug(f"yf_metadata: {yf_metadata}")

        elif table_name == "yf_mutualfund_holders":

            yf_metadata = stock.get_mutualfund_holders()
            logger.debug(f"yf_metadata: {yf_metadata}")

        elif table_name == "yf_shares_full":

            yf_metadata = stock.get_shares_full()

        elif table_name == "yf_isin":

            yf_metadata = stock.get_isin()

        elif table_name == "yf_news":

            yf_metadata = stock.get_news()

        if yf_metadata is not None:
            is_successful = True
            return yf_metadata, is_successful
        else:
            is_successful = "No Data"
            return yf_metadata, is_successful

    except Exception as e:
        logger.error(f"テーブル {table_name} のデータの取得中にエラーが発生しました。処理をスキップします。")
        logger.error(str(e))
        return None,is_successful

def process_data(cursor, symbol, sql_info, logger):
    try:

        logger.info("関数 process_data の実行開始")

        # SQLファイルのデータ取得
        table_name = sql_info.table_name
        members = sql_info.members
        additional_statement = sql_info.additional_statement

        logger.debug(f"symbol: {symbol}")
        logger.debug(f"table_name: {table_name}")
        logger.debug(f"members: {members}")
        logger.debug(f"additional_statement: {additional_statement}")

        is_successful = False  # エラーフラグを初期化

        # YFinanceのAPIより「ティッカーシンボル」を指定し、YFinanceのデータを取得
        stock = yf.Ticker(symbol)

        # 実行日付を取得
        execution_date = date.today()

        yf_metadata, is_successful = get_metadata(table_name,stock,logger)
        logger.debug(f"yf_metadata: {yf_metadata}")

        if is_successful == "No Data":
            logger.debug("データがないため、処理をスキップします。")
            return is_successful

        if is_successful == False:
            logger.error("エラーが発生したため、処理を終了します。")
            return is_successful

        if ( 
            table_name == "yf_shares_full"
        ):
            # yf_metadata.get_shares_full の結果を取得
            shares_full = yf_metadata

            for date_str, shares in shares_full.items():

                # pd.to_datetimeメソッドを使って日付をパース
                dt = pd.to_datetime(date_str)
                DateTime_Share = dt.strftime('%Y-%m-%d %H:%M:%S')

                # yf_shares_full のDateTime_Share,symbolが yf_shares_full テーブルに存在するかチェック
                query = "SELECT DateTime_Share,symbol FROM yf_shares_full WHERE DateTime_Share = %s and symbol = %s"
                cursor.execute(query, (DateTime_Share,symbol))
                result = cursor.fetchone()

                if result:

                    update_conditions = ['DateTime_Share', 'symbol']
                    update_conditions_values = (DateTime_Share, symbol)

                    # データが存在する場合はUPDATE文を実行
                    upd_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    update_columns = [f"{member} = %s" for member in members if member != 'INS_DATE']
                    update_values = (DateTime_Share,symbol,shares,upd_date)

                    # SQL文を表示またはログファイルに書き込み
                    logger.debug(update_values)  # 値をセットしたSQL文を表示

                    # UPDATE文を実行
                    DB_INS_00_Utils.update_data_in_table(cursor, table_name, update_columns, tuple(update_values), update_conditions, update_conditions_values, logger)
                    logger.debug(f"正常に更新されました。:{table_name}:{update_values}:{update_conditions_values}")

                else:
                    # データが存在しない場合はINSERT文を実行
                    ins_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    insert_values = (DateTime_Share,symbol,shares,ins_date,None)

                    # SQL文を表示またはログファイルに書き込み
                    logger.debug(insert_values)  # 値をセットしたSQL文を表示

                    # INSERT文を実行
                    DB_INS_00_Utils.insert_data_into_table(cursor, table_name, members, tuple(insert_values), logger)
                    logger.debug(f"正常に登録されました。:{table_name}:{members}:{insert_values}")

            is_successful = True  # データの挿入または更新が成功した場合にフラグを設定

            logger.info(f"処理が完了しました。")
            return is_successful

        if ( 
            table_name == "yf_news"
        ):
            # yf_metadata.get_news の結果を取得
            news_data = yf_metadata

            for news in news_data:

                uuid = news['uuid']
                title = news['title']
                publisher = news['publisher']
                link = news['link']
                providerPublishTime = news['providerPublishTime']
                type = news['type']
                relatedTickers = str(news['relatedTickers'])

                # yf_news のDate_YYYYMMDD,symbol,uuidが yf_news テーブルに存在するかチェック
                query = "SELECT Date_YYYYMMDD,symbol,uuid FROM yf_news WHERE Date_YYYYMMDD = %s and symbol = %s and uuid = %s"
                cursor.execute(query, (execution_date,symbol,uuid))
                result = cursor.fetchone()

                if result:

                    update_conditions = ['Date_YYYYMMDD', 'symbol','uuid']
                    update_conditions_values = (execution_date,symbol,uuid)

                    # データが存在する場合はUPDATE文を実行
                    upd_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    update_columns = [f"{member} = %s" for member in members if member != 'INS_DATE']
                    update_values = (execution_date,symbol,uuid,title,publisher,link,providerPublishTime,type,relatedTickers,upd_date)

                    # SQL文を表示またはログファイルに書き込み
                    logger.debug(update_values)  # 値をセットしたSQL文を表示

                    # UPDATE文を実行
                    DB_INS_00_Utils.update_data_in_table(cursor, table_name, update_columns, tuple(update_values), update_conditions, update_conditions_values, logger)
                    logger.debug(f"正常に更新されました。:{table_name}:{update_values}:{update_conditions_values}")

                else:
                    # データが存在しない場合はINSERT文を実行
                    ins_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    insert_values = (execution_date,symbol,uuid,title,publisher,link,providerPublishTime,type,relatedTickers,ins_date,None)

                    # SQL文を表示またはログファイルに書き込み
                    logger.debug(insert_values)  # 値をセットしたSQL文を表示

                    # INSERT文を実行
                    DB_INS_00_Utils.insert_data_into_table(cursor, table_name, members, tuple(insert_values), logger)
                    logger.debug(f"正常に登録されました。:{table_name}:{members}:{insert_values}")

            is_successful = True  # データの挿入または更新が成功した場合にフラグを設定

            logger.info(f"処理が完了しました。")
            return is_successful

        if ( 
            table_name == "yf_major_holders"
            or table_name == "yf_mutualfund_holders"
        ):
            # yf_metadata.get_major_holders の結果を取得
            holders = yf_metadata

            for index, row in holders.iterrows():

                if table_name == "yf_major_holders":

                    category_name = row[1]  # カテゴリ名の列
                    value = row[0]  # 値の列

                else:
                    category_name = row[0]  # カテゴリ名の列
                    value = row[1]  # 値の列

                # category_name のカテゴリが holder_categories テーブルに存在するかチェック
                query = "SELECT id FROM yf_holder_categories WHERE category_name = %s"
                cursor.execute(query, (category_name,))
                result = cursor.fetchone()

                if result is None:
                    # カテゴリが存在しない場合は、新しいカテゴリを追加
                    insdate = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    query = "INSERT INTO yf_holder_categories (category_name,INS_DATE) VALUES (%s,%s)"
                    cursor.execute(query, (category_name,insdate))
                    category_id = cursor.lastrowid
                else:
                    # カテゴリが既に存在する場合は、そのカテゴリの id を取得
                    category_id = result[0]

                select_query = f"SELECT * FROM {table_name} WHERE symbol = %s AND Date_YYYYMMDD = %s AND category_id = %s"
                select_values = (symbol, execution_date, category_id)

                update_conditions = ['symbol', 'Date_YYYYMMDD','category_id']
                update_conditions_values = (symbol, execution_date,category_id)

                # SQL文を表示またはログファイルに書き込み
                logger.debug("実行するSQL文:")
                logger.debug(select_query % tuple(select_values))  # 値をセットしたSQL文を表示

                result = DB_Common_Utils.execute_sql_query(cursor, select_query, select_values, logger)
                logger.debug(f"SELECTの結果: {result}")

                if result:
                    # データが存在する場合はUPDATE文を実行
                    upd_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    update_columns = [f"{member} = %s" for member in members if member != 'INS_DATE']
                    update_values = (execution_date,symbol,category_id,value,upd_date)

                    # SQL文を表示またはログファイルに書き込み
                    logger.debug(update_values)  # 値をセットしたSQL文を表示

                    # UPDATE文を実行
                    DB_INS_00_Utils.update_data_in_table(cursor, table_name, update_columns, tuple(update_values), update_conditions, update_conditions_values, logger)
                    logger.debug(f"正常に更新されました。:{table_name}:{update_values}:{update_conditions_values}")

                else:
                    # データが存在しない場合はINSERT文を実行
                    ins_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    insert_values = (execution_date,symbol,category_id,value,ins_date,None)

                    # SQL文を表示またはログファイルに書き込み
                    logger.debug(insert_values)  # 値をセットしたSQL文を表示

                    # INSERT文を実行
                    DB_INS_00_Utils.insert_data_into_table(cursor, table_name, members, tuple(insert_values), logger)
                    logger.debug(f"正常に登録されました。:{table_name}:{members}:{insert_values}")

            is_successful = True  # データの挿入または更新が成功した場合にフラグを設定

            logger.info(f"処理が完了しました。")
            return is_successful

        # データをテーブルに挿入または更新
        insert_values = []
        update_values = []
        for member in members:
            try:
                if member == 'INS_DATE':
                    value = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    logger.debug(f"1:{member}:{value}")
                    insert_values.append(value)

                elif member == 'UPD_DATE':
                    value = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    logger.debug(f"2:{member}:{value}")
                    insert_values.append(None)
                    update_values.append(value)

                elif member == 'Date_YYYYMMDD':

                    insert_values.append(execution_date)
                    update_values.append(execution_date)

                elif member == 'category_id':

                    insert_values.append(category_id)
                    update_values.append(category_id)

                elif member == 'symbol':

                    insert_values.append(symbol)
                    update_values.append(symbol)

                elif member == 'validRanges':

                    value = ', '.join(yf_metadata.get(member, []))
                    insert_values.append(value)
                    update_values.append(value)

                elif member == 'firstTradeDate':

                    value = convert_unix_timestamp(yf_metadata.get(member, None))
                    insert_values.append(value)
                    update_values.append(value)

                elif member == 'regularMarketTime':

                    value = convert_unix_timestamp(yf_metadata.get(member, None))
                    insert_values.append(value)
                    update_values.append(value)

                elif member == 'ISIN':

                    insert_values.append(yf_metadata)
                    update_values.append(yf_metadata)

                else:

                    # yf_financials
                    if ( 
                        table_name == "yf_financials"
                        or table_name == "yf_financials"
                        or table_name == "yf_cash_flow"
                        or table_name == "yf_income_statement"
                        or table_name == "yf_balancesheet"
                    ):
                        # カラムが存在するかチェック
                        if member in yf_metadata.index:
                            # 直近の列の値を取得
                            value = yf_metadata.loc[member].iloc[-1]
                            if pd.isna(value):
                                value = None

                            insert_values.append(value)
                            update_values.append(value)
                        else:
                            logger.debug(f"カラムが存在しないためスキップします: {table_name}.{member}")
                            insert_values.append(None)
                            update_values.append(None)
                    elif ( 
                        table_name == "yf_actions"
                    ):

                        # 直近のアクションデータを取得
                        actions = yf_metadata.tail(1)

                        # カラムの件数を取得
                        column_count = actions.shape[1]

                        # カラムが存在するかチェック
                        if column_count > 0:

                            if member == "ActDate":
                                value = actions.index[-1]

                                if value is None:
                                    continue

                            elif member == "StockSplits":
                                value = actions['Stock Splits'].tail(1).values[0]

                            else:
                                value = actions[member].tail(1).values[0]

                                if pd.isna(value):
                                    value = None

                            insert_values.append(value)
                            update_values.append(value)
                        else:
                            logger.debug(f"カラムが存在しないためスキップします: {table_name}.{member}")
                            insert_values.append(None)
                            update_values.append(None)


                    else:
                        value = yf_metadata.get(member, None)

                        if value == "Infinity":

                            value = None
                            logger.debug(f"infinityの置換:{member}:{value}")

                        insert_values.append(value)
                        update_values.append(value)

            except Exception as e:
                logger.error(f"エラーが発生しました: {str(e)}")
                insert_values.append(None)
                update_values.append(None)
                
                continue

        # SELECT文を実行してデータの存在有無を確認
        # テーブルに実行日付を条件に追加
        if (
            table_name == "yf_history_metadata"
            or table_name == "yf_stock_info"
            or table_name == "yf_financials"
            or table_name == "yf_cash_flow"
            or table_name == "yf_income_statement"
            or table_name == "yf_balancesheet"
            or table_name == "yf_major_holders"
            or table_name == "yf_mutualfund_holders"
            or table_name == "yf_shares_full"
            or table_name == "yf_news"
        ):

            select_query = f"SELECT * FROM {table_name} WHERE symbol = %s AND Date_YYYYMMDD = %s "
            select_values = (symbol, execution_date)

            update_conditions = ['symbol', 'Date_YYYYMMDD']
            update_conditions_values = (symbol, execution_date)

        else:
            select_query = f"SELECT * FROM {table_name} WHERE symbol = %s"
            select_values = (symbol,)

            update_conditions = ['symbol']
            update_conditions_values = (symbol,)

        # SQL文を表示またはログファイルに書き込み
        logger.debug("実行するSQL文:")
        logger.debug(select_query % tuple(select_values))  # 値をセットしたSQL文を表示

        result = DB_Common_Utils.execute_sql_query(cursor, select_query, select_values, logger)
        logger.debug(f"SELECTの結果: {result}")

        if result:
            # データが存在する場合はUPDATE文を実行
            update_columns = [f"{member} = %s" for member in members if member != 'INS_DATE']

            # SQL文を表示またはログファイルに書き込み
            logger.debug(update_values)  # 値をセットしたSQL文を表示

            # UPDATE文を実行
            DB_INS_00_Utils.update_data_in_table(cursor, table_name, update_columns, tuple(update_values), update_conditions, update_conditions_values, logger)
            logger.debug(f"正常に更新されました。:{table_name}:{update_values}:{update_conditions_values}")

        else:
            # データが存在しない場合はINSERT文を実行

            # SQL文を表示またはログファイルに書き込み
            logger.debug(insert_values)  # 値をセットしたSQL文を表示

            # INSERT文を実行
            DB_INS_00_Utils.insert_data_into_table(cursor, table_name, members, tuple(insert_values), logger)
            logger.debug(f"正常に登録されました。:{table_name}:{members}:{insert_values}")

        logger.info(f"処理が完了しました。")
        is_successful = True  # データの挿入または更新が成功した場合にフラグを設定
        return is_successful

    except Exception as e:

        logger.error("処理中にエラーが発生しました。")
        logger.error(str(e))
        logger.error(traceback.format_exc())  # 修正: トレースバック情報をログに出力
        logger.error("エラーが発生したため、処理を終了します。")
        return is_successful
