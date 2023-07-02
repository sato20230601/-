import os
from configparser import ConfigParser

# 共通関数の読み込み
import DB_Common_Utils

#configファイルより取得
config_path = r"C:\Users\sabe2\OneDrive\デスクトップ\Python\06_DATABASE\06-03_SRC\config.txt"
config = DB_Common_Utils.read_config_file(config_path)

def show_menu():
    os.system("cls")  # ページの先頭に移動
    print("===== メニュー =====")
    print("1. YFinanceの情報取得")
    print("2. 楽天証券の情報取得")
    print("3. sp500情報取得")
    print("4. ラッセル2000取得")
    print("5. 決算日情報取得")
    print("6. 経済カレンダー情報取得")
    print("7. 取引履歴情報の取込み(楽天証券)")
    print("8. 資産合計・保有商品詳細の取込み(楽天証券)")
    print("9. ログの確認")
    print("10.SQLファイルの確認")
    print("11.SQL配置ファイルの確認")
    print("12.ソースの確認")
    print("q. 終了")
    print("===================")

def show_sub_menu():
    print("a) テーブル作成")
    print("b) データ収集/登録")

def execute_script(script_path):
    command = f'c:/Users/sabe2/OneDrive/デスクトップ/Python/venv/Scripts/python.exe {script_path}'
    os.system(command)

def show_dir_files(folder):

    while True:

        files = os.listdir(folder)
        os.system("cls")  # ページの先頭に移動
        print("===== ファイル一覧 =====")
        for i, file in enumerate(files):
            file_path = os.path.join(folder, file)
            if os.path.isfile(file_path):  # ファイルのみ表示
                print(f"{i+1}. {file}")
        print("==========================")

        choice = input("表示するファイルの番号を入力してください (q: 戻る): ")
        if choice == "q":
            return choice

        try:
            file_index = int(choice) - 1
            if file_index < 0 or file_index >= len(files):
                print("無効な選択肢です。もう一度選択してください。")
                continue

            log_file = files[file_index]
            log_path = os.path.join(folder, log_file)

            display_log_file(log_path)
            while True:
                sub_choice = input("q: メニューに戻る　n: ファイルのリストを表示して選択する d: ファイルを削除する")
                if sub_choice == "q":
                    return sub_choice
                elif sub_choice == "n":
                    break
                elif sub_choice == "d":
                    delete_log_file(log_path)
                    os.system("cls")  # ページの先頭に移動
                    print(f"ファイル {log_file} を削除しました。")
                    input("エンターキーを押して続行してください。")
                    sub_choice = "n"
                    break
                else:
                    print("無効な選択肢です。メニューに戻ります。")

        except ValueError:
            print("無効な選択肢です。もう一度選択してください。")

def display_log_file(file_path):
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            lines = file.readlines()
            page_size = 10  # 1ページに表示する行数を指定（この場合は10行）
            total_pages = (len(lines) // page_size) + 1  # 全体のページ数を計算
            current_page = 1  # 現在のページを初期化

            while True:
                os.system('cls' if os.name == 'nt' else 'clear')  # 画面をクリア
                print(f"=== {file_path} の内容 ===")
                start_index = (current_page - 1) * page_size
                end_index = start_index + page_size
                print("".join(lines[start_index:end_index]))

                print(f"ページ: {current_page}/{total_pages}")

                choice = input("Enterキーを押すと次のページを表示します。qを入力して終了します。")
                if choice == "q":
                    break

                if current_page < total_pages:
                    current_page += 1
                else:
                    break

    except FileNotFoundError:
        print("指定されたログファイルが見つかりません。")

def get_dir_file_path(sub_choice,dir_path):
    files = os.listdir(dir_path)
    try:
        log_index = int(sub_choice) - 1
        if log_index >= 0 and log_index < len(files):
            return os.path.join(dir_path, files[log_index])
    except ValueError:
        pass
    return None

def delete_log_file(file_path):
    try:
        os.remove(file_path)
        print("ログファイルを削除しました。")
    except FileNotFoundError:
        print("ログファイルが見つかりませんでした。")

def main():
    while True:
        show_menu()
        choice = input("選択肢を入力してください: ")

        if choice == "1":
            show_sub_menu()
            sub_choice = input("選択肢を入力してください: ")

            if sub_choice == "a":
                execute_script(r"c:/Users/sabe2/OneDrive/デスクトップ/Python/06_DATABASE/06-03_SRC/DB_CRE_01_YFinDataTable.py")
            elif sub_choice == "b":
                execute_script(r"c:/Users/sabe2/OneDrive/デスクトップ/Python/06_DATABASE/06-03_SRC/DB_INS_01_1_YFinDataTable.py")
            else:
                print("無効な選択肢です。もう一度選択してください。")

        elif choice == "2":
            show_sub_menu()
            sub_choice = input("選択肢を入力してください: ")

            if sub_choice == "a":
                execute_script(r"c:/Users/sabe2/OneDrive/デスクトップ/Python/06_DATABASE/06-03_SRC/DB_CRE_02_RakFinTraRst.py")
            elif sub_choice == "b":
                execute_script(r"c:/Users/sabe2/OneDrive/デスクトップ/Python/06_DATABASE/06-03_SRC/DB_INS_02_1_RakFinTraRst.py")
            else:
                print("無効な選択肢です。もう一度選択してください。")

        elif choice == "3":
            show_sub_menu()
            sub_choice = input("選択肢を入力してください: ")

            if sub_choice == "a":
                execute_script(r"c:/Users/sabe2/OneDrive/デスクトップ/Python/06_DATABASE/06-03_SRC/DB_CRE_03_sp500DataTable.py")
            elif sub_choice == "b":
                execute_script(r"c:/Users/sabe2/OneDrive/デスクトップ/Python/06_DATABASE/06-03_SRC/DB_INS_03_1_sp500DataTable.py")
            else:
                print("無効な選択肢です。もう一度選択してください。")

        elif choice == "4":
            show_sub_menu()
            sub_choice = input("選択肢を入力してください: ")

            if sub_choice == "a":
                execute_script(r"c:/Users/sabe2/OneDrive/デスクトップ/Python/06_DATABASE/06-03_SRC/DB_CRE_04_rus2000DataTable.py")
            elif sub_choice == "b":
                execute_script(r"c:/Users/sabe2/OneDrive/デスクトップ/Python/06_DATABASE/06-03_SRC/DB_INS_04_1_rus2000DataTable.py")
            else:
                print("無効な選択肢です。もう一度選択してください。")

        elif choice == "5":
            show_sub_menu()
            sub_choice = input("選択肢を入力してください: ")

            if sub_choice == "a":
                execute_script(r"c:/Users/sabe2/OneDrive/デスクトップ/Python/06_DATABASE/06-03_SRC/DB_CRE_05_FinCloseDateTable.py")
            elif sub_choice == "b":
                execute_script(r"c:/Users/sabe2/OneDrive/デスクトップ/Python/06_DATABASE/06-03_SRC/DB_INS_05_1_FinCloseDateTable.py")
            else:
                print("無効な選択肢です。もう一度選択してください。")

        elif choice == "6":
            show_sub_menu()
            sub_choice = input("選択肢を入力してください: ")

            if sub_choice == "a":
                execute_script(r"c:/Users/sabe2/OneDrive/デスクトップ/Python/06_DATABASE/06-03_SRC/DB_CRE_06_EcoCalender.py")
            elif sub_choice == "b":
                execute_script(r"c:/Users/sabe2/OneDrive/デスクトップ/Python/06_DATABASE/06-03_SRC/DB_INS_06_1_EcoCalender.py")
            else:
                print("無効な選択肢です。もう一度選択してください。")

        elif choice == "7":
            show_sub_menu()
            sub_choice = input("選択肢を入力してください: ")

            if sub_choice == "a":
                execute_script(r"c:/Users/sabe2/OneDrive/デスクトップ/Python/06_DATABASE/06-03_SRC/DB_CRE_07_Trading_History.py")
            elif sub_choice == "b":
                execute_script(r"c:/Users/sabe2/OneDrive/デスクトップ/Python/06_DATABASE/06-03_SRC/DB_INS_07_1_Trading_History.py")
            else:
                print("無効な選択肢です。もう一度選択してください。")

        elif choice == "8":
            show_sub_menu()
            sub_choice = input("選択肢を入力してください: ")

            if sub_choice == "a":
                execute_script(r"c:/Users/sabe2/OneDrive/デスクトップ/Python/06_DATABASE/06-03_SRC/DB_CRE_08_AsetHoldingData.py")
            elif sub_choice == "b":
                execute_script(r"c:/Users/sabe2/OneDrive/デスクトップ/Python/06_DATABASE/06-03_SRC/DB_INS_08_1_AsetHoldingData.py")
            else:
                print("無効な選択肢です。もう一度選択してください。")

        elif choice == "9":

            log_folder = config.get("LOG_FOLDER")

            sub_choice_9 = show_dir_files(log_folder)
            if sub_choice_9 == "q":
                choice = "q"
                continue
            else:
                sub_choice = input("ログファイルの番号を入力してください: ")
                file_path = get_dir_file_path(sub_choice,log_folder)
                if file_path:
                    display_log_file(file_path)
                else:
                    print("無効な選択肢です。もう一度選択してください。")

        elif choice == "10":

            sql_folder = config.get("SQL_FOLDER")

            sub_choice_10 = show_dir_files(sql_folder)
            if sub_choice_10 == "q":
                choice = "q"
                continue
            else:
                sub_choice = input("SQLファイルの番号を入力してください: ")
                file_path = get_dir_file_path(sub_choice,sql_folder)
                if file_path:
                    display_log_file(file_path)
                else:
                    print("無効な選択肢です。もう一度選択してください。")

        elif choice == "11":

            sql_adm_folder = config.get("SQL_ADM_FOLDER")

            sub_choice_11 = show_dir_files(sql_adm_folder)
            if sub_choice_11 == "q":
                choice = "q"
                continue
            else:
                sub_choice = input("SQLファイルの番号を入力してください: ")
                file_path = get_dir_file_path(sub_choice,sql_adm_folder)
                if file_path:
                    display_log_file(file_path)
                else:
                    print("無効な選択肢です。もう一度選択してください。")

        elif choice == "12":

            sorce_folder = config.get("SORCE_FOLDER")

            sub_choice_12 = show_dir_files(sorce_folder)
            if sub_choice_12 == "q":
                choice = "q"
                continue
            else:
                sub_choice = input("ソースファイルの番号を入力してください: ")
                file_path = get_dir_file_path(sub_choice,sorce_folder)
                if file_path:
                    display_log_file(file_path)
                else:
                    print("無効な選択肢です。もう一度選択してください。")

        elif choice == "q":
            print("プログラムを終了します。")
            break

        else:
            print("無効な選択肢です。もう一度選択してください。")

if __name__ == "__main__":
    main()
