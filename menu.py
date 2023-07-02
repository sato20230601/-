import os

LOG_FOLDER = r"c:/Users/sabe2/OneDrive/デスクトップ/Python/06_DATABASE/06-03_SRC/LOG"

def show_menu():
    print("===== メニュー =====")
    print("1. YFinanceの情報取得")
    print("2. 楽天証券の情報取得")
    print("3. sp500情報取得")
    print("4. ラッセル2000取得")
    print("5. 決算日情報取得")
    print("6. 経済カレンダー情報取得")
    print("7. 取引履歴情報の取込み")
    print("8. 資産合計・保有商品詳細の取込み")
    print("9. ログの確認")
    print("q. 終了")
    print("===================")

def show_sub_menu():
    print("a) テーブル作成")
    print("b) データ収集/登録")

def execute_script(script_path):
    command = f'c:/Users/sabe2/OneDrive/デスクトップ/Python/venv/Scripts/python.exe {script_path}'
    os.system(command)

def show_log_files():

    while True:

        files = os.listdir(LOG_FOLDER)
        print("===== ログファイル一覧 =====")
        for i, file in enumerate(files):
            print(f"{i+1}. {file}")
        print("==========================")

        choice = input("表示するログの番号を入力してください (q: 戻る): ")
        if choice == "q":
            return

        try:
            log_index = int(choice) - 1
            if log_index < 0 or log_index >= len(files):
                print("無効な選択肢です。もう一度選択してください。")
                continue

            log_file = files[log_index]
            log_path = os.path.join(LOG_FOLDER, log_file)

            display_log_file(log_path)

            while True:
                sub_choice = input("q: メニューに戻る　n: ログのリストを表示して選択する")
                if sub_choice == "q":
                    return sub_choice
                elif sub_choice == "n":
                    break
                else:
                    print("無効な選択肢です。メニューに戻ります。")

           # ログのリストを再表示
            print("===== ログファイル一覧 =====")
            for i, file in enumerate(files):
                print(f"{i+1}. {file}")
            print("==========================")

        except ValueError:
            print("無効な選択肢です。もう一度選択してください。")

def display_log_file(file_path):
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            print(f"=== {file_path} の内容 ===")
            print(file.read())
            print("=====================")
    except FileNotFoundError:
        print("指定されたログファイルが見つかりません。")

def get_log_file_path(sub_choice):
    log_dir = r"c:/Users/sabe2/OneDrive/デスクトップ/Python/06_DATABASE/06-03_SRC/LOG"
    files = os.listdir(log_dir)
    try:
        log_index = int(sub_choice) - 1
        if log_index >= 0 and log_index < len(files):
            return os.path.join(log_dir, files[log_index])
    except ValueError:
        pass
    return None

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

            sub_choice_9 = show_log_files()
            if sub_choice_9 == "q":
                choice = "q"
                continue
            else:
                sub_choice = input("ログファイルの番号を入力してください: ")
                file_path = get_log_file_path(sub_choice)
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
