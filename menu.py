import os

def show_menu():
    print("===== メニュー =====")
    print("1. YFinanceの情報取得")
    print("2. 楽天証券の情報取得")
    print("3. sp500情報取得")
    print("4. ラッセル2000取得")
    print("5. 決算日情報取得")
    print("6. 経済カレンダー情報取得")
    print("7. 取引履歴情報の取込み")
    print("q. 終了")
    print("===================")

def show_sub_menu():
    print("a) テーブル作成")
    print("b) データ収集/登録")

def execute_script(script_path):
    command = f'c:/Users/sabe2/OneDrive/デスクトップ/Python/venv/Scripts/python.exe {script_path}'
    os.system(command)

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
        
        elif choice == "q":
            print("プログラムを終了します。")
            break
        
        else:
            print("無効な選択肢です。もう一度選択してください。")

if __name__ == "__main__":
    main()
