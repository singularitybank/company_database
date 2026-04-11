@echo off
:: ============================================================
:: ハローワーク 日次バッチ処理
:: タスクスケジューラに登録するバッチファイル
::
:: [タスクスケジューラの設定]
::   操作:        プログラムの開始
::   プログラム:  C:\Users\singu\github\company_database\scripts\run_hellowork.bat
::   作業フォルダ: C:\Users\singu\github\company_database
:: ============================================================
setlocal

set PROJECT_DIR=C:\Users\singu\github\company_database
set CONDA_ROOT=C:\Users\singu\anaconda3
set CONDA_ENV=data

:: conda 環境を activate
call "%CONDA_ROOT%\Scripts\activate.bat" %CONDA_ENV%
if errorlevel 1 (
    echo [ERROR] conda環境のactivateに失敗しました: %CONDA_ENV%
    exit /b 1
)

:: バッチ処理を実行
:: NOTE: タスクスケジューラではブラウザウィンドウが表示されないため --headless を指定
::       手動実行でブラウザを表示したい場合は --headless を削除する
python "%PROJECT_DIR%\scripts\run_hellowork.py" --headless
set EXIT_CODE=%errorlevel%

call conda deactivate
exit /b %EXIT_CODE%
