@echo off
:: ============================================================
:: PR Times RSS 収集バッチ
:: タスクスケジューラに登録するバッチファイル
::
:: [タスクスケジューラの設定]
::   操作:        プログラムの開始
::   プログラム:  C:\Users\singu\github\company_database\scripts\run_prtimes_rss.bat
::   作業フォルダ: C:\Users\singu\github\company_database
::
:: [トリガー設定]
::   開始:        当日 00:00:00
::   繰り返し間隔: 1時間
::   継続時間:    無制限
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

:: RSS 収集バッチを実行
python "%PROJECT_DIR%\scripts\run_prtimes_rss.py"
set EXIT_CODE=%errorlevel%

call conda deactivate
exit /b %EXIT_CODE%
