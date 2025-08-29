

== FAZER BACKUP TABELAS ==

@echo off
REM === CONFIGURAÇÕES ===
set PGPASSWORD=XXXXXXXXXXX
set PG_BIN="C:\Program Files\PostgreSQL\17\bin"
set BACKUP_DIR="C:\Users\Guilherme\postgres"
set DB_NAME=erp_dw

REM === CRIA NOME DO ARQUIVO COM DATA ===
for /f %%i in ('wmic os get LocalDateTime ^| find "."') do set dt=%%i
set DATA=%dt:~0,4%-%dt:~4,2%-%dt:~6,2%_%dt:~8,2%-%dt:~10,2%

set FILE_NAME=%BACKUP_DIR%\%DB_NAME%_%DATA%.backup

REM === EXECUTA BACKUP ===
call %PG_BIN%\pg_dump.exe -U postgres -h localhost -p 5432 -F c -b -v -f "%FILE_NAME%" %DB_NAME%

REM === MENSAGEM FINAL ===
echo Backup concluído: %FILE_NAME%
pause

-----------------------------------


== FAZER BACKUP TABELAS ==

@echo off
REM === CONFIGURAÇÕES ===
set PGPASSWORD=XXXXXXXXXXX
set PG_BIN="C:\Program Files\PostgreSQL\17\bin"
set BACKUP_DIR="C:\Users\Guilherme\postgres"

REM === CRIA NOME DO ARQUIVO COM DATA ===
for /f %%i in ('wmic os get LocalDateTime ^| find "."') do set dt=%%i
set DATA=%dt:~0,4%-%dt:~4,2%-%dt:~6,2%_%dt:~8,2%-%dt:~10,2%

set FILE_NAME=%BACKUP_DIR%\roles_%DATA%.sql

REM === EXECUTA BACKUP DAS ROLES ===
call %PG_BIN%\pg_dumpall.exe -U postgres --globals-only -f "%FILE_NAME%"

REM === MENSAGEM FINAL ===
echo Backup de roles concluído: %FILE_NAME%
pause

-----------------------------------