

== CRIAR BACKUP COMPLETO - VIA POWER SHELL ==

$env:PGPASSWORD = 'SENHA' 
& "C:\Program Files\PostgreSQL\17\bin\pg_dumpall.exe" -U postgres -f "C:\Users\Usuario\Postgres\backup_completo.sql"
--------------------------------------------------------


== RESTAURAR BACKUP COMPLETO - VIA POWER SHELL ==

psql -U postgres -f "C:\Users\Usuario\Postgres\backup_completo.sql"
--------------------------------------------------------
