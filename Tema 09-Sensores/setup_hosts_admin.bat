@echo off
REM ================================================================
REM setup_hosts.bat — Adiciona mongo1/mongo2/mongo3 ao arquivo hosts
REM EXECUTE COMO ADMINISTRADOR (clique direito -> Executar como admin)
REM ================================================================

echo Verificando permissoes de administrador...
net session >nul 2>&1
IF %ERRORLEVEL% NEQ 0 (
    echo [ERRO] Este script precisa ser executado como Administrador.
    echo Clique com o botao direito em setup_hosts.bat e selecione
    echo "Executar como administrador".
    pause
    exit /b 1
)

set HOSTS_FILE=C:\Windows\System32\drivers\etc\hosts

REM Verificar se ja existe
findstr /C:"mongo1" "%HOSTS_FILE%" >nul 2>&1
IF %ERRORLEVEL% EQU 0 (
    echo [!] Entradas mongo1/mongo2/mongo3 ja existem no arquivo hosts.
    echo Conteudo atual:
    findstr "mongo" "%HOSTS_FILE%"
    pause
    exit /b 0
)

REM Adicionar entradas
echo. >> "%HOSTS_FILE%"
echo # Tema 09 - MongoDB Replica Set Docker >> "%HOSTS_FILE%"
echo 127.0.0.1 mongo1 >> "%HOSTS_FILE%"
echo 127.0.0.1 mongo2 >> "%HOSTS_FILE%"
echo 127.0.0.1 mongo3 >> "%HOSTS_FILE%"

echo [OK] Entradas adicionadas ao arquivo hosts:
findstr "mongo" "%HOSTS_FILE%"
echo.
echo Agora voce pode usar a URI: mongodb://mongo1:27017/?replicaSet=rs0
pause
