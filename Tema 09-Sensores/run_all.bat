@echo off
REM ================================================================
REM run_all.bat — Orquestra toda a execução do Tema 09
REM Execute com: run_all.bat
REM ================================================================

echo.
echo ================================================================
echo   TEMA 09 — Sensores que escrevem mais rapido que o banco
echo   MongoDB + Apache Cassandra + Docker + Python
echo ================================================================
echo.

REM ── Pré-requisito: Docker Desktop rodando ──────────────────────
docker info >nul 2>&1
IF %ERRORLEVEL% NEQ 0 (
    echo [ERRO] Docker nao encontrado ou nao esta rodando.
    echo        Instale Docker Desktop: https://www.docker.com/products/docker-desktop/
    pause
    exit /b 1
)

REM ── Instalar dependências Python ───────────────────────────────
echo [1/8] Instalando dependencias Python...
pip install -r python\requirements.txt --quiet
echo       OK

REM ================================================================
REM PARTE A — CASSANDRA
REM ================================================================
echo.
echo ----------------------------------------------------------------
echo  PARTE A: Cluster Cassandra (3 nos)
echo ----------------------------------------------------------------

echo [2/8] Subindo cluster Cassandra...
cd cassandra
docker compose up -d
echo       Aguardando no seed (cassandra-1) ficar saudavel...

:wait_cassandra
timeout /t 15 /nobreak >nul
docker exec cassandra-1 nodetool status >nul 2>&1
IF %ERRORLEVEL% NEQ 0 (
    echo       Ainda aguardando...
    goto wait_cassandra
)
echo       Cassandra UP!

echo [3/8] Verificando status do cluster...
docker exec cassandra-1 nodetool status

echo [4/8] Criando schema no Cassandra...
cd ..\python
python setup_cassandra.py

echo [5/8] Executando benchmark Cassandra...
python benchmark_cassandra.py

REM ================================================================
REM PARTE B — MONGODB
REM ================================================================
echo.
echo ----------------------------------------------------------------
echo  PARTE B: Cluster MongoDB Replica Set (3 nos)
echo ----------------------------------------------------------------

REM Para o Cassandra antes de subir o MongoDB (poupar RAM)
echo [6/8] Parando Cassandra para liberar RAM...
cd ..\cassandra
docker compose down
cd ..

echo [7/8] Subindo cluster MongoDB...
cd mongodb
docker compose up -d
echo       Aguardando Replica Set inicializar (30s)...
timeout /t 35 /nobreak >nul

echo [8/8] Criando schema e executando benchmark MongoDB...
cd ..\python
python setup_mongodb.py
python benchmark_mongodb.py

REM ── Relatório comparativo ──────────────────────────────────────
echo.
echo ----------------------------------------------------------------
echo  RELATORIO COMPARATIVO
echo ----------------------------------------------------------------
python comparar_resultados.py

echo.
echo ================================================================
echo   Concluido! Resultados em: resultados\
echo ================================================================

REM Limpeza opcional
echo.
set /p LIMPAR="Derrubar os containers Docker? [S/N]: "
IF /I "%LIMPAR%"=="S" (
    cd ..\mongodb
    docker compose down -v
    echo Containers removidos.
)

pause
