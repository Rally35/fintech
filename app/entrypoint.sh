#!/bin/bash
set -e

# Ustawienia domyślne, jeśli zmienne środowiskowe nie są podane
HOST="${DB_HOST:-db}"
PORT="${DB_PORT:-5432}"
USER="${DB_USER:-postgres}"

echo "Czekam na bazę danych Postgres pod adresem $HOST:$PORT..."

# Pętla czekająca na gotowość bazy danych
# Wymaga zainstalowanego pakietu postgresql-client (jest w Dockerfile)
until pg_isready -h "$HOST" -p "$PORT" -U "$USER"; do
  >&2 echo "Postgres jest niedostępny - czekam..."
  sleep 2
done

>&2 echo "Postgres jest gotowy! Uruchamiam skrypty..."

# Uruchomienie skryptów Pythona
python scripts/import_quarterly.py
python scripts/update_prices.py

echo "Skrypty wykonane. Uruchamiam aplikację..."

# Wykonanie głównej komendy przekazanej w CMD (Streamlit)
exec "$@"