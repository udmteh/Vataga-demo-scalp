import sqlite3
import os
import sys

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

SETTINGS_DIR = os.path.expandvars(r"%APPDATA%\Vataga\Vataga.terminal\Settings")
PROXIES_DB = os.path.join(SETTINGS_DIR, "proxies.db")
EXCHANGES_DB = os.path.join(SETTINGS_DIR, "exchanges.db")

def uninstall():
    print("=" * 60)
    print("  🧹 VATAGA DEMO BRIDGE by CGTehnik — УДАЛЕНИЕ ДЕМО-ПОДКЛЮЧЕНИЙ")
    print("=" * 60)

    try:
        conn = sqlite3.connect(EXCHANGES_DB)
        cur = conn.cursor()
        cur.execute("DELETE FROM BybitUserConnections WHERE Title LIKE '%by CGTehnik%' OR Title LIKE '%ZENICA%'")
        cur.execute("DELETE FROM BinanceUserConnections WHERE Title LIKE '%by CGTehnik%' OR Title LIKE '%ZENICA%'")
        conn.commit()
        conn.close()
        print("✅ Удалены демо-подключения из exchanges.db")
    except Exception as e:
        print(f"Ошибка exchanges.db: {e}")

    try:
        conn = sqlite3.connect(PROXIES_DB)
        cur = conn.cursor()
        cur.execute("DELETE FROM Proxies WHERE Host='127.0.0.1' AND Port=8989")
        conn.commit()
        conn.close()
        print("✅ Удален прокси-шлюз из proxies.db")
    except Exception as e:
        print(f"Ошибка proxies.db: {e}")

    print("=" * 60)
    print("✨ Терминал Ватага возвращен в исходное состояние!")
    print("=" * 60)

if __name__ == "__main__":
    uninstall()
