import sqlite3
import os
import datetime
import uuid
import sys

# Configure UTF-8 stdout for Windows consoles
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

SETTINGS_DIR = os.path.expandvars(r"%APPDATA%\Vataga\Vataga.terminal\Settings")
PROXIES_DB = os.path.join(SETTINGS_DIR, "proxies.db")
EXCHANGES_DB = os.path.join(SETTINGS_DIR, "exchanges.db")

def install():
    print("=" * 60)
    print("  🔧 VATAGA DEMO BRIDGE by CGTehnik — УСТАНОВЩИК ДЕМО-РЕЖИМА")
    print("=" * 60)

    if not os.path.exists(SETTINGS_DIR):
        print(f"❌ Ошибка: Папка настроек Ватаги не найдена: {SETTINGS_DIR}")
        return

    # 1. Proxies.db
    proxy_id = None
    try:
        conn = sqlite3.connect(PROXIES_DB)
        cur = conn.cursor()
        cur.execute("SELECT ID, Host, Port FROM Proxies WHERE Host='127.0.0.1' AND Port=8989")
        row = cur.fetchone()
        if row:
            proxy_id = row[0]
            cur.execute("UPDATE Proxies SET Title='Vataga Demo Gateway by CGTehnik (127.0.0.1:8989)' WHERE ID=?", (proxy_id,))
            conn.commit()
            print(f"✅ Локальный шлюз уже зарегистрирован в Ватаге (ID: {proxy_id})")
        else:
            now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
            new_id = str(uuid.uuid4()).upper()
            cur.execute("""
                INSERT INTO Proxies (ID, CreationDate, Host, Login, PasswordEncrypted, Port, Title, VisualOrder)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (new_id, now_iso, "127.0.0.1", "", "", 8989, "Vataga Demo Gateway by CGTehnik (127.0.0.1:8989)", 1))
            conn.commit()
            proxy_id = new_id
            print(f"✅ Прокси-шлюз успешно добавлен в proxies.db (ID: {proxy_id})")
        conn.close()
    except Exception as e:
        print(f"❌ Ошибка при настройке proxies.db: {e}")
        return

    # 2. Exchanges.db
    try:
        conn = sqlite3.connect(EXCHANGES_DB)
        cur = conn.cursor()

        # Clean up legacy demo titles if any
        cur.execute("DELETE FROM BybitUserConnections WHERE Title LIKE '%ZENICA DEMO%'")
        cur.execute("DELETE FROM BinanceUserConnections WHERE Title LIKE '%ZENICA DEMO%'")
        conn.commit()

        # Bybit Demo Connection
        cur.execute("SELECT ID FROM BybitUserConnections WHERE Title LIKE '%by CGTehnik%'")
        if cur.fetchone():
            print("✅ Подключение 'Bybit Linear [by CGTehnik]' уже существует в Ватаге")
        else:
            bybit_id = str(uuid.uuid4())
            cur.execute("""
                INSERT INTO BybitUserConnections (
                    ID, Title, AllowMarginModeChange, AutoConnect, Color, Credential,
                    Discriminator, IsFavorite, IsValidated, MarginMode, ProxyID,
                    Type, VisualOrder, IsKeyBelongsToAuthUser, ConnectionType, IsReferralUser, Network
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                bybit_id, "Bybit Linear [by CGTehnik]", 1, 1, "#00E676",
                '{"Key":"cgt_demo_key","Secret":"cgt_demo_secret"}',
                "BybitUserConnection", 1, 1, 1, proxy_id,
                1, 1, 1, 1, 0, 0
            ))
            conn.commit()
            print("✅ Создано подключение: Bybit Linear [by CGTehnik]")

        # Binance Demo Connection
        cur.execute("SELECT ID FROM BinanceUserConnections WHERE Title LIKE '%by CGTehnik%'")
        if cur.fetchone():
            print("✅ Подключение 'Binance Futures [by CGTehnik]' уже существует в Ватаге")
        else:
            binance_id = str(uuid.uuid4())
            cur.execute("""
                INSERT INTO BinanceUserConnections (
                    ID, Title, AllowMarginModeChange, AutoConnect, Color, Credential,
                    Discriminator, IsFavorite, IsValidated, MarginMode, ProxyID,
                    Type, VisualOrder, IsKeyBelongsToAuthUser, ConnectionType, IsReferralUser, Network
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                binance_id, "Binance Futures [by CGTehnik]", 1, 1, "#FFD600",
                '{"Key":"cgt_demo_key","Secret":"cgt_demo_secret"}',
                "BinanceUserConnection", 1, 1, 1, proxy_id,
                1, 2, 1, 1, 0, 0
            ))
            conn.commit()
            print("✅ Создано подключение: Binance Futures [by CGTehnik]")

        conn.close()
    except Exception as e:
        print(f"❌ Ошибка при настройке exchanges.db: {e}")
        return

    print("=" * 60)
    print("🎉 ДЕМО-РЕЖИМ УСПЕШНО ИНТЕГРИРОВАН В ТЕРМИНАЛ ВАТАГА!")
    print("👉 Теперь запусти шлюз: python gateway.py (или start_gateway.bat)")
    print("=" * 60)

if __name__ == "__main__":
    install()
