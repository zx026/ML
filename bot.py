import asyncio
import yfinance as yf
import pandas_ta as ta
import sqlite3
import schedule
import time
from datetime import datetime
from telegram.ext import Application, CommandHandler

# ========= CONFIG =========
TOKEN = "8330533753:AAG_2Fn5deWSVIx1euC-LshE4JNmSA9Jtgs"
CHAT_ID = -1003635838231
PAIRS = ["EURUSD=X"]
SIGNALS_DB = "ml_binary_signals.db"

# ========= DATABASE =========
conn = sqlite3.connect(SIGNALS_DB, check_same_thread=False)
cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS candles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    time TEXT,
    pair TEXT,
    close REAL,
    rsi2 REAL,
    stoch REAL,
    bb_pos REAL,
    volume REAL,
    volume_sma REAL
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    time TEXT,
    pair TEXT,
    direction TEXT,
    entry_price REAL,
    rsi2 REAL,
    stoch REAL,
    bb_pos REAL,
    volume_ratio REAL,
    result INTEGER,
    pl REAL
)
""")
conn.commit()


def save_candle(pair, t, close, rsi2, stoch, bb_pos, vol, vol_sma):
    cur.execute("""
        INSERT INTO candles
        (time,pair,close,rsi2,stoch,bb_pos,volume,volume_sma)
        VALUES (?,?,?,?,?,?,?,?)
    """, (t, pair, close, rsi2, stoch, bb_pos, vol, vol_sma))
    conn.commit()


def save_trade(pair, direction, entry_price, rsi2, stoch, bb_pos, vol_ratio):
    cur.execute("""
        INSERT INTO trades
        (time,pair,direction,entry_price,rsi2,stoch,bb_pos,volume_ratio,result,pl)
        VALUES (?,?,?,?,?,?,?,?,NULL,NULL)
    """, (
        datetime.utcnow().isoformat(),
        pair,
        direction,
        entry_price,
        rsi2,
        stoch,
        bb_pos,
        vol_ratio
    ))
    conn.commit()


def get_stats():
    total = cur.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
    w = cur.execute("SELECT COUNT(*) FROM trades WHERE result=1").fetchone()[0]
    l = cur.execute("SELECT COUNT(*) FROM trades WHERE result=0").fetchone()[0]
    winrate = (w / total * 100) if total > 0 else 0
    return total, w, l, winrate


# ========= SIGNAL SCAN =========
async def scan_and_signal(app: Application):
    for symbol in PAIRS:
        try:
            data = yf.download(symbol, period="2d", interval="1m", progress=False)
            if len(data) < 50:
                continue

            data["RSI2"] = ta.rsi(data["Close"], 2)
            stoch = ta.stoch(data["High"], data["Low"], data["Close"])
            data["Stoch"] = stoch["STOCHk_14_3_3"]

            bb = ta.bbands(data["Close"], length=20)
            data["BB_lower"] = bb["BBL_20_2.0"]
            data["BB_upper"] = bb["BBU_20_2.0"]

            data["Volume_SMA"] = ta.sma(data["Volume"], 10)

            latest = data.iloc[-1]

            bb_pos = (latest["Close"] - latest["BB_lower"]) / (
                latest["BB_upper"] - latest["BB_lower"] + 1e-9
            )
            vol_ratio = latest["Volume"] / (latest["Volume_SMA"] + 1e-9)

            save_candle(
                symbol,
                latest.name.isoformat(),
                float(latest["Close"]),
                float(latest["RSI2"]),
                float(latest["Stoch"]),
                float(bb_pos),
                float(latest["Volume"]),
                float(latest["Volume_SMA"]),
            )

            direction = None
            text_sig = None

            if (
                latest["RSI2"] < 8
                and latest["Stoch"] < 15
                and latest["Close"] <= latest["BB_lower"] * 1.001
                and vol_ratio > 1.2
            ):
                direction = "CALL"
                text_sig = "🚀 SUPER CALL ✅"

            elif (
                latest["RSI2"] > 92
                and latest["Stoch"] > 85
                and latest["Close"] >= latest["BB_upper"] * 0.999
                and vol_ratio > 1.2
            ):
                direction = "PUT"
                text_sig = "💥 SUPER PUT ❌"

            if direction:
                save_trade(
                    symbol,
                    direction,
                    float(latest["Close"]),
                    float(latest["RSI2"]),
                    float(latest["Stoch"]),
                    float(bb_pos),
                    float(vol_ratio),
                )

                msg = (
                    f"📊 {symbol}\n"
                    f"Price: {latest['Close']:.5f}\n"
                    f"RSI2: {latest['RSI2']:.1f}\n"
                    f"Stoch: {latest['Stoch']:.1f}\n"
                    f"Volume Ratio: {vol_ratio:.2f}\n"
                    f"Signal: {text_sig}\n\n"
                    "Features saved for ML training."
                )

                await app.bot.send_message(chat_id=CHAT_ID, text=msg)

        except Exception as e:
            print("Error:", symbol, e)


# ========= TELEGRAM COMMANDS =========
async def start(update, context):
    total, w, l, winrate = get_stats()
    await update.message.reply_text(
        "🤖 ML-ready Binary Bot ON\n"
        f"Pairs: {', '.join(PAIRS)}\n"
        f"Trades stored: {total} (W:{w} L:{l})\n"
        f"Winrate est: {winrate:.1f}%\n\n"
        "Bot market data + ML features store kar raha hai.\n"
        "/stats se status dekho."
    )


async def stats(update, context):
    total, w, l, winrate = get_stats()
    await update.message.reply_text(
        "📊 STATS\n"
        f"Trades stored: {total}\n"
        f"Wins: {w}\n"
        f"Losses: {l}\n"
        f"Winrate: {winrate:.1f}%\n\n"
        "Note: result column abhi auto-fill nahi hai."
    )


# ========= MAIN =========
async def main():
    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("stats", stats))

    await app.initialize()
    await app.start()
    await app.bot.initialize()

    schedule.every(2).minutes.do(
        lambda: asyncio.create_task(scan_and_signal(app))
    )

    print("🤖 Bot running...")

    while True:
        schedule.run_pending()
        await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
