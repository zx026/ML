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
CHAT_IDS = set()
PAIRS = ["EURUSD=X"]  # ML ke liye pehle 1 pair rakho
SIGNALS_DB = "ml_binary_signals.db"

# ========= DATABASE =========
conn = sqlite3.connect(SIGNALS_DB, check_same_thread=False)
cur = conn.cursor()

# Features + trades table
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
    result INTEGER,        -- 1=win,0=loss,NULL=unknown
    pl REAL                -- profit/loss amount (optional)
)
""")
conn.commit()


def save_candle(pair, t, close, rsi2, stoch, bb_pos, vol, vol_sma):
    cur.execute("""
        INSERT INTO candles (time,pair,close,rsi2,stoch,bb_pos,volume,volume_sma)
        VALUES (?,?,?,?,?,?,?,?)
    """, (t, pair, close, rsi2, stoch, bb_pos, vol, vol_sma))
    conn.commit()


def save_trade(pair, direction, entry_price, rsi2, stoch, bb_pos, vol_ratio):
    cur.execute("""
        INSERT INTO trades (time,pair,direction,entry_price,rsi2,stoch,bb_pos,volume_ratio,result,pl)
        VALUES (?,?,?,?,?,?,?,?,NULL,NULL)
    """, (datetime.utcnow().isoformat(), pair, direction, entry_price,
          rsi2, stoch, bb_pos, vol_ratio))
    conn.commit()


def get_stats():
    total = cur.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
    w = cur.execute("SELECT COUNT(*) FROM trades WHERE result=1").fetchone()[0]
    l = cur.execute("SELECT COUNT(*) FROM trades WHERE result=0").fetchone()[0]
    winrate = (w / total * 100) if total > 0 else 0
    return total, w, l, winrate


# ========= SIGNAL + DATA COLLECTION =========
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

            # ---- feature calculation for ML ----
            bb_pos = (latest["Close"] - latest["BB_lower"]) / (
                latest["BB_upper"] - latest["BB_lower"] + 1e-9
            )  # 0=lower,1=upper
            vol_ratio = latest["Volume"] / (latest["Volume_SMA"] + 1e-9)

            # Save candle for learning
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

            # ---- rule-based signal (as before) ----
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
                # Save trade for future ML training
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
                    f"{symbol}: {latest['Close']:.5f} | RSI2: {latest['RSI2']:.1f} | "
                    f"Stoch: {latest['Stoch']:.1f} | {text_sig}
"
                    f"Features saved for ML training."
                )
                for cid in CHAT_IDS:
                    await app.bot.send_message(chat_id=cid, text=msg)

        except Exception as e:
            print("Error:", symbol, e)


# ========= TELEGRAM =========
async def start(update, context):
    CHAT_IDS.add(update.effective_chat.id)
    total, w, l, winrate = get_stats()
    await update.message.reply_text(
        "🤖 ML-ready Binary Bot ON
"
        f"Pairs: {', '.join(PAIRS)}
"
        f"Trades stored: {total} (W:{w} L:{l}) Winrate est: {winrate:.1f}%
"
        "Bot ab market data + features store karega.
"
        "/stats se status dekh sakte ho."
    )


async def stats(update, context):
    total, w, l, winrate = get_stats()
    await update.message.reply_text(
        f"📊 STATS
Trades stored: {total}
Wins: {w}
Losses: {l}
Winrate: {winrate:.1f}%
"
        "Note: result column abhi manually/auto fill hona baaki hai."
    )


async def main():
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("stats", stats))

    await app.initialize()
    await app.start()
    await app.updater.start_polling()

    # har 2 minute me scan
    schedule.every(2).minutes.do(lambda: asyncio.create_task(scan_and_signal(app)))

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
