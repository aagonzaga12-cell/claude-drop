"""
Strategy Lab — WICK PARITY TEST (experimento decisivo de sesgo de mechas)
==========================================================================
Pregunta que responde: ¿cuánto del score/expectancy de las candidatas es un
artefacto de que el backtest evalúa SL/TP solo con el CLOSE (sin high/low)?

Contexto verificado en código:
  - simulator.run_backtest llama a vbt.Portfolio.from_signals(close=...) sin
    high/low. En vectorbt 0.26.2 (portfolio/nb.py L2062-2068), si faltan,
    _high=_low=close → las mechas intrabar NO existen para SL/TP.
  - El paper engine SÍ usa high/low (incluso velas 1m) → divergencia total.

Método:
  Para cada ESPECIE única de genoma en SIMULATING/PROBATION/PRODUCTION
  (dedupe: ~130 clones RSI-Oversold cuentan una vez), re-backtestea sobre
  las velas reales de la DB con TU simulator (mismos indicadores, señales,
  fees y slippage), dos veces:
    A) close-only  — como está hoy (control)
    B) OHLC        — con open/high/low reales (mundo con mechas)
  Δ = B − A es el sesgo de mechas, cuantificado por especie.

CRITERIO PRE-REGISTRADO (fijado antes de ver resultados):
  Sea S = especies con total_return > 0 en (A).
  supervivencia = fracción de S que mantiene total_return > 0 en (B).
  - supervivencia < 0.5  → CONFIRMADO: el pipeline selecciona artefactos de
    mechas. Fix: pasar OHLC a from_signals + re-validar todo el vivero
    (y auditar Lab Madre, que comparte paridad).
  - supervivencia ≥ 0.5 → el sesgo existe pero no es el asesino principal;
    siguientes sospechosos: timeout no modelado y señales rancias.

USO (shell del Worker en Railway):
    python /tmp/wpt.py            # tabla + veredicto
Genera: /tmp/wick_parity_report.json
"""
import asyncio
import json
import hashlib
import traceback

import numpy as np
import pandas as pd
import vectorbt as vbt
from sqlalchemy import text

from app.core.config import settings
from app.core.database import AsyncSessionLocal
from app.services.simulator import (
    calculate_indicators,
    generate_signals,
    _extract_portfolio_stats,
    TF_TO_FREQ,
)

MIN_BARS = 300          # mismo umbral que run_backtest
ESTADOS  = ("SIMULATING", "PROBATION", "PRODUCTION")


# ──────────────────────────────────────────────────────────────────────────────
# Núcleo: un backtest con el simulador real, con o sin mechas
# ──────────────────────────────────────────────────────────────────────────────
def run_variant(df: pd.DataFrame, genome: dict, with_wicks: bool):
    """Backtest idéntico a simulator.run_backtest, salvo el paso de OHLC."""
    df_ind = calculate_indicators(df, genome)
    entries, exits, sl_fr, tp_fr = generate_signals(df_ind, genome)
    if entries.sum() == 0:
        return None

    kwargs = dict(
        close=df_ind["close"], entries=entries, exits=exits,
        sl_stop=sl_fr, tp_stop=tp_fr,
        fees=settings.BACKTEST_FEES, slippage=settings.BACKTEST_SLIPPAGE,
        freq=TF_TO_FREQ.get(genome["timeframe"], "15min"),
        init_cash=settings.BACKTEST_INIT_CASH,
    )
    if genome["direction"] != "long":
        kwargs["direction"] = "shortonly"
    if with_wicks:
        kwargs["open"] = df_ind["open"]
        kwargs["high"] = df_ind["high"]
        kwargs["low"]  = df_ind["low"]

    pf = vbt.Portfolio.from_signals(**kwargs)
    st = _extract_portfolio_stats(pf, timeframe=genome["timeframe"])
    st.pop("_trades_df", None)
    st.pop("pnl_list", None)
    return st


# ──────────────────────────────────────────────────────────────────────────────
# Dedupe por especie (condiciones+filtros+exit+dir+tf+symbol; ignora trailing,
# que es gen muerto en ambos motores)
# ──────────────────────────────────────────────────────────────────────────────
def species_key(genome: dict, symbol: str) -> str:
    core = {
        "entry":     genome.get("entry"),
        "filters":   genome.get("filters"),
        "exit":      {k: v for k, v in (genome.get("exit") or {}).items()
                      if k != "trailing"},
        "direction": genome.get("direction"),
        "timeframe": genome.get("timeframe"),
        "symbol":    symbol,
    }
    return hashlib.md5(json.dumps(core, sort_keys=True).encode()).hexdigest()[:10]


def norm_symbol(s: str) -> str:
    return (s or "").upper().replace(":USDT", "").replace("/", "").replace("-", "")


# ──────────────────────────────────────────────────────────────────────────────
# Carga de velas desde la DB del propio sistema
# ──────────────────────────────────────────────────────────────────────────────
async def load_candles(session, symbol: str, timeframe: str) -> pd.DataFrame:
    rows = (await session.execute(text(
        "SELECT timestamp, open, high, low, close, volume FROM candles "
        "WHERE symbol = :s AND timeframe = :tf ORDER BY timestamp"
    ), {"s": symbol, "tf": timeframe})).all()
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close", "volume"])
    for c in ("open", "high", "low", "close", "volume"):
        df[c] = df[c].astype(float)
    df["datetime"] = pd.to_datetime(df["timestamp"].astype("int64"), unit="ms", utc=True)
    return df.set_index("datetime").drop(columns=["timestamp"])


async def main():
    async with AsyncSessionLocal() as session:
        # 1) Símbolos disponibles en candles (para resolver formato genome↔DB)
        simbolos_db = (await session.execute(
            text("SELECT DISTINCT symbol FROM candles")
        )).scalars().all()
        mapa_sym = {norm_symbol(s): s for s in simbolos_db}
        print(f"Símbolos en candles: {simbolos_db}")

        # 2) Estrategias activas → especies únicas
        rows = (await session.execute(text(
            "SELECT name, symbol, genome, status FROM strategies "
            f"WHERE status IN {ESTADOS}"
        ))).all()
        print(f"Estrategias activas: {len(rows)}")

        especies: dict[str, dict] = {}
        for name, symbol, genome, status in rows:
            if isinstance(genome, str):
                genome = json.loads(genome)
            sym = symbol or genome.get("symbol") or ""
            key = species_key(genome, norm_symbol(sym))
            if key not in especies:
                especies[key] = {"name": name, "genome": genome,
                                 "symbol": sym, "clones": 0}
            especies[key]["clones"] += 1
        print(f"Especies únicas (dedupe, trailing ignorado): {len(especies)}\n")

        # 3) Cache de velas por (symbol_db, timeframe)
        cache: dict[tuple, pd.DataFrame] = {}
        resultados = []

        for key, esp in especies.items():
            g   = esp["genome"]
            tf  = g.get("timeframe", "15m")
            sdb = mapa_sym.get(norm_symbol(esp["symbol"]))
            if sdb is None:
                print(f"  SKIP {esp['name'][:45]} — símbolo '{esp['symbol']}' sin velas en DB")
                continue
            ck = (sdb, tf)
            if ck not in cache:
                cache[ck] = await load_candles(session, sdb, tf)
                d = cache[ck]
                rango = f"{d.index[0]} → {d.index[-1]}" if len(d) else "vacío"
                print(f"  [velas] {sdb} {tf}: {len(d)} barras ({rango})")
            df = cache[ck]
            if len(df) < MIN_BARS:
                print(f"  SKIP {esp['name'][:45]} — solo {len(df)} barras (<{MIN_BARS})")
                continue

            try:
                a = run_variant(df, g, with_wicks=False)
                b = run_variant(df, g, with_wicks=True)
            except Exception as e:
                print(f"  ERROR {esp['name'][:45]} — {e}")
                traceback.print_exc()
                continue
            if a is None or b is None:
                print(f"  SKIP {esp['name'][:45]} — sin señales en histórico DB")
                continue

            resultados.append({
                "key": key, "name": esp["name"], "clones": esp["clones"],
                "tf": tf, "dir": g["direction"],
                "A_trades": a["total_trades"], "B_trades": b["total_trades"],
                "A_ret": round(a["total_return"] * 100, 2),
                "B_ret": round(b["total_return"] * 100, 2),
                "A_wr": round(a.get("win_rate", 0) * 100, 1),
                "B_wr": round(b.get("win_rate", 0) * 100, 1),
                "A_pf": round(a.get("profit_factor", 0), 2),
                "B_pf": round(b.get("profit_factor", 0), 2),
            })

    # 4) Tabla
    print("\n" + "═" * 118)
    print("  WICK PARITY — A=close-only (actual) vs B=OHLC (mechas reales) — mismas velas, mismo simulador, mismos costes")
    print("═" * 118)
    print(f"  {'especie':<44} {'clones':>6} {'tf':>4} {'dir':>5} | "
          f"{'A n':>4} {'A ret%':>8} {'A wr':>5} {'A pf':>5} | "
          f"{'B n':>4} {'B ret%':>8} {'B wr':>5} {'B pf':>5} | {'Δret%':>8}")
    print("  " + "-" * 114)
    for r in sorted(resultados, key=lambda x: x["A_ret"], reverse=True):
        print(f"  {r['name'][:44]:<44} {r['clones']:>6} {r['tf']:>4} {r['dir']:>5} | "
              f"{r['A_trades']:>4} {r['A_ret']:>8} {r['A_wr']:>5} {r['A_pf']:>5} | "
              f"{r['B_trades']:>4} {r['B_ret']:>8} {r['B_wr']:>5} {r['B_pf']:>5} | "
              f"{r['B_ret'] - r['A_ret']:>8.2f}")

    # 5) Veredicto pre-registrado
    print("═" * 118)
    positivas_a = [r for r in resultados if r["A_ret"] > 0]
    if positivas_a:
        sobreviven = [r for r in positivas_a if r["B_ret"] > 0]
        surv = len(sobreviven) / len(positivas_a)
        delta_med = float(np.median([r["B_ret"] - r["A_ret"] for r in resultados]))
        print(f"  Especies positivas en A: {len(positivas_a)} | "
              f"siguen positivas en B: {len(sobreviven)} | supervivencia = {surv:.0%}")
        print(f"  Δret mediano (B−A) sobre todas las especies: {delta_med:+.2f}%")
        if surv < 0.5:
            print("\n  ▶ ❌ CONFIRMADO: el pipeline selecciona artefactos de mechas.")
            print("     Fix requerido: pasar open/high/low a from_signals en simulator.run_backtest,")
            print("     re-validar TODO el vivero, y auditar el motor de Lab Madre (paridad = mismo sesgo).")
        else:
            print("\n  ▶ ⚠️  El sesgo de mechas existe (ver Δret) pero NO explica solo las pérdidas.")
            print("     Siguientes sospechosos a testear: timeout no modelado en backtest y señales")
            print("     rancias en la entrada (lookback + entrada al último close).")
    else:
        print("  ▶ Ninguna especie es positiva ni siquiera en A (close-only) sobre las velas de la DB.")
        print("    Las candidatas ya no baten ni a su propio simulador sesgado en datos recientes.")
    print("═" * 118)

    with open("/tmp/wick_parity_report.json", "w") as f:
        json.dump(resultados, f, indent=2)
    print("\nGuardado: /tmp/wick_parity_report.json")


if __name__ == "__main__":
    asyncio.run(main())
