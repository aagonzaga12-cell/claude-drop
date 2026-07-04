"""
Strategy Lab — Purga de especies catastróficas del wick test.
Lee /tmp/wick_parity_report.json (generado por wpt.py EN ESTE contenedor)
y retira (status=RETIRED) todas las estrategias activas cuya especie
tuvo B_ret < THRESH en el test con mechas.

USO (shell del Worker, tras ejecutar wpt.py):
    python purge_retired.py            # DRY RUN: solo lista
    python purge_retired.py --apply    # aplica RETIRED
"""
import asyncio
import hashlib
import json
import sys

from sqlalchemy import text
from app.core.database import AsyncSessionLocal

THRESH = -20.0
APPLY = "--apply" in sys.argv
ACTIVOS = ("TESTING", "SIMULATING", "PROBATION", "PRODUCTION")


def norm(s: str) -> str:
    return (s or "").upper().replace(":USDT", "").replace("/", "").replace("-", "")


def skey(g: dict, sym: str) -> str:
    core = {
        "entry": g.get("entry"),
        "filters": g.get("filters"),
        "exit": {k: v for k, v in (g.get("exit") or {}).items() if k != "trailing"},
        "direction": g.get("direction"),
        "timeframe": g.get("timeframe"),
        "symbol": sym,
    }
    return hashlib.md5(json.dumps(core, sort_keys=True).encode()).hexdigest()[:10]


async def main():
    rep = json.load(open("/tmp/wick_parity_report.json"))
    bad = {r["key"] for r in rep if r["B_ret"] < THRESH}
    print(f"Especies condenadas (B_ret < {THRESH}%): {len(bad)}")

    async with AsyncSessionLocal() as s:
        rows = (await s.execute(text(
            f"SELECT id, name, symbol, genome, status FROM strategies WHERE status IN {ACTIVOS}"
        ))).all()
        targets = []
        for id_, name, symbol, g, st in rows:
            if isinstance(g, str):
                g = json.loads(g)
            if skey(g, norm(symbol or g.get("symbol") or "")) in bad:
                targets.append((id_, name, st))

        print(f"Estrategias a retirar: {len(targets)} de {len(rows)} activas")
        for id_, name, st in targets:
            print(f"   [{st}] {id_} {name[:58]}")

        if not APPLY:
            print("\nDRY RUN — nada modificado. Para aplicar: python purge_retired.py --apply")
            return

        for id_, _, _ in targets:
            await s.execute(text("UPDATE strategies SET status='RETIRED' WHERE id=:i"), {"i": id_})
        await s.commit()
        print("RETIRED aplicado.")

        abiertos = (await s.execute(text(
            "SELECT t.id, s.name FROM paper_trades t JOIN strategies s ON s.id = t.strategy_id "
            "WHERE t.is_open = true AND s.status = 'RETIRED'"
        ))).all()
        print(f"Trades paper AÚN ABIERTOS de retiradas: {len(abiertos)}")
        for tid, name in abiertos:
            print("   ", tid, name[:50])


if __name__ == "__main__":
    asyncio.run(main())
