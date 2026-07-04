"""
Lab Madre — Comparacion distribucional de scores PRE-FIX vs POST-FIX.
Ejecutar con >=24h de datos post-fix. Usa LM_DB_PATH y su .bak hermano.

USO:  python lm_score_compare.py
"""
import os
import sqlite3

src = os.environ.get("LM_DB_PATH")
bak = src.replace(".db", "_prefix_20260704.bak")
print("servicio:", os.path.basename(src))


def dist(path, tag):
    if not os.path.exists(path):
        print(f"{tag}: NO EXISTE {path}")
        return
    c = sqlite3.connect(path)
    rows = [r[0] for r in c.execute(
        "SELECT score FROM genomes WHERE score IS NOT NULL ORDER BY score")]
    c.close()
    if not rows:
        print(f"{tag}: sin datos")
        return
    n = len(rows)
    p = lambda q: round(rows[min(n - 1, int(n * q))], 2)
    exp = sum(1 for s in rows if s >= 50)
    print(f"{tag}: n={n:>7} p50={p(.5):>7} p90={p(.9):>7} p99={p(.99):>7} "
          f"max={rows[-1]:>7.2f} score>=50: {exp} ({exp / n * 100:.2f}%)")


dist(bak, "PRE-FIX ")
dist(src, "POST-FIX")
