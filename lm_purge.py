"""
Lab Madre — Purga de hall of fame pre-fix (multi-servicio).
Usa LM_DB_PATH del propio servicio y cutoff = arranque del contenedor
(contenedor nuevo = codigo post-fix). Dry-run por defecto.

USO (shell del worker de cada servicio):
    python lm_purge.py            # dry-run
    python lm_purge.py --apply    # backup + DELETE + VACUUM
"""
import os
import shutil
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone

APPLY = "--apply" in sys.argv

fix = subprocess.run(["grep", "-n", "high=df", "/app/validation/backtest.py"],
                     capture_output=True, text=True).stdout.strip()
print("FIX:", fix if fix else "AUSENTE")
if not fix:
    print("ABORT: este servicio no tiene el fix deployado. No purgar.")
    sys.exit(1)

src = os.environ.get("LM_DB_PATH")
print("LM_DB_PATH:", src)
if not src or not os.path.exists(src):
    print("ABORT: LM_DB_PATH no definido o el archivo no existe.")
    sys.exit(1)

boot = datetime.fromtimestamp(os.stat("/proc/1").st_ctime, tz=timezone.utc)
cutoff = boot.isoformat()
print("cutoff (arranque contenedor):", cutoff)

c = sqlite3.connect(src)
tablas = [r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")]
if "genomes" not in tablas:
    print(f"ABORT: sin tabla genomes en {src} (tablas: {tablas})")
    sys.exit(1)

pre = c.execute("SELECT COUNT(*) FROM genomes WHERE created_at < ?", (cutoff,)).fetchone()[0]
post = c.execute("SELECT COUNT(*) FROM genomes WHERE created_at >= ?", (cutoff,)).fetchone()[0]
print(f"pre-fix a borrar: {pre} | post-fix a conservar: {post}")

# limpieza de la DB vacia accidental creada por el diagnostico anterior
stray = "/data/lab_madre.db"
if stray != src and os.path.exists(stray) and os.path.getsize(stray) < 100_000:
    sc = sqlite3.connect(stray)
    st = [r[0] for r in sc.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    sc.close()
    if not st:
        print(f"detectada DB vacia accidental: {stray} ({os.path.getsize(stray)} bytes)"
              + (" — eliminada" if APPLY else " — se eliminaria con --apply"))
        if APPLY:
            os.remove(stray)

if not APPLY:
    print("DRY RUN — nada modificado. Aplicar con: python lm_purge.py --apply")
    sys.exit(0)

bak = src.replace(".db", "_prefix_20260704.bak")
if not os.path.exists(bak):
    shutil.copy2(src, bak)
    print(f"backup: {bak} ({os.path.getsize(bak) // 1024 // 1024} MB)")
c.execute("DELETE FROM genomes WHERE created_at < ?", (cutoff,))
c.commit()
c.execute("VACUUM")
print("purgado. quedan:", c.execute("SELECT COUNT(*) FROM genomes").fetchone()[0])
