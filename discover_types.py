"""Identify which rows of statement 1202 would drift under Evolution's
tax recompute. Total drift should equal the 9c we see in Evolution."""
import os
import pyodbc
from dotenv import load_dotenv
load_dotenv()

STMNT_LINK = 1203  # change if needed

DB_SERVER = os.environ.get('DB_SERVER') or os.environ['EVO_SERVER']
DB_NAME = os.environ['DB_NAME']
DB_USER = os.environ.get('DB_USER') or os.environ['EVO_USERNAME']
DB_PASSWORD = os.environ.get('DB_PASSWORD') or os.environ['EVO_PASSWORD']

conn = pyodbc.connect(
    f'DRIVER={{SQL SERVER}};SERVER={DB_SERVER};DATABASE={DB_NAME};UID={DB_USER};PWD={DB_PASSWORD}'
)
cur = conn.cursor()

# Reproduce sp_CreateBatchCSV's SELECT for this statement (read-only).
cur.execute("""
    SELECT
        SPLIT.HistSplitStmntReference AS Reference,
        MAS.PolicyHolder AS Description,
        ABS(SPLIT.HistSplitCommExcl) AS AmountExcl,
        ABS(SPLIT.HistSplitCommIncl) AmountIncl,
        CASE WHEN MAS.PolicyInsuranceType = 'KOSTE' THEN '03' ELSE '01' END AS TaxType
    FROM CommStmntHistSplit SPLIT
    JOIN CommPolicyMaster MAS ON MAS.PolicyLink = SPLIT.HistSplitPolicyLink
    WHERE SPLIT.HistSplitStmntId = ?
""", STMNT_LINK)
rows = [dict(zip([c[0] for c in cur.description], r)) for r in cur.fetchall()]
conn.close()

print(f"Statement {STMNT_LINK}: {len(rows)} row(s)\n")

rate_map = {'01': 0.15, '03': 0.0}
db_total = 0.0
evo_total = 0.0
problem_rows = []

for r in rows:
    db_incl = float(r['AmountIncl'])
    db_excl = float(r['AmountExcl'])
    rate = rate_map.get(r['TaxType'], 0.0)

    # Evolution's path: derive Excl from Incl, then Tax from that Excl, then Incl from Excl+Tax
    if rate > 0:
        evo_excl = round(db_incl / (1 + rate), 2)
        evo_tax  = round(evo_excl * rate, 2)
    else:
        evo_excl = db_incl
        evo_tax  = 0.0
    evo_incl = round(evo_excl + evo_tax, 2)

    db_total  += db_incl
    evo_total += evo_incl

    if abs(evo_incl - db_incl) > 0.001:
        problem_rows.append((r, db_incl, evo_incl, evo_incl - db_incl))

print(f"DB total:        R {db_total:,.2f}")
print(f"Evolution total: R {evo_total:,.2f}")
print(f"Drift:           R {evo_total - db_total:+,.2f}")
print(f"\n{len(problem_rows)} row(s) drift:\n")
for r, di, ei, d in problem_rows[:30]:
    print(f"  Ref={r['Reference']!r:<20} Excl={r['AmountExcl']:>10}  DB_Incl={di:>10.2f}  Evo_Incl={ei:>10.2f}  Diff={d:+.2f}")
if len(problem_rows) > 30:
    print(f"  ... and {len(problem_rows) - 30} more")
