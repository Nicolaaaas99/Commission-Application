"""Dry-run: pulls one real row from sp_CreateBatchCSV via a SQL transaction
that we always rollback, then builds (but doesn't Post) the SDK object.
Verifies all property assignments succeed without touching real data."""
import os
import pyodbc
from dotenv import load_dotenv
load_dotenv()

from connection import get_sdk

DB_SERVER = os.environ.get('DB_SERVER') or os.environ['EVO_SERVER']
DB_NAME = os.environ['DB_NAME']
DB_USER = os.environ.get('DB_USER') or os.environ['EVO_USERNAME']
DB_PASSWORD = os.environ.get('DB_PASSWORD') or os.environ['EVO_PASSWORD']

conn = pyodbc.connect(
    f'DRIVER={{SQL SERVER}};SERVER={DB_SERVER};DATABASE={DB_NAME};UID={DB_USER};PWD={DB_PASSWORD}'
)
cursor = conn.cursor()

# Reproduce sp_CreateBatchCSV's SELECT shape from a real split row — without
# running the SP (so no rows get marked exported). Pick a recent row with non-zero
# commission so all fields are meaningful.
cursor.execute("""
    SELECT TOP 1
        CONVERT(varchar(10), SPLIT.HistSplitStmntDate, 103) AS TransDate,
        MAS.PolicyInsurerCode AS Account,
        'AR' AS Module,
        CASE WHEN SPLIT.HistSplitCommIncl > 0 THEN 'IN' ELSE 'CN' END AS TransCode,
        SPLIT.HistSplitStmntReference AS Reference,
        MAS.PolicyHolder AS Description,
        MAS.PolicyNumber AS OrderNumber,
        ABS(SPLIT.HistSplitCommExcl) AS AmountExcl,
        CASE WHEN MAS.PolicyInsuranceType = 'KOSTE' THEN '03' ELSE '01' END AS TaxType,
        ABS(SPLIT.HistSplitCommIncl) AS AmountIncl,
        MAS.PolicyInsuranceType AS ProjectCode,
        BROK.ComBrokerCode AS RepCode,
        CASE WHEN MAS.PolicyInsuranceType = 'KOSTE' THEN '4210>000-00.HOF'
             ELSE '1000>000-' + SPLIT.HistSplitBranch END AS ContraAccount
    FROM CommStmntHistSplit SPLIT
    JOIN CommPolicyMaster MAS ON MAS.PolicyLink = SPLIT.HistSplitPolicyLink
    JOIN _uvCommBroker BROK ON BROK.ComBrokerEvoRepId = SPLIT.HistSplitBrokerLink
    WHERE SPLIT.HistSplitCommIncl <> 0
    ORDER BY SPLIT.HistSplitStmntDate DESC
""")
columns = [c[0] for c in cursor.description]
fetched = cursor.fetchone()
conn.close()

if not fetched:
    print("No CommStmntHistSplit rows with non-zero commission found.")
    raise SystemExit(0)

rows = [dict(zip(columns, fetched))]
print("Synthesised one batch row (read-only, no SP, no DB writes):")
for k, v in rows[0].items():
    print(f"  {k}: {v}")

if not rows:
    raise SystemExit(0)

print("\n--- Building SDK CustomerTransaction (no Post) ---")
get_sdk()
from Pastel.Evolution import (
    CustomerTransaction, Customer, GLAccount, SalesRepresentative,
    Project, TaxRate, TransactionCode, Module,
)
from System import DateTime

r = rows[0]
ct = CustomerTransaction()
ct.Account = Customer(str(r['Account']))
ct.Amount = float(r['AmountIncl'])
ct.Tax = round(float(r['AmountIncl']) - float(r['AmountExcl']), 2)
ct.TransactionCode = TransactionCode(Module.AR, str(r['TransCode']))
ct.TaxRate = TaxRate(str(r['TaxType']))
ct.Reference = str(r.get('Reference') or '')
ct.Description = str(r.get('Description') or '')
if r.get('OrderNumber'):
    ct.OrderNo = str(r['OrderNumber'])
if r.get('ContraAccount'):
    contra = GLAccount(str(r['ContraAccount']))
    if str(r['TransCode']) == 'IN':
        ct.OverrideCreditAccount = contra
    else:
        ct.OverrideDebitAccount = contra
if r.get('RepCode'):
    ct.SalesRep = SalesRepresentative(str(r['RepCode']))
if r.get('ProjectCode'):
    ct.Project = Project(str(r['ProjectCode']))
d = str(r['TransDate']).split('/')
ct.Date = DateTime(int(d[2]), int(d[1]), int(d[0]))

print("All properties assigned successfully.")
print(f"  Account:    {ct.Account.Code if hasattr(ct.Account, 'Code') else ct.Account}")
print(f"  Amount:     {ct.Amount}")
print(f"  Tax:        {ct.Tax}")
print(f"  TaxRate:    {ct.TaxRate.Description}")
print(f"  TransCode:  {ct.TransactionCode.Code}")
print(f"  Reference:  {ct.Reference}")
print(f"  Date:       {ct.Date}")
print("\nNot calling Post(). Dry-run complete.")
