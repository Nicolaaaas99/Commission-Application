"""Wrapper for posting AR commission batches directly into Sage Evolution
via the Pastel.Evolution SDK. Builds a single CustomerBatch per call so each
Evolution audit prefix groups all lines of one batch with sequential .NNNN
suffixes, and batch.Process() commits atomically (all rows or none)."""
import logging
import threading
from connection import reconnect

logger = logging.getLogger(__name__)

# Serialises SDK posts. reconnect() closes the SDK's SqlConnection before
# reopening it; if a second post tried to run mid-reconnect it would see a
# closed connection.
_post_lock = threading.Lock()


def _parse_trans_date(value):
    """Convert a TransDate (dd/MM/yyyy string or date) into a .NET DateTime."""
    if value is None or value == '':
        return None
    from System import DateTime
    if hasattr(value, 'year') and hasattr(value, 'month') and hasattr(value, 'day'):
        return DateTime(value.year, value.month, value.day)
    parts = str(value).strip().split('/')
    if len(parts) != 3:
        raise ValueError(f"Unrecognised TransDate format: {value!r}")
    day, month, year = int(parts[0]), int(parts[1]), int(parts[2])
    return DateTime(year, month, day)


def post_ar_batch(rows, batch_number=None, batch_description=None):
    """Post a batch of AR transaction rows to Evolution as a single CustomerBatch.

    Each row is a dict from one of the batch SPs and must contain:
      TransDate, Account, TransCode (IN/CN), Reference, Description,
      OrderNumber, AmountExcl, TaxType, AmountIncl, ProjectCode, RepCode,
      ContraAccount.

    Atomic: every line posts together or none do. Returns the audit string
    (e.g. '24585') — each line in Evolution will carry that prefix with a
    sequential .NNNN suffix.
    """
    if not rows:
        return {"success": False, "posted": 0, "audit": None, "error": "No rows to post."}

    with _post_lock:
        # Fresh SDK connection so this batch starts a new audit collection.
        reconnect()
        from Pastel.Evolution import (
            CustomerBatch, BatchDetail, BatchModule, Customer, GLAccount,
            SalesRepresentative, Project, TaxRate, TransactionCode, Module,
        )

        if not batch_number:
            import time
            batch_number = f"COMM{int(time.time())}"

        try:
            batch = CustomerBatch()
            batch.BatchNo = str(batch_number)
            if batch_description:
                batch.Description = str(batch_description)
            batch.DefaultModule = BatchModule.AccountsReceivable
            batch.AllowDuplicateReferences = True

            for idx, row in enumerate(rows, start=1):
                d = BatchDetail()
                d.LineModule = BatchModule.AccountsReceivable

                trans_code = str(row['TransCode'])
                d.TransactionCode = TransactionCode(Module.AR, trans_code)
                d.IsDebit = (trans_code == 'IN')

                d.Account = Customer(str(row['Account']))
                d.Reference = str(row.get('Reference') or '')
                d.Description = str(row.get('Description') or '')

                order_num = row.get('OrderNumber')
                if order_num not in (None, ''):
                    d.OrderNumber = str(order_num)

                rep = row.get('RepCode')
                if rep not in (None, ''):
                    d.Representative = SalesRepresentative(str(rep))

                project_code = row.get('ProjectCode')
                if project_code not in (None, ''):
                    d.Project = Project(str(project_code))

                trans_date = _parse_trans_date(row.get('TransDate'))
                if trans_date is not None:
                    d.Date = trans_date

                contra = row.get('ContraAccount')
                if contra not in (None, ''):
                    d.GLContraAccount = GLAccount(str(contra))

                # DB Incl is now Evolution-quantizable thanks to the SP fix,
                # so Evolution's internal Excl/Tax recompute lands back on
                # our exact Incl. Just pass TaxType + Inclusive.
                d.TaxType = TaxRate(str(row['TaxType']))
                d.AmountInclusive = float(row['AmountIncl'])

                try:
                    batch.Detail.Add(d)
                except Exception as e:
                    raise RuntimeError(
                        f"Validation failed on row {idx}/{len(rows)} "
                        f"(Account={row.get('Account')}, Ref={row.get('Reference')}): {e}"
                    )

            audit = batch.Process()
            return {
                "success": True,
                "posted": len(rows),
                "audit": str(audit) if audit else None,
                "error": None,
            }

        except Exception as e:
            logger.error("Failed posting AR batch: %s", e)
            return {
                "success": False,
                "posted": 0,
                "audit": None,
                "error": str(e),
            }
