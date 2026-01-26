# app.py

import os
import re
from waitress import serve
import pandas as pd
import pyodbc
from flask import Flask, request, render_template, redirect, flash, jsonify, url_for, g
from werkzeug.utils import secure_filename
import logging
import math
import io
import csv
from dotenv import load_dotenv
load_dotenv()

# --- CONFIG ---
UPLOAD_FOLDER = 'Uploads'
ALLOWED_EXTENSIONS = {'csv', 'xlsx'}

DB_SERVER = os.environ.get('DB_SERVER')
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
STAGING_TABLE = 'ZZImportStage'


# --- Stored Procedure Names ---
SELECT_PAYMENTS_SP = 'sp_SelectPaymentsToProcess' 
PROCESS_IMPORT_SP = 'sp_ProcessCommissionImport' # Requires User
CREATE_POLICY_SP = 'sp_CreatePolicyMasterFromInput' # Requires User
GET_ACTION_SP = 'sp_GetPoliciesNeedingAction'
INSERT_SPLIT_SP = 'sp_InsertPolicySplit'
UPDATE_INSURANCE_TYPE_SP = 'sp_UpdatePolicyInsuranceType' # Requires User
INSERT_HIST_SP = 'sp_InsertIntoHistoryFromInput' # Requires User
INSERT_HISTSPLIT_SP = 'sp_InsertCommStmntHistSplit'# Requires User
INSERT_EVOCOMM_SP = 'sp_RetrieveEvoCommissionSplits'
SEARCH_POLICIES_SP = 'sp_SearchPoliciesAdvanced'
GET_SPLITS_SP = 'sp_GetPolicySplits'
UPDATE_COMPLIANCE_SP = 'sp_UpdatePolicyCompliance'
COMMISSION_RUN_SP = 'sp_ExecuteCommissionRun'
CANCEL_RUN_SP = 'sp_CancelCommissionRun'
GET_BROKER_REPORT_SP = 'sp_GetBrokerCommissionReport'
GET_COMMISSION_PERIODS_SP = 'sp_GetCommissionPeriods'
CREATE_BATCH_CSV_SP = 'sp_CreateBatchCSV'
GET_BROKER_REPORT_PERIODS_SP = 'sp_GetBrokerReportPeriods'
GET_SPLIT_HISTORY_SP = 'sp_GetPolicySplitHistoryReport'
VOID_BATCH_CSV_SP = 'sp_VoidBatchCSV'
SYNC_TO_EVO_SP = 'sp_SyncCommissionToEvolution'
UNSYNC_FROM_EVO_SP = 'sp_UnsyncCommissionFromEvolution'
GET_LAPSED_POLICIES_SP = 'sp_GetLapsedPolicies'
CREATE_DEFERRAL_SP = 'sp_CreateDeferralBatchCSV'
GET_UI_HISTORY_SP = 'sp_GetPolicyHistoryForUI'
SPLIT_CORRECTION_SP = 'sp_SplitCorrection'
CREATE_CORRECTION_CSV_SP = 'sp_CreateCorrectionBatchCSV'
REVERSE_UPLOAD_SP = 'sp_ReverseStatementUpload'
SEARCH_STATEMENTS_SP = 'sp_SearchStatementsAdvanced'



# Expected columns for the new upload format
EXPECTED_COLUMNS = [
    'Plan_number', 'Planholder_surname', 'Planholder_initials',
    'Commission_Fees_Incl', 'VAT_Amount'
]

app = Flask(__name__)
app.secret_key = 'secret-key-123'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
logging.basicConfig(level=logging.INFO)

# --- Helpers ---
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def clean_fields(df):
    def clean_cell(cell):
        if isinstance(cell, str):
            match = re.match(r'^="([^"]*)"$', cell)
            if match:
                return match.group(1).strip()
            return cell.strip()
        return cell
    return df.apply(lambda x: x.map(clean_cell) if x.dtype == 'object' else x)

def get_db_connection():
    """Establishes a connection to the SQL Server database."""
    if 'db_conn' not in g:
        conn_str = (
            f'DRIVER={{SQL SERVER}};'
            f'SERVER={DB_SERVER};DATABASE={DB_NAME};UID={DB_USER};PWD={DB_PASSWORD}'
        )
        g.db_conn = pyodbc.connect(conn_str)
    return g.db_conn

@app.teardown_appcontext
def close_db(exception):
    db_conn = g.pop('db_conn', None)
    if db_conn is not None:
        db_conn.close()

def run_sp(sp_name, *params):
    """A generic helper to execute a stored procedure without fetching results."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        sql_exec = f"EXEC {sp_name} {', '.join(['?'] * len(params))}"
        cursor.execute(sql_exec, *params)
        conn.commit()
    except pyodbc.Error as e:
        logging.error(f"Error executing {sp_name}: {e}")
        conn.rollback()
        raise Exception(str(e))


def run_sp_with_results(sp_name, *params):
    """A helper to execute a stored procedure and fetch the first result set."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        sql_exec = f"EXEC {sp_name} {', '.join(['?'] * len(params))}"
        cursor.execute(sql_exec, *params)
        columns = [column[0] for column in cursor.description]
        result = cursor.fetchone()
        conn.commit()
        return dict(zip(columns, result)) if result else None
    except pyodbc.Error as e:
        logging.error(f"Error executing {sp_name} with results: {e}")
        conn.rollback()
        raise Exception(f"Database error in {sp_name}: {e}")


def insert_to_staging(df):
    """Inserts data from a DataFrame into the ZZImportStage table with inclusive amount and calculated VAT."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(f"TRUNCATE TABLE {STAGING_TABLE}")
        insert_sql = f"""
            INSERT INTO {STAGING_TABLE} (
                Plan_number, Planholder_surname, Planholder_initials,
                Commission_Fees_Incl, VAT_Amount
            ) VALUES (?, ?, ?, ?, ?)
        """
        data_to_insert = []
        for _, row in df.iterrows():
            # Get the inclusive amount from user's upload
            commission_incl = float(row['Commission_Fees_Incl']) if pd.notna(row['Commission_Fees_Incl']) else 0.0
            
            # Calculate VAT from inclusive amount (ignore the uploaded VAT_Amount)
            vat_amount = (commission_incl / 1.15) * 0.15
            
            data_to_insert.append((
                str(row['Plan_number']) if pd.notna(row['Plan_number']) else None,
                str(row['Planholder_surname']) if pd.notna(row['Planholder_surname']) else None,
                str(row['Planholder_initials']) if pd.notna(row['Planholder_initials']) else None,
                commission_incl,  # Keep the original inclusive amount
                vat_amount        # Store the calculated VAT amount
            ))
        
        cursor.executemany(insert_sql, data_to_insert)
        conn.commit()
        logging.info(f"Data inserted into {STAGING_TABLE} successfully")
    except pyodbc.Error as e:
        logging.error(f"Database error during staging insert: {e}")
        raise Exception(f"Database error during insert: {e}")

# --- Main Routes ---
@app.route('/')
def index():
    """Main page: shows upload form and the list of policies needing action."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(f"EXEC {GET_ACTION_SP}")
        columns = [column[0] for column in cursor.description]
        policies = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        actions_pending = len(policies) > 0
        
        return render_template('index.html', 
                               policies_needing_action=policies, 
                               actions_pending=actions_pending)
    except Exception as e:
        flash(f"Error loading page: {str(e)}", 'error')
        return render_template('index.html', policies_needing_action=[], actions_pending=False)

@app.route('/reports')
def reports():
    """Renders the consolidated reports page."""
    return render_template('reports.html')

@app.route('/upload', methods=['POST'])
def upload_file_route():
    """Handles file upload, validation, and processing pipeline."""
    file = request.files.get('file')
    company_id = request.form.get('company_id')
    insurer_link = request.form.get('insurer_link')
    payment_ids = request.form.getlist('payment_ids')

    if not all([file, company_id, insurer_link, payment_ids]) or not allowed_file(file.filename):
        flash('A Company, Insurer, at least one Payment, and a valid CSV/XLSX file are required.', 'error')
        return redirect(url_for('index'))

    filepath = os.path.join(app.config['UPLOAD_FOLDER'], secure_filename(file.filename))

    try:
        file.save(filepath)
        file_ext = file.filename.rsplit('.', 1)[1].lower()
        df = pd.read_csv(filepath, dtype=str, keep_default_na=False) if file_ext == 'csv' else pd.read_excel(filepath, dtype=str, keep_default_na=False)
        df = clean_fields(df)

        if not all(col in df.columns for col in EXPECTED_COLUMNS):
            missing_cols = [col for col in EXPECTED_COLUMNS if col not in df.columns]
            raise ValueError(f"File is missing required columns: {', '.join(missing_cols)}")

        df['Commission_Fees_Incl'] = pd.to_numeric(df['Commission_Fees_Incl'], errors='coerce').fillna(0)
        df['VAT_Amount'] = pd.to_numeric(df['VAT_Amount'], errors='coerce').fillna(0)
        uploaded_total = (df['Commission_Fees_Incl']).sum()
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        placeholders = ','.join(['?'] * len(payment_ids))
        sql_query = f"SELECT CommPmtAmount FROM dbo._uvCommPayment WHERE CommPmtAutoIdx IN ({placeholders})"
        
        cursor.execute(sql_query, [int(pid) for pid in payment_ids])
        payment_records = cursor.fetchall()

        if len(payment_records) != len(payment_ids):
            raise ValueError("One or more of the selected payments could not be found.")

        expected_total = sum(record.CommPmtAmount for record in payment_records)
        
        if not math.isclose(uploaded_total, expected_total, rel_tol=1e-5):
            raise ValueError(f"Amount mismatch! File total: R{uploaded_total:,.2f}, Selected payments total: R{expected_total:,.2f}.")

        payment_ids_str = ','.join(payment_ids)
        new_statement = run_sp_with_results(SELECT_PAYMENTS_SP, int(company_id), int(insurer_link), payment_ids_str, 0, 'System')
        
        if not new_statement:
            raise Exception("Failed to create a master statement record.")

        insert_to_staging(df)
        run_sp(PROCESS_IMPORT_SP, int(company_id), int(insurer_link), new_statement['StmntLink'], new_statement['StmntDate'], new_statement['StmntReference'])
        
        run_sp(CREATE_POLICY_SP)
        run_sp(INSERT_HIST_SP)
        run_sp(INSERT_HISTSPLIT_SP)
        
        flash(f"File processed successfully against new statement {new_statement['StmntReference']}! Totals matched.", 'success')

    except (ValueError, pd.errors.ParserError) as e:
        flash(f"Error processing file: {e}", 'error')
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        flash(f"An unexpected error occurred: {e}", 'error')
    finally:
        if os.path.exists(filepath):
            os.remove(filepath)
            
    return redirect(url_for('index'))


# --- API Routes ---
@app.route('/api/companies')
def get_companies():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT CompanyLink, CompanyDescription FROM dbo.CommCompany ORDER BY CompanyDescription")
    companies = [{"id": row.CompanyLink, "name": row.CompanyDescription} for row in cursor.fetchall()]
    return jsonify(companies)

@app.route('/api/insurers/<int:company_id>')
def get_insurers(company_id):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT InsurerLink, InsurerCodeName FROM dbo._uvCommInsurer WHERE InsurerCompanyId = ? ORDER BY InsurerCodeName", company_id)
    insurers = [{"id": row.InsurerLink, "name": row.InsurerCodeName} for row in cursor.fetchall()]
    return jsonify(insurers)

@app.route('/api/payments/<int:company_id>/<int:insurer_link>')
def get_payments(company_id, insurer_link):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT p.CommPmtAutoIdx, CONVERT(varchar, p.CommPmtDate, 23) as PaymentDate, p.CommPmtReference, p.CommPmtAmount
        FROM dbo._uvCommPayment p
        LEFT JOIN dbo.CommPayment cp ON p.CommPmtAutoIdx = cp.PmtARAutoIdx
        WHERE p.CommPmtCompanyId = ? 
          AND p.CommPmtInsurerLink = ? 
          AND cp.PmtStmntId IS NULL
        ORDER BY p.CommPmtDate ASC
    """, company_id, insurer_link)
    payments = [{"id": r.CommPmtAutoIdx, "text": f"{r.PaymentDate} | {r.CommPmtReference} | R {r.CommPmtAmount:,.2f}"} for r in cursor.fetchall()]
    return jsonify(payments)

@app.route('/api/brokers')
def get_brokers():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT SalesRepId, SalesRepCodeName FROM _uvCommSalesRep WHERE Rep_On_Hold = 'N' ORDER BY SalesRepCodeName")
    brokers = [{"id": row.SalesRepId, "name": row.SalesRepCodeName} for row in cursor.fetchall()]
    return jsonify(brokers)

@app.route('/api/report_brokers')
def get_report_brokers():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT ComBrokerEvoRepId as id, ComBrokerCodeName as name FROM _uvCommissionReportRaw WHERE ComBrokerEvoRepId IS NOT NULL ORDER BY name")
        brokers = [{"id": row.id, "name": row.name} for row in cursor.fetchall()]
        return jsonify(brokers)
    except Exception as e:
        logging.error(f"Error fetching report brokers: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/api/split_history_report')
def get_split_history_report():
    """Fetches split history data based on a date range and other filters."""
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    company_id = request.args.get('company_id', 0, type=int)
    broker_id = request.args.get('broker_id', 0, type=int)
    compliance = request.args.get('compliance', 'All')
    term = request.args.get('term', '')

    if not start_date or not end_date:
        return jsonify({"success": False, "message": "Start and end dates are required."}), 400

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(f"EXEC {GET_SPLIT_HISTORY_SP} ?, ?, ?, ?, ?, ?", 
                       start_date, end_date, company_id, broker_id, compliance, term)
        columns = [column[0] for column in cursor.description]
        history_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return jsonify(history_data)
    except Exception as e:
        logging.error(f"Error fetching split history report: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/api/insurance_types')
def get_insurance_types():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT InsuranceTypeLink, InsuranceTypeName FROM [dbo].[_uvCommInsuranceTypes] ORDER BY InsuranceTypeName")
    types = [{"id": row.InsuranceTypeLink, "name": row.InsuranceTypeName} for row in cursor.fetchall()]
    return jsonify(types)

@app.route('/api/search_policies')
def search_policies():
    company_id = request.args.get('company_id', type=int)
    insurer_id = request.args.get('insurer_id', 0, type=int)
    broker_id = request.args.get('broker_id', 0, type=int)
    term = request.args.get('term', '')
    compliance_status = request.args.get('compliance', 'All') 

    if not company_id:
        return jsonify({"success": False, "message": "A Company must be selected."}), 400
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("EXEC sp_SearchPoliciesAdvanced ?, ?, ?, ?, ?", company_id, insurer_id, broker_id, term, compliance_status) 
        columns = [column[0] for column in cursor.description]
        policies = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return jsonify(policies)
    except Exception as e:
        logging.error(f"Error searching policies: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/api/get_policy_splits/<int:policy_link>')
def get_policy_splits(policy_link):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(f"EXEC {GET_SPLITS_SP} ?", policy_link)
        columns = [column[0] for column in cursor.description]
        splits = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return jsonify(splits)
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500


@app.route('/api/save_splits', methods=['POST'])
def save_splits():
    data = request.get_json()
    policy_id = data.get('policyLink')
    splits = data.get('splits')
    user_name = 'System' 

    if not policy_id or splits is None:
        return jsonify({"success": False, "message": "Missing data."}), 400
    
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        conn.autocommit = False
        
        cursor.execute("DELETE FROM [dbo].[CommSplit] WHERE SplitPolicyId = ?", policy_id)
        
        for split in splits:
            cursor.execute(f"EXEC {INSERT_SPLIT_SP} ?, ?, ?, ?", 
                           policy_id, split['brokerId'], split['percent'], user_name)
        
        run_sp(INSERT_HISTSPLIT_SP)
        conn.commit()
        return jsonify({"success": True, "message": "Splits saved successfully!"})
    except pyodbc.Error as e:
        if conn: conn.rollback()
        return jsonify({"success": False, "message": f"Database error: {str(e)}"}), 500
    finally:
        if conn: conn.autocommit = True

@app.route('/api/bulk_save_splits', methods=['POST'])
def bulk_save_splits():
    data = request.get_json()
    policy_ids = data.get('policyIds')
    splits = data.get('splits')
    compliance = data.get('compliance')
    insurance_type_id = data.get('insuranceTypeId')

    if not policy_ids or not splits:
        return jsonify({"success": False, "message": "A list of policy IDs and splits is required."}), 400

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        policy_ids_csv = ",".join(str(int(p)) for p in policy_ids)
        split_parts = [f"{int(s['brokerId'])}:{float(s['percent']):.8f}" for s in splits]
        split_defs_csv = ",".join(split_parts)

        cursor.execute("EXEC dbo.sp_BulkApplyPolicySplits_CSV ?, ?, ?", policy_ids_csv, split_defs_csv, 'SystemBulk')
        conn.commit()

        if compliance in ('Yes','No') or insurance_type_id:
            for pid in policy_ids:
                if compliance in ('Yes','No'):
                    cursor.execute("EXEC sp_UpdatePolicyCompliance ?, ?", int(pid), compliance)
                if insurance_type_id:
                    cursor.execute("EXEC sp_UpdatePolicyInsuranceType ?, ?", int(pid), int(insurance_type_id))
            conn.commit()

        cursor.execute("EXEC sp_InsertCommStmntHistSplit")
        conn.commit()

        return jsonify({"success": True, "message": f"{len(policy_ids)} policies updated successfully!"})

    except Exception as e:
        logging.error("Error during bulk split save: %s", e)
        return jsonify({"success": False, "message": str(e)}), 500



@app.route('/api/save_insurance_type', methods=['POST'])
def save_insurance_type():
    data = request.get_json()
    policy_id = data.get('policyLink')
    type_id = data.get('insuranceTypeId')

    if not all([policy_id, type_id]):
        return jsonify({"success": False, "message": "Missing policy or insurance type ID."}), 400
    
    try:
        run_sp(UPDATE_INSURANCE_TYPE_SP, int(policy_id), int(type_id))
        return jsonify({"success": True, "message": "Insurance type saved successfully!"})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500


@app.route('/api/update_compliance', methods=['POST'])
def update_compliance():
    data = request.get_json()
    policy_link = data.get('policyLink')
    is_compliant = data.get('isCompliant')

    if not policy_link or is_compliant not in ['Yes', 'No']:
        return jsonify({"success": False, "message": "Missing or invalid data."}), 400

    try:
        run_sp(UPDATE_COMPLIANCE_SP, int(policy_link), is_compliant)
        return jsonify({"success": True, "message": "Compliance status updated."})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/api/get_commission_periods/<int:company_id>')
def get_commission_periods(company_id):
    """Fetches valid, future commission periods for a specific company."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(f"EXEC {GET_COMMISSION_PERIODS_SP} ?", company_id)
        columns = [column[0] for column in cursor.description]
        periods = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return jsonify(periods)
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/api/execute_commission_run', methods=['POST'])
def execute_commission_run():
    data = request.get_json()
    period_link = data.get('periodLink')
    year_month = data.get('yearMonth')
    company_id = data.get('companyId')

    if not all([period_link, year_month, company_id]):
        return jsonify({"success": False, "message": "Period, YearMonth, and Company ID are required."}), 400

    try:
        run_sp(COMMISSION_RUN_SP, int(period_link), int(year_month), int(company_id))
        run_sp(SYNC_TO_EVO_SP, company_id)
        return jsonify({"success": True, "message": f"Commission run for period {year_month} completed successfully."})
    except Exception as e:
        error_message = str(e)
        friendly_message = error_message.split(']')[-1].strip()
        return jsonify({"success": False, "message": friendly_message}), 500

@app.route('/api/cancel_commission_run', methods=['POST'])
def cancel_commission_run():
    data = request.get_json()
    period_link = data.get('periodLink')
    company_id = data.get('companyId')

    if not all([period_link, company_id]):
        return jsonify({"success": False, "message": "Period and Company ID are required."}), 400
    
    try:
        run_sp(UNSYNC_FROM_EVO_SP, company_id)
        run_sp(CANCEL_RUN_SP, int(period_link), int(company_id))
        return jsonify({"success": True, "message": f"Commission run for period link {period_link} has been cancelled."})
    except Exception as e:
        error_message = str(e)
        friendly_message = error_message.split(']')[-1].strip()
        return jsonify({"success": False, "message": friendly_message}), 500


@app.route('/api/broker_report/<int:broker_id>')
def get_broker_report(broker_id):
    compliance_status = request.args.get('compliance', 'All')
    split_status = request.args.get('split_status', 'All')
    period_id = request.args.get('period_id', 0, type=int) 

    if broker_id is None: return jsonify({"success": False, "message": "Broker ID is required."}), 400
    if compliance_status not in ['Yes', 'No', 'All']: return jsonify({"success": False, "message": "Invalid compliance status."}), 400
    if split_status not in ['Split', 'Non-Split', 'All']: return jsonify({"success": False, "message": "Invalid split status."}), 400

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(f"EXEC {GET_BROKER_REPORT_SP} ?, ?, ?, ?", broker_id, compliance_status, split_status, period_id)
        columns = [column[0] for column in cursor.description]
        report_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return jsonify(report_data)
    except Exception as e:
        logging.error(f"Error fetching broker report for ID {broker_id}: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/api/report_periods/<int:company_id>')
def get_report_periods(company_id):
    """Fetches the last 12 closed commission periods plus a 'Current' period for reporting."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("EXEC sp_GetBrokerReportPeriods ?", company_id)
        columns = [column[0] for column in cursor.description]
        periods = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return jsonify(periods)
    except Exception as e:
        logging.error(f"Error fetching report periods: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/api/unprocessed_statements')
def get_unprocessed_statements():
    """Fetches statements that have not been processed in Evolution."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        sql = "SELECT StmntLink, StmntCompanyId, StmntInsurerLink, InsurerCodeName, StmntDate, StmntReference, StmntTotalAmount FROM dbo._uvUnprocessedStatements ORDER BY StmntDate ASC"
        cursor.execute(sql)
        columns = [column[0] for column in cursor.description]
        statements = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return jsonify(statements)
    except Exception as e:
        logging.error(f"Error fetching unprocessed statements: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/api/create_batch', methods=['POST'])
def create_batch():
    """Creates a CSV batch file for a given statement."""
    data = request.get_json()
    company_id = data.get('companyId')
    insurer_id = data.get('insurerId')
    stmnt_link = data.get('stmntLink')

    if not all([company_id, insurer_id, stmnt_link]):
        return jsonify({"success": False, "message": "Company ID, Insurer ID, and Statement Link are required."}), 400

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(1) FROM CommStmntHistSplit WHERE HistSplitStmntId = ? AND HistSplitBatchExported = 1", stmnt_link)
        if cursor.fetchone()[0] > 0:
            return jsonify({"success": False, "message": "This statement has already been exported."}), 400
        
        cursor.execute(f"EXEC {CREATE_BATCH_CSV_SP} ?, ?, ?", int(company_id), int(insurer_id), int(stmnt_link))
        columns = [column[0] for column in cursor.description]
        batch_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
        conn.commit()

        if not batch_data:
             return jsonify({"success": False, "message": "No data found to create a batch."}), 404

        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=columns)
        writer.writeheader()
        writer.writerows(batch_data)
        return jsonify({"success": True, "csv_data": output.getvalue()})

    except Exception as e:
        logging.error(f"Error creating batch for statement link {stmnt_link}: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/api/void_batch', methods=['POST'])
def void_batch():
    """Voids a previously exported batch, allowing for re-export."""
    data = request.get_json()
    company_id = data.get('companyId')
    insurer_id = data.get('insurerId')
    stmnt_link = data.get('stmntLink')

    if not all([company_id, insurer_id, stmnt_link]):
        return jsonify({"success": False, "message": "Company ID, Insurer ID, and Statement Link are required."}), 400

    try:
        run_sp(VOID_BATCH_CSV_SP, int(company_id), int(insurer_id), int(stmnt_link))
        return jsonify({"success": True, "message": "The batch has been successfully voided."})
    except Exception as e:
        logging.error(f"Error voiding batch for statement link {stmnt_link}: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

# --- NEW Deferral Feature API Endpoints ---

@app.route('/api/lapsed_policies/<int:company_id>')
def get_lapsed_policies(company_id):
    """Fetches policies with unprocessed negative commissions for a given company."""
    if not company_id:
        return jsonify({"success": False, "message": "Company ID is required."}), 400
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(f"EXEC {GET_LAPSED_POLICIES_SP} ?", company_id)
        columns = [column[0] for column in cursor.description]
        policies = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return jsonify(policies)
    except Exception as e:
        logging.error(f"Error fetching lapsed policies: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/api/create_deferral_batch', methods=['POST'])
def create_deferral_batch():
    """Creates deferral transactions and returns a CSV batch file."""
    data = request.get_json()
    policy_link = data.get('policyLink')
    num_periods = data.get('numPeriods')
    user_name = 'WebAppUser' # Can be replaced with actual user session later

    if not all([policy_link, num_periods]):
        return jsonify({"success": False, "message": "Policy Link and Number of Periods are required."}), 400

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(f"EXEC {CREATE_DEFERRAL_SP} ?, ?, ?", int(policy_link), int(num_periods), user_name)
        
        columns = [column[0] for column in cursor.description]
        batch_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
        conn.commit()

        if not batch_data:
             return jsonify({"success": False, "message": "No data returned from deferral process. The policy might not have had any negative commission to defer."}), 404

        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=columns)
        writer.writeheader()
        writer.writerows(batch_data)
        
        return jsonify({"success": True, "csv_data": output.getvalue()})

    except Exception as e:
        friendly_message = str(e).split(']')[-1].strip()
        logging.error(f"Error creating deferral for policy link {policy_link}: {friendly_message}")
        return jsonify({"success": False, "message": friendly_message}), 500

@app.route('/api/correction_history/<int:policy_link>')
def get_correction_history(policy_link):
    """Fetches unique history rows per statement with cleaned split details."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # We group by StmntId to get one row per transaction.
        # The subquery (S2) ensures we only get DISTINCT broker splits for that specific statement.
        query = """
            SELECT 
                S.HistSplitStmntId,
                MAX(S.HistSplitStmntReference) AS HistSplitStmntReference,
                MAX(S.HistSplitStmntDate) AS StmntDate,
                MAX(S.HistSplitCommPeriodYearMonth) AS PeriodYearMonth,
                MAX(S.HistSplitCommPeriodLink) AS HistSplitCommPeriodLink,
                SUM(S.HistSplitCommIncl) AS TotalCommIncl,
                MAX(CAST(S.HistSplitIsCorrection AS INT)) AS IsCorrected,
                CASE WHEN SUM(S.HistSplitCommIncl) < 0 THEN 1 ELSE 0 END AS IsReversal,
                (
                    SELECT STRING_AGG(CONCAT(B2.ComBrokerCode, ':', CAST(S2.HistSplitPercent * 100 AS INT), '%'), ', ')
                    FROM (
                        SELECT DISTINCT HistSplitBrokerLink, HistSplitPercent 
                        FROM CommStmntHistSplit 
                        WHERE HistSplitStmntId = S.HistSplitStmntId 
                        AND HistSplitPolicyLink = S.HistSplitPolicyLink
                    ) S2
                    JOIN _uvCommBroker B2 ON S2.HistSplitBrokerLink = B2.ComBrokerEvoRepId
                ) AS SplitDetails
            FROM CommStmntHistSplit S
            WHERE S.HistSplitPolicyLink = ?
            GROUP BY S.HistSplitStmntId, S.HistSplitPolicyLink
            ORDER BY MAX(S.HistSplitStmntDate) DESC
        """
        
        cursor.execute(query, policy_link)
        columns = [column[0] for column in cursor.description]
        history = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        return jsonify(history)
    except Exception as e:
        logging.error(f"Error in correction_history: {e}")
        return jsonify({"success": False, "message": str(e)}), 500
    

@app.route('/api/preview_correction/<int:policy_link>/<int:stmnt_id>')
def preview_correction(policy_link, stmnt_id):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        def process_splits(rows):
            if not rows: return []
            
            # 1. Use a dictionary to deduplicate by Broker Code automatically
            unique_splits = {}
            for row in rows:
                code = row[0]
                percent = float(row[1])
                unique_splits[code] = percent
            
            # 2. Convert dictionary back to list of objects
            data = [{"broker": k, "percent": v} for k, v in unique_splits.items()]
            
            # 3. Detect decimal format (0.5 vs 50.0)
            # If no single broker has more than 1.0, it is likely in decimal format
            max_val = max((d['percent'] for d in data), default=0)
            if 0 < max_val <= 1.0:
                for d in data:
                    d['percent'] = round(d['percent'] * 100, 2)
            else:
                for d in data:
                    d['percent'] = round(d['percent'], 2)
            return data

        # 1. NEW SPLITS (Current Policy Settings) - Fetch Code
        cursor.execute("""
            SELECT B.ComBrokerCode, S.SplitPercent 
            FROM CommSplit S
            JOIN _uvCommBroker B ON S.SplitBrokerId = B.ComBrokerEvoRepId
            WHERE S.SplitPolicyId = ?
        """, policy_link)
        new_splits = process_splits(cursor.fetchall())
        
        # 2. OLD SPLITS (Historical Statement) - Fetch Code + DISTINCT
        cursor.execute("""
            SELECT DISTINCT B.ComBrokerCode, S.HistSplitPercent 
            FROM CommStmntHistSplit S
            JOIN _uvCommBroker B ON S.HistSplitBrokerLink = B.ComBrokerEvoRepId
            WHERE S.HistSplitStmntId = ? AND S.HistSplitPolicyLink = ?
        """, stmnt_id, policy_link)
        old_splits = process_splits(cursor.fetchall())
        
        return jsonify({
            "success": True, 
            "old_splits": old_splits, 
            "new_splits": new_splits
        })
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500


@app.route('/api/process_split_correction', methods=['POST'])
def process_split_correction():
    """1. Loops through selected transactions. 2. Runs sp_SplitCorrection for each. 3. Runs sp_CreateCorrectionBatchCSV to generate the file."""
    data = request.get_json()
    policy_link = data.get('policyLink')
    transactions = data.get('transactions')
    
    if not policy_link or not transactions:
        return jsonify({"success": False, "message": "Missing policy or transactions."}), 400
    
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 1. Apply Logic: Reverse old, Insert new (using current split config)
        for item in transactions:
            cursor.execute(f"EXEC {SPLIT_CORRECTION_SP} ?, ?, ?", 
                          int(item['stmntId']), int(policy_link), int(item['periodLink']))
        
        # 2. Generate CSV: Selects the just-created unexported rows
        stmnt_ids_csv = ",".join(str(t['stmntId']) for t in transactions)
        cursor.execute(f"EXEC {CREATE_CORRECTION_CSV_SP} ?, ?", int(policy_link), stmnt_ids_csv)
        
        # Fetch the data that was selected
        batch_data = []
        # Try to get results if any
        try:
            columns = [column[0] for column in cursor.description]
            rows = cursor.fetchall()
            batch_data = [dict(zip(columns, row)) for row in rows]
        except Exception as fetch_err:
            logging.warning(f"No rows returned from procedure: {fetch_err}")
        
        conn.commit()
        
        if not batch_data:
            return jsonify({
                "success": False, 
                "message": "Correction applied, but no financial data generated for batch (amounts might be 0 or no unexported rows found)."
            }), 404
        
        # 3. Return CSV File
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=columns)
        writer.writeheader()
        writer.writerows(batch_data)
        
        return jsonify({
            "success": True, 
            "csv_data": output.getvalue()
        })
        
    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"Error processing correction: {e}")
        return jsonify({"success": False, "message": str(e)}), 500
        
    finally:
        # Don't close the connection here - let Flask handle it
        if cursor:
            cursor.close()

@app.route('/api/reverse_statement', methods=['POST'])
def reverse_statement():
    """Reverses/deletes a statement and all related records."""
    data = request.get_json()
    stmnt_link = data.get('stmntLink')
    user_name = 'WebAppUser'  # You can get this from session/auth later
    
    if not stmnt_link:
        return jsonify({"success": False, "message": "Statement Link is required."}), 400
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Execute the reversal stored procedure
        cursor.execute(f"EXEC {REVERSE_UPLOAD_SP} ?, ?", int(stmnt_link), user_name)
        
        # Get the result
        columns = [column[0] for column in cursor.description]
        result = cursor.fetchone()
        
        if result:
            result_dict = dict(zip(columns, result))
            conn.commit()
            
            # Log the action
            logging.info(f"Statement {stmnt_link} ({result_dict.get('StmntReference')}) reversed by {user_name}")
            
            return jsonify({
                "success": True,
                "message": f"Statement {result_dict.get('StmntReference')} has been successfully reversed and deleted.",
                "details": result_dict
            })
        else:
            conn.rollback()
            return jsonify({"success": False, "message": "No result returned from reversal procedure."}), 500
            
    except pyodbc.Error as e:
        if conn:
            conn.rollback()
        error_msg = str(e).split(']')[-1].strip()
        logging.error(f"Error reversing statement {stmnt_link}: {error_msg}")
        return jsonify({"success": False, "message": error_msg}), 500
    except Exception as e:
        if conn:
            conn.rollback()
        logging.error(f"Unexpected error reversing statement {stmnt_link}: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/api/search_statements')
def search_statements():
    """Searches for statements with various filters."""
    company_id = request.args.get('company_id', 0, type=int)
    insurer_id = request.args.get('insurer_id', 0, type=int)
    search_term = request.args.get('term', '')
    limit = request.args.get('limit', 30, type=int)
    include_exported = request.args.get('include_exported', 0, type=int)
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(f"EXEC {SEARCH_STATEMENTS_SP} ?, ?, ?, ?, ?", 
                      company_id, insurer_id, search_term, limit, include_exported)
        columns = [column[0] for column in cursor.description]
        statements = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return jsonify(statements)
    except Exception as e:
        logging.error(f"Error searching statements: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8080))
    serve(app, host="127.0.0.1", port=port)
