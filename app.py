import os
import re
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

# Debug print (temporary)
print(os.environ.get('DB_SERVER'))  # Should print SIGMAFIN-RDS\EVOLUTION

# --- Stored Procedure Names ---
SELECT_PAYMENTS_SP = 'sp_SelectPaymentsToProcess'
PROCESS_IMPORT_SP = 'sp_ProcessCommissionImport'
CREATE_POLICY_SP = 'sp_CreatePolicyMasterFromInput'
GET_ACTION_SP = 'sp_GetPoliciesNeedingAction'
INSERT_SPLIT_SP = 'sp_InsertPolicySplit'
UPDATE_INSURANCE_TYPE_SP = 'sp_UpdatePolicyInsuranceType'
INSERT_HIST_SP = 'sp_InsertIntoHistoryFromInput'
INSERT_HISTSPLIT_SP = 'sp_InsertCommStmntHistSplit'
INSERT_EVOCOMM_SP = 'sp_RetrieveEvoCommissionSplits'
SEARCH_POLICIES_SP = 'sp_SearchPolicies'
GET_SPLITS_SP = 'sp_GetPolicySplits'
UPDATE_COMPLIANCE_SP = 'sp_UpdatePolicyCompliance'
COMMISSION_RUN_SP = 'sp_ExecuteCommissionRun'
CANCEL_RUN_SP = 'sp_CancelCommissionRun'
GET_BROKER_REPORT_SP = 'sp_GetBrokerCommissionReport'
GET_COMMISSION_PERIODS_SP = 'sp_GetCommissionPeriods'
CREATE_BATCH_CSV_SP = 'sp_CreateBatchCSV'


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
        raise Exception(f"Database error in {sp_name}: {e}")

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
    """Inserts data from a DataFrame into the ZZImportStage table."""
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
        data_to_insert = [
            (
                str(row['Plan_number']) if pd.notna(row['Plan_number']) else None,
                str(row['Planholder_surname']) if pd.notna(row['Planholder_surname']) else None,
                str(row['Planholder_initials']) if pd.notna(row['Planholder_initials']) else None,
                float(row['Commission_Fees_Incl']) if pd.notna(row['Commission_Fees_Incl']) else 0.0,
                float(row['VAT_Amount']) if pd.notna(row['VAT_Amount']) else 0.0
            ) for _, row in df.iterrows()
        ]
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

@app.route('/report')
def report():
    """Renders the broker commission report page."""
    return render_template('report.html')


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
        run_sp(INSERT_EVOCOMM_SP)
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
        ORDER BY p.CommPmtDate DESC
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

@app.route('/api/insurance_types')
def get_insurance_types():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT InsuranceTypeLink, InsuranceTypeName FROM [dbo].[_uvCommInsuranceTypes] ORDER BY InsuranceTypeName")
    types = [{"id": row.InsuranceTypeLink, "name": row.InsuranceTypeName} for row in cursor.fetchall()]
    return jsonify(types)

@app.route('/api/search_policies')
def search_policies():
    term = request.args.get('term', '')
    if not term or len(term) < 3:
        return jsonify({"success": False, "message": "Search term must be at least 3 characters long."}), 400
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(f"EXEC {SEARCH_POLICIES_SP} ?", f'%{term}%')
        columns = [column[0] for column in cursor.description]
        policies = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return jsonify(policies)
    except Exception as e:
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

    if not policy_id or splits is None:
        return jsonify({"success": False, "message": "Missing data."}), 400
    
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        conn.autocommit = False
        
        cursor.execute("DELETE FROM [dbo].[CommSplit] WHERE SplitPolicyId = ?", policy_id)
        
        for split in splits:
            cursor.execute(f"EXEC {INSERT_SPLIT_SP} ?, ?, ?", policy_id, split['brokerId'], split['percent'])
        
        run_sp(INSERT_HISTSPLIT_SP)
        conn.commit()
        return jsonify({"success": True, "message": "Splits saved successfully!"})
    except pyodbc.Error as e:
        if conn: conn.rollback()
        return jsonify({"success": False, "message": f"Database error: {str(e)}"}), 500
    finally:
        if conn: conn.autocommit = True


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
        return jsonify({"success": True, "message": f"Commission run for period {year_month} completed successfully."})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/api/cancel_commission_run', methods=['POST'])
def cancel_commission_run():
    data = request.get_json()
    period_link = data.get('periodLink')
    company_id = data.get('companyId')

    if not all([period_link, company_id]):
        return jsonify({"success": False, "message": "Period and Company ID are required."}), 400
    
    try:
        run_sp(CANCEL_RUN_SP, int(period_link), int(company_id))
        return jsonify({"success": True, "message": f"Commission run for period link {period_link} has been cancelled."})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@app.route('/api/broker_report/<int:broker_id>')
def get_broker_report(broker_id):
    compliance_status = request.args.get('compliance', 'All')
    split_status = request.args.get('split_status', 'All')

    if not broker_id:
        return jsonify({"success": False, "message": "Broker ID is required."}), 400
    if compliance_status not in ['Yes', 'No', 'All']:
        return jsonify({"success": False, "message": "Invalid compliance status."}), 400
    if split_status not in ['Split', 'Non-Split', 'All']:
        return jsonify({"success": False, "message": "Invalid split status."}), 400

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute(f"EXEC {GET_BROKER_REPORT_SP} ?, ?, ?", broker_id, compliance_status, split_status)
        
        columns = [column[0] for column in cursor.description]
        report_data = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return jsonify(report_data)
        
    except Exception as e:
        logging.error(f"Error fetching broker report for ID {broker_id}: {e}")
        return jsonify({"success": False, "message": str(e)}), 500

# --- API Routes for Batch Creation (Corrected) ---

@app.route('/api/unprocessed_statements')
def get_unprocessed_statements():
    """Fetches statements that have not been processed in Evolution."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT StmntCompanyId, StmntInsurerLink, StmntDate, StmntReference, StmntTotalAmount FROM dbo._uvUnprocessedStatements ORDER BY StmntDate DESC")
        columns = [column[0] for column in cursor.description]
        statements = []
        for row in cursor.fetchall():
            row_dict = dict(zip(columns, row))
            cursor.execute("SELECT StmntLink FROM dbo.CommStmntMaster WHERE StmntCompanyId = ? AND StmntInsurerLink = ? AND StmntDate = ? AND StmntReference = ?", 
                           row_dict['StmntCompanyId'], row_dict['StmntInsurerLink'], row_dict['StmntDate'], row_dict['StmntReference'])
            stmnt_link_row = cursor.fetchone()
            if stmnt_link_row:
                row_dict['stmntLink'] = stmnt_link_row.StmntLink
                statements.append(row_dict)

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
        
        cursor.execute(f"EXEC {CREATE_BATCH_CSV_SP} ?, ?, ?", int(company_id), int(insurer_id), int(stmnt_link))
        
        columns = [column[0] for column in cursor.description]
        batch_data = [dict(zip(columns, row)) for row in cursor.fetchall()]

        # Force TaxType to have leading zeroes
        for row in batch_data:
            if "TaxType" in row and row["TaxType"] is not None:
                row["TaxType"] = str(row["TaxType"]).zfill(2)

        if not batch_data:
             return jsonify({"success": False, "message": "No data found to create a batch for the selected statement."}), 404

        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=columns)
        writer.writeheader()
        writer.writerows(batch_data)
        
        csv_string = output.getvalue()
        
        return jsonify({"success": True, "csv_data": csv_string})

    except Exception as e:
        logging.error(f"Error creating batch for statement link {stmnt_link}: {e}")
        return jsonify({"success": False, "message": str(e)}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5050, debug=True)
