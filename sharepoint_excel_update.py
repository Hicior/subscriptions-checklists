import os
import requests
import pandas as pd
import datetime
import time
import json
import sys
import urllib3
from datetime import datetime as dt, timezone
from tqdm import tqdm
import numpy as np
from msal import ConfidentialClientApplication
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# =============================================================================
# CONFIGURATION AND API SETUP
# =============================================================================

# Load configuration from JSON file
config_file_path = 'config.json'
try:
    with open(config_file_path, 'r', encoding='utf-8') as config_file:
        config = json.load(config_file)
    print("✅ Configuration loaded from config.json")
except FileNotFoundError:
    print("❌ Configuration file config.json not found. Please create the config.json file.")
    exit()
except json.JSONDecodeError as e:
    print(f"❌ Error parsing config.json: {e}. Please fix the JSON syntax in the config file.")
    exit()

# API Keys
calendesk_api_key = os.getenv('CALENDESK_API_KEY')
stripe_api_key = os.getenv('STRIPE_API_KEY')

# Azure/SharePoint credentials
azure_app_id = os.getenv('ApplicationID')
azure_directory_id = os.getenv('DirectoryID')
azure_secret_id = os.getenv('SecretID')
azure_secret_value = os.getenv('SecretValue')

# CRM credentials
crm_password = os.getenv('CRM_PASSWORD')

if not calendesk_api_key:
    print("❌ No Calendesk API key found. Please set the CALENDESK_API_KEY environment variable.")
    exit()

if not stripe_api_key:
    print("❌ No Stripe API key found. Please set the STRIPE_API_KEY environment variable.")
    exit()

if not all([azure_app_id, azure_directory_id, azure_secret_value]):
    print("❌ Missing Azure credentials. Please set ApplicationID, DirectoryID, and SecretValue environment variables.")
    exit()

if not crm_password:
    print("❌ No CRM password found. Please set the CRM_PASSWORD environment variable.")
    exit()

print("✅ API keys, Azure credentials, and CRM credentials loaded successfully")

# SharePoint configuration
sharepoint_site_id = "slawomirmentzen.sharepoint.com,95990869-0549-4ea1-b74d-937205ea3c78,2b2b1d2f-91a9-40f1-9c02-28d6d7204e3a"
sharepoint_drive_id = "b!aQiZlUkFoU63TZNyBeo8eC8dKyupkfFAnAIo1tcgTjodLd5FMwyAS7vEmsg7KfFs"
sharepoint_file_id = "01AXKB4J3CCZBLAQ2G7RB3B5WZF2SLW64T"

# Sheet names
subscriptions_sheet_name = 'Subskrypcje klientów'

# Fetch all available data - no page limits

# Calendesk API configuration
calendesk_headers = {
    "X-Tenant": "slawomir-mentzen-rvs",
    "X-Api-Key": calendesk_api_key
}

subscriptions_url = 'https://api.calendesk.com/api/admin/subscriptions'
users_url = 'https://api.calendesk.com/api/admin/v2/users/subscriptions'

# Stripe API configuration
stripe_headers = {
    'Authorization': f'Bearer {stripe_api_key}',
}
stripe_base_url = 'https://api.stripe.com/v1/invoices'

# CRM API configuration
crm_config = {
    "api": {
        "base_url": "https://crm.mentzen.pl/api",
        "login_url": "https://crm.mentzen.pl/api/authentication/login",
        "email": "w.kuczkowski@mentzen.pl",
        "verify_ssl": False
    },
    "project": {
        "id": "62737570496f3c001a5576ef"
    },
    "target_status": "UMOWY TRADYCYJNE"
}

# =============================================================================
# SHAREPOINT AUTHENTICATION AND FILE OPERATIONS
# =============================================================================

def get_access_token():
    """Get access token for Microsoft Graph API"""
    authority = f"https://login.microsoftonline.com/{azure_directory_id}"
    scope = ["https://graph.microsoft.com/.default"]
    
    app = ConfidentialClientApplication(
        azure_app_id,
        authority=authority,
        client_credential=azure_secret_value,
    )
    
    result = app.acquire_token_silent(scope, account=None)
    
    if not result:
        print("🔑 Acquiring token from Azure...")
        result = app.acquire_token_for_client(scopes=scope)
    
    if "access_token" in result:
        print("✅ Successfully acquired access token")
        return result["access_token"]
    else:
        print(f"❌ Failed to acquire token: {result.get('error')}")
        print(f"   Error description: {result.get('error_description')}")
        return None

def clear_excel_worksheet(access_token, worksheet_name, column_range=None):
    """Clear data from a worksheet (keeping headers) - clears ALL rows to ensure no old data remains"""
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    
    # Define the range to clear - clear many rows to ensure all old data is removed
    if column_range:
        # Clear specific column range from row 2 to row 15000 to ensure all data is cleared
        # This is much safer than just clearing the used range
        start_col = column_range.split(':')[0]
        end_col = column_range.split(':')[1]
        clear_range = f"{start_col}2:{end_col}15000"
        print(f"🧹 Clearing range {clear_range} in {worksheet_name} to ensure all old data is removed...")
    else:
        # Clear all columns from row 2 to row 15000
        clear_range = "2:15000"
        print(f"🧹 Clearing range {clear_range} in {worksheet_name} to ensure all old data is removed...")
    
    clear_url = f"https://graph.microsoft.com/v1.0/sites/{sharepoint_site_id}/drives/{sharepoint_drive_id}/items/{sharepoint_file_id}/workbook/worksheets/{worksheet_name}/range(address='{clear_range}')/clear"
    clear_response = requests.post(clear_url, headers=headers, json={"applyTo": "Contents"})
    
    # Status code 200 or 204 both indicate success for clear operations
    if clear_response.status_code in [200, 204]:
        print(f"✅ Successfully cleared range {clear_range}")
        return True
    else:
        print(f"❌ Failed to clear range {clear_range}: Status code {clear_response.status_code}")
        print(f"   Response: {clear_response.text[:500]}...")
        return False

def update_excel_worksheet_directly(access_token, worksheet_name, data_df, start_column='A'):
    """Update Excel worksheet directly in SharePoint using Graph API"""
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    
    print(f"📝 Updating {worksheet_name} directly in SharePoint...")
    
    # Clear existing data first - C:U columns for Subskrypcje klientów
    if worksheet_name == 'Subskrypcje klientów':
        clear_excel_worksheet(access_token, worksheet_name, "C:U")
    else:
        clear_excel_worksheet(access_token, worksheet_name)
    
    if len(data_df) == 0:
        print(f"⚠ No data to write to {worksheet_name}")
        return True
    
    # Convert DataFrame to values (as list of lists)
    data_values = data_df.values.tolist()
    
    # Convert any pandas NaT or NaN to None for JSON serialization
    for i in range(len(data_values)):
        for j in range(len(data_values[i])):
            if pd.isna(data_values[i][j]):
                data_values[i][j] = None
            elif hasattr(data_values[i][j], 'isoformat'):  # DateTime objects
                # Use Excel-friendly date format without 'T' separator
                data_values[i][j] = data_values[i][j].strftime('%Y-%m-%d %H:%M:%S')
    
    # Determine range - start from row 2 to preserve headers
    num_rows = len(data_values)
    num_cols = len(data_df.columns)
    
    # Convert column count to Excel column letter
    def get_column_letter(col_num):
        """Convert 1-based column number to Excel column letter"""
        result = ""
        while col_num > 0:
            col_num -= 1
            result = chr(col_num % 26 + ord('A')) + result
            col_num //= 26
        return result
    
    # For Subskrypcje klientów sheet, start from column C
    if worksheet_name == 'Subskrypcje klientów':
        start_col_num = 3  # Column C
        end_col_num = start_col_num + num_cols - 1
        start_col_letter = get_column_letter(start_col_num)
        end_col_letter = get_column_letter(end_col_num)
        range_address = f"{start_col_letter}2:{end_col_letter}{num_rows + 1}"
    else:
        end_col = get_column_letter(num_cols)
        range_address = f"{start_column}2:{end_col}{num_rows + 1}"
    
    # Update the range with new data
    update_url = f"https://graph.microsoft.com/v1.0/sites/{sharepoint_site_id}/drives/{sharepoint_drive_id}/items/{sharepoint_file_id}/workbook/worksheets/{worksheet_name}/range(address='{range_address}')"
    
    payload = {
        "values": data_values
    }
    
    response = requests.patch(update_url, headers=headers, json=payload)
    
    if response.status_code == 200:
        print(f"✅ Updated {worksheet_name}: {len(data_values)} rows")
        return True
    else:
        print(f"❌ Failed to update {worksheet_name}: Status code {response.status_code}")
        print(f"   Response: {response.text[:500]}...")
        return False

def update_current_date_cell(access_token, worksheet_name):
    """Update A2 cell with current date (without time)"""
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    
    print(f"📅 Updating A2 cell with current date in {worksheet_name}...")
    
    # Get current date without time
    current_date = dt.now().strftime('%Y-%m-%d')
    
    # Update A2 cell
    update_url = f"https://graph.microsoft.com/v1.0/sites/{sharepoint_site_id}/drives/{sharepoint_drive_id}/items/{sharepoint_file_id}/workbook/worksheets/{worksheet_name}/range(address='A2')"
    
    payload = {
        "values": [[current_date]]
    }
    
    response = requests.patch(update_url, headers=headers, json=payload)
    
    if response.status_code == 200:
        print(f"✅ Updated A2 cell with current date: {current_date}")
        return True
    else:
        print(f"❌ Failed to update A2 cell: Status code {response.status_code}")
        print(f"   Response: {response.text[:500]}...")
        return False

# =============================================================================
# CRM API HANDLER AND FUNCTIONS
# =============================================================================

class CRMAPIHandler:
    def __init__(self, config):
        self.config = config
        self.session = None
        self.headers = None
        
        # Disable SSL warnings if verify_ssl is False
        if not config['api']['verify_ssl']:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    def initialize_session(self):
        """Initialize session with authentication"""
        login_payload = {
            "email": self.config['api']['email'],
            "password": crm_password
        }

        self.session = requests.Session()
        login_response = self.session.post(
            self.config['api']['login_url'], 
            json=login_payload, 
            verify=self.config['api']['verify_ssl']
        )

        if login_response.status_code != 201:
            print(f"❌ CRM login failed: {login_response.status_code}")
            return False

        auth_token = self.session.cookies.get('Authentication')
        if not auth_token:
            print("❌ Failed to get CRM Authentication token.")
            return False

        self.headers = {
            "Content-Type": "application/json",
            "Cookie": f"Authentication={auth_token}"
        }
        print("✅ CRM session initialized successfully")
        return True
    
    def fetch_project_data(self, project_id):
        """Fetch project data including statuses"""
        url = f"{self.config['api']['base_url']}/projects/{project_id}"
        response = self.session.get(url, headers=self.headers, verify=self.config['api']['verify_ssl'])
        if response.status_code != 200:
            print(f"❌ Failed to fetch project data: {response.status_code}")
            return None
        return response.json()

    def fetch_tasks_by_status(self, status_id):
        """Fetch all tasks for a specific status"""
        tasks = []
        page = 0
        while True:
            url = f"{self.config['api']['base_url']}/tasks/by-status/{status_id}?page={page}"
            response = self.session.post(url, headers=self.headers, json={}, verify=self.config['api']['verify_ssl'])
            if response.status_code != 200:
                print(f"❌ Failed to fetch tasks for status {status_id} on page {page}: {response.status_code}")
                break
            data = response.json()
            tasks_on_page = data.get("tasks", [])
            tasks.extend(tasks_on_page)
            total_pages = data.get("totalPage", data.get("totalPages", 1))
            if page >= total_pages:
                break
            page += 1
        return tasks

    def fetch_task_data(self, task_id):
        """Fetch detailed task data"""
        url = f"{self.config['api']['base_url']}/tasks/{task_id}"
        response = self.session.get(url, headers=self.headers, verify=self.config['api']['verify_ssl'])
        if response.status_code == 200:
            return response.json()
        else:
            print(f"❌ Failed to fetch task data for task {task_id}: {response.status_code}")
            return None

def extract_client_nip(task):
    """Extract client NIP from a task dictionary"""
    try:
        if 'client' in task and isinstance(task['client'], dict):
            client = task['client']
            if 'company' in client and isinstance(client['company'], dict):
                nip = client['company'].get('nip', '')
                
                # Convert numeric NIP to string with proper format (10 digits)
                if isinstance(nip, (int, float)) and nip > 0:
                    nip_str = str(int(nip))  # Convert to int first to remove any decimal part
                    
                    # If NIP is less than 10 digits, pad with leading zeros
                    if len(nip_str) < 10:
                        nip_str = nip_str.zfill(10)
                    
                    return nip_str
                elif isinstance(nip, str) and nip:
                    return nip
                else:
                    return ''
            else:
                return ''
        else:
            return ''
    except Exception as e:
        print(f"❌ Error extracting NIP: {e} for task: {task.get('_id', '')}")
        return ''

def find_status_by_name(project_data, status_name):
    """Find status ID by status name"""
    for status in project_data.get("statuses", []):
        if status.get("name", "").strip() == status_name.strip():
            return status["_id"]
    return None

def fetch_crm_data():
    """Fetch CRM data for UMOWY TRADYCYJNE status"""
    print("🔄 Fetching CRM data for UMOWY TRADYCYJNE...")
    
    # Initialize API handler
    api_handler = CRMAPIHandler(crm_config)
    if not api_handler.initialize_session():
        print("❌ Failed to initialize CRM session")
        return []
    
    # Fetch project data to get status information
    project_data = api_handler.fetch_project_data(crm_config['project']['id'])
    if not project_data:
        print("❌ Failed to fetch CRM project data")
        return []
    
    # Find the status ID for "UMOWY TRADYCYJNE"
    status_id = find_status_by_name(project_data, crm_config['target_status'])
    
    if not status_id:
        print(f"❌ Status '{crm_config['target_status']}' not found in project")
        return []
    
    # Fetch tasks for the target status
    tasks = api_handler.fetch_tasks_by_status(status_id)
    
    if not tasks:
        print("⚠ No tasks found for UMOWY TRADYCYJNE status")
        return []
    
    print(f"📋 Found {len(tasks)} tasks with UMOWY TRADYCYJNE status")
    
    # Process each task and extract client NIP
    nip_list = []
    with tqdm(desc="Processing CRM tasks", total=len(tasks)) as pbar:
        for task in tasks:
            task_id = task.get("_id", "")
            
            # Fetch detailed task data to get complete client info
            detailed_task = api_handler.fetch_task_data(task_id)
            
            if detailed_task:
                # Extract client NIP
                nip = extract_client_nip(detailed_task)
                
                if nip:
                    nip_list.append(nip)
            
            pbar.update(1)
    
    print(f"✓ Extracted {len(nip_list)} NIP numbers from CRM")
    return nip_list

def create_crm_dataframe(nip_list):
    """Create DataFrame for CRM data with required columns matching Calendesk structure"""
    if not nip_list:
        print("⚠ No CRM data to process")
        return pd.DataFrame()
    
    print(f"🔄 Creating CRM DataFrame with {len(nip_list)} records...")
    
    # Create empty DataFrame with all the same columns as Calendesk data
    crm_data = []
    
    for i, nip in enumerate(nip_list):
        # Create a record with all columns, setting specific values as required
        record = {
            'ID Subskrypcji Klienta': 0,  # Set to 0 as requested
            'ID Subskrypcji': 0,
            'Status': '',
            'Data zakupu': None,
            'Pakiet': 'UMOWA TRADYCYJNA',  # Set to "UMOWA TRADYCYJNA" as requested
            'Data wygaśnięcia': None,
            'Data anulowania': None,
            'ID Klienta': 0,
            'Imię i Nazwisko Klienta': '',
            'Email': '',
            'Typ pakietu': '',
            'ID Suba STRIPE': '',
            'NIP': nip,  # The actual NIP number from CRM
            'Nazwa Firmy': '',
            'Telefon': '',
            # Custom columns with empty values
            'Invoice status in chosen month': '',
            'Invoice status in last 2 months': '',
            'Last invoice month': '',
            'Status3': 'Nieokreślony'  # Default status
        }
        crm_data.append(record)
    
    df_crm = pd.DataFrame(crm_data)
    print(f"✓ Created CRM DataFrame with {len(df_crm)} records")
    return df_crm

# =============================================================================
# UTILITY FUNCTIONS (Fetch all data)
# =============================================================================

def fetch_calendesk_data_all(url, headers, description="Fetching data"):
    """Fetch data from a Calendesk API endpoint (limited to 10 pages for testing)"""
    all_data = []
    current_page = 1
    max_pages = 10  # Limit to 10 pages for testing
    
    print(f"🔄 {description} - fetching up to {max_pages} pages for testing...")
    with tqdm(desc=description, unit="page") as pbar:
        while current_page <= max_pages:
            params = {
                'limit': 100,
                'page': current_page,
                'order_by': 'id',
                'ascending': 0
            }
            
            response = make_api_request_with_retry(url, headers, params)
            if response and response.status_code == 200:
                response_data = response.json()
                data = response_data.get('data', [])
                
                if data:
                    all_data.extend(data)
                    pbar.update(1)
                    current_page += 1
                else:
                    # No more data available
                    break
            else:
                error_msg = f"Status code {response.status_code}" if response else "No response after retries"
                print(f'  ❌ Failed to fetch page {current_page}: {error_msg}')
                break
    
    print(f"✓ Fetched {len(all_data)} records from {current_page-1} pages (testing mode)")
    return all_data

def fetch_stripe_invoices_all():
    """Fetch Stripe invoices with pagination (limited to 10 pages for testing)"""
    # Get filter date from configuration
    filter_config = config['stripe_fetch_settings']['filter_date']
    filter_date = dt(filter_config['year'], filter_config['month'], filter_config['day'], 0, 0, 0, tzinfo=timezone.utc)
    filter_timestamp = int(filter_date.timestamp())
    
    params = {
        'limit': 100,
        'created[gte]': filter_timestamp
    }
    
    processed_data = []
    pages_fetched = 0
    max_pages = 10  # Limit to 10 pages for testing
    
    print(f"🔄 Fetching Stripe invoices - up to {max_pages} pages for testing...")
    with tqdm(desc="Fetching Stripe invoices") as pbar:
        while pages_fetched < max_pages:
            response = requests.get(stripe_base_url, headers=stripe_headers, params=params)
            
            if response.status_code != 200:
                print(f"❌ Failed to fetch Stripe data: Status code {response.status_code}")
                break
            
            response_data = response.json()
            invoices = response_data.get('data', [])
            
            if not invoices:
                # No more invoices available
                break
            
            for invoice in invoices:
                lines_data = invoice['lines']['data']
                if lines_data:
                    first_line_item = lines_data[0]
                    lines_data_description = first_line_item.get('description', 'No description')
                    
                    period = first_line_item.get('period', {})
                    plan = first_line_item.get('plan', {})
                    
                    plan_active = plan.get('active', 'No plan active info') if plan else 'No plan active info'
                    plan_interval = plan.get('interval', 'No plan interval info') if plan else 'No plan interval info'
                else:
                    lines_data_description = 'No description'
                    plan_active = 'No plan active info'
                    plan_interval = 'No plan interval info'
                
                invoice_data = {
                    'id': invoice['id'],
                    'amount_due': invoice['amount_due'] / 100,
                    'amount_paid': invoice['amount_paid'] / 100,
                    'amount_remaining': invoice['amount_remaining'] / 100,
                    'created': convert_timestamp_to_date(invoice['created']),
                    'customer': invoice['customer'],
                    'lines_data_description': lines_data_description,
                    'plan_active': plan_active,
                    'plan_interval': plan_interval,
                    'subscription': invoice['subscription'],
                    'attempt_count': invoice.get('attempt_count', 0),
                    'payment_intent': invoice.get('payment_intent', 'No payment intent'),
                    'status': invoice['status'],
                    'paid': invoice.get('paid', False)
                }
                processed_data.append(invoice_data)
            
            pbar.update(len(invoices))
            pages_fetched += 1
            
            if response_data.get('has_more', False) and invoices:
                last_invoice_id = invoices[-1]['id']
                params['starting_after'] = last_invoice_id
            else:
                # No more pages available
                break
    
    print(f"✓ Fetched {len(processed_data)} invoices from {pages_fetched} pages (testing mode)")
    return processed_data

def convert_timestamp_to_date(timestamp):
    """Convert Unix timestamp to date string"""
    return dt.fromtimestamp(timestamp).strftime('%Y-%m-%d')

def make_api_request_with_retry(url, headers, params, max_retries=3, delay=2):
    """Make API request with retry logic for transient failures"""
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            
            if response.status_code == 200:
                return response
            elif response.status_code == 429:  # Rate limiting
                wait_time = delay * (2 ** attempt)  # Exponential backoff
                print(f"  ⏳ Rate limited (attempt {attempt + 1}/{max_retries}), waiting {wait_time} seconds...")
                time.sleep(wait_time)
                continue
            elif response.status_code >= 500:  # Server errors
                wait_time = delay * (attempt + 1)
                print(f"  🔄 Server error {response.status_code} (attempt {attempt + 1}/{max_retries}), retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                continue
            else:
                # Client error, don't retry
                return response
                
        except requests.exceptions.Timeout:
            print(f"  ⏱ Request timeout (attempt {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                time.sleep(delay * (attempt + 1))
                continue
        except requests.exceptions.RequestException as e:
            print(f"  ❌ Request error (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(delay * (attempt + 1))
                continue
    
    # All retries failed
    return None

def validate_calendesk_data(data, endpoint_type):
    """Validate the structure of Calendesk API data"""
    if not data:
        print(f"⚠ Warning: No data returned from {endpoint_type} endpoint")
        return False
    
    required_fields = {
        'subscriptions': ['id', 'name', 'price'],
        'users': ['id', 'subscription_id', 'user', 'status']
    }
    
    fields_to_check = required_fields.get(endpoint_type, [])
    if not fields_to_check:
        return True
    
    # Check first few records for required fields
    sample_size = min(3, len(data))
    missing_fields = set()
    
    for i in range(sample_size):
        record = data[i]
        for field in fields_to_check:
            if field not in record:
                missing_fields.add(field)
    
    if missing_fields:
        print(f"⚠ Warning: Missing expected fields in {endpoint_type} data: {missing_fields}")
        print(f"   Sample record keys: {list(data[0].keys())[:10]}...")
        return False
    
    print(f"✅ Data structure validation passed for {endpoint_type}")
    return True

# =============================================================================
# CUSTOM CALCULATION FUNCTIONS (Same as original)
# =============================================================================

def calculate_invoice_status_chosen_month(row, invoices_df, config_data=None):
    """Calculate invoice status for chosen month"""
    if pd.isna(row['ID Suba STRIPE']) or row['ID Suba STRIPE'] == '':
        return "Nie można określić"
    
    # Get configuration values
    if config_data is None:
        config_data = config['date_settings']['invoice_status_chosen_month']
    
    chosen_month = config_data['month']
    chosen_year = config_data['year']
    yearly_start_year = config_data['yearly_subscription_start_year']
    
    subscription_id = row['ID Suba STRIPE']
    package_type = row['Typ pakietu']
    
    if package_type == 'miesięczny':
        # Monthly subscription
        filtered_invoices = invoices_df[
            (invoices_df['ID_Subskrypcji'] == subscription_id) &
            (invoices_df['Data Utworzenia'].dt.month == chosen_month) &
            (invoices_df['Data Utworzenia'].dt.year == chosen_year)
        ]
        if not filtered_invoices.empty:
            return filtered_invoices.iloc[0]['Status Faktury']
    elif package_type == 'roczny':
        # Yearly subscription
        start_date = dt(yearly_start_year, chosen_month, 1)
        end_date = dt(chosen_year, chosen_month, 30)
        filtered_invoices = invoices_df[
            (invoices_df['ID_Subskrypcji'] == subscription_id) &
            (invoices_df['Data Utworzenia'] >= start_date) &
            (invoices_df['Data Utworzenia'] <= end_date)
        ]
        if not filtered_invoices.empty:
            return filtered_invoices.iloc[0]['Status Faktury']
    
    return ""

def calculate_invoice_status_last_2_months(row, invoices_df, config_data=None):
    """Calculate invoice status for last 2 months"""
    if pd.isna(row['ID Suba STRIPE']) or row['ID Suba STRIPE'] == '':
        return "Nie można określić"
    
    # Get configuration values
    if config_data is None:
        config_data = config['date_settings']['invoice_status_last_2_months']
    
    month1 = config_data['month1']
    month2 = config_data['month2']
    year = config_data['year']
    yearly_start_year = config_data['yearly_subscription_start_year']
    
    subscription_id = row['ID Suba STRIPE']
    package_type = row['Typ pakietu']
    
    if package_type == 'miesięczny':
        # Monthly subscription
        start_date = dt(year, month1, 1)
        end_date = dt(year, month2, 31)
        filtered_invoices = invoices_df[
            (invoices_df['ID_Subskrypcji'] == subscription_id) &
            (invoices_df['Data Utworzenia'] >= start_date) &
            (invoices_df['Data Utworzenia'] <= end_date) &
            (invoices_df['Status Faktury'] == 'paid')
        ]
        return "paid" if not filtered_invoices.empty else ""
    elif package_type == 'roczny':
        # Yearly subscription
        start_date = dt(yearly_start_year, month2, 1)
        end_date = dt(year, month2, 31)
        filtered_invoices = invoices_df[
            (invoices_df['ID_Subskrypcji'] == subscription_id) &
            (invoices_df['Data Utworzenia'] >= start_date) &
            (invoices_df['Data Utworzenia'] <= end_date) &
            (invoices_df['Status Faktury'] == 'paid')
        ]
        return "paid" if not filtered_invoices.empty else ""
    
    return ""

def calculate_last_invoice_month(row, invoices_df, config_data=None):
    """Calculate last invoice month"""
    if pd.isna(row['ID Suba STRIPE']) or row['ID Suba STRIPE'] == '':
        return ""
    
    # Get configuration values
    if config_data is None:
        config_data = config['date_settings']['last_invoice_month']
    
    current_year = config_data['current_year']
    yearly_start_year = config_data['yearly_subscription_start_year']
    
    subscription_id = row['ID Suba STRIPE']
    package_type = row['Typ pakietu']
    
    # Polish month names mapping
    polish_months = {
        1: 'styczeń', 2: 'luty', 3: 'marzec', 4: 'kwiecień',
        5: 'maj', 6: 'czerwiec', 7: 'lipiec', 8: 'sierpień',
        9: 'wrzesień', 10: 'październik', 11: 'listopad', 12: 'grudzień'
    }
    
    filtered_invoices = invoices_df[
        (invoices_df['ID_Subskrypcji'] == subscription_id) &
        (invoices_df['Status Faktury'] == 'paid')
    ]
    
    if package_type == 'miesięczny':
        filtered_invoices = filtered_invoices[
            filtered_invoices['Data Utworzenia'].dt.year == current_year
        ]
    elif package_type == 'roczny':
        filtered_invoices = filtered_invoices[
            filtered_invoices['Data Utworzenia'] >= dt(yearly_start_year, 1, 1)
        ]
    
    if not filtered_invoices.empty:
        max_date = filtered_invoices['Data Utworzenia'].max()
        return polish_months.get(max_date.month, "")
    
    return ""

def calculate_status3(row):
    """Calculate Status3 based on expiration date and subscription client ID"""
    current_datetime = dt.now()
    expiration_date = row.get('Data wygaśnięcia')
    subscription_client_id = row.get('ID Subskrypcji Klienta')
    
    # Check if subscription client ID is blank
    if pd.isna(subscription_client_id) or subscription_client_id == '':
        return "Nieokreślony"
    
    # Check if expiration date is blank
    expiration_date_is_blank = pd.isna(expiration_date) or expiration_date == ''
    
    # Convert expiration date to datetime if it's not blank
    if not expiration_date_is_blank:
        if hasattr(expiration_date, 'to_pydatetime'):
            expiration_date = expiration_date.to_pydatetime()
        elif isinstance(expiration_date, str):
            try:
                expiration_date = dt.strptime(expiration_date, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                try:
                    expiration_date = dt.strptime(expiration_date, '%Y-%m-%d')
                except ValueError:
                    expiration_date_is_blank = True
    
    # Apply the Status3 logic from Excel formula
    if not expiration_date_is_blank and expiration_date > current_datetime:
        # Data wygaśnięcia > NOW() AND NOT(ISBLANK(Data wygaśnięcia)) AND NOT(ISBLANK(ID Subskrypcji Klienta))
        return "Anulowana (aktywna)"
    elif expiration_date_is_blank or expiration_date > current_datetime:
        # (Data wygaśnięcia > NOW() OR ISBLANK(Data wygaśnięcia)) AND NOT(ISBLANK(ID Subskrypcji Klienta))
        return "Aktywna"
    elif not expiration_date_is_blank and expiration_date <= current_datetime:
        # Data wygaśnięcia <= NOW() AND NOT(ISBLANK(ID Subskrypcji Klienta))
        return "Anulowana"
    else:
        return "Nieokreślony"

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    print("🚀 Starting SharePoint Excel updater...")
    
    # Get access token
    access_token = get_access_token()
    if not access_token:
        print("❌ Failed to get access token. Exiting.")
        return
    
    print("📋 Connecting to Excel file in SharePoint...")
    
    # Fetch Calendesk data
    print("\n🔄 Fetching Calendesk data...")
    all_subscriptions = fetch_calendesk_data_all(subscriptions_url, calendesk_headers, "Calendesk subscriptions")
    validate_calendesk_data(all_subscriptions, 'subscriptions')
    
    users_subscriptions = fetch_calendesk_data_all(users_url, calendesk_headers, "Calendesk users")
    validate_calendesk_data(users_subscriptions, 'users')
    
    print(f"✓ Calendesk: {len(all_subscriptions)} subscriptions, {len(users_subscriptions)} users")
    
    # Process Calendesk data (same as original script)
    print("🔄 Processing Calendesk data...")
    
    if not all_subscriptions:
        print("❌ No subscriptions data fetched - cannot continue")
        return
    
    if not users_subscriptions:
        print("❌ No user subscriptions data fetched - cannot continue")
        return
    
    try:
        subscriptions_df = pd.json_normalize(all_subscriptions, sep='.')
        users_subscriptions_df = pd.json_normalize(users_subscriptions, sep='.')
        
        print(f"✓ DataFrames created: {len(subscriptions_df)} subscriptions, {len(users_subscriptions_df)} user records")
        
        # Extract phone number
        users_subscriptions_df['user_default_phone_e164'] = users_subscriptions_df['user.default_phone.e164'].fillna('')
        
        # Merge DataFrames
        print("🔄 Merging Calendesk DataFrames...")
        df_calendesk = pd.merge(
            users_subscriptions_df,
            subscriptions_df[['id', 'price.recurring_interval']],
            left_on='subscription_id',
            right_on='id',
            how='left'
        )
        
        # Filter data
        excluded_subscription_ids = [260, 231, 169, 157, 140, 92, 42, 9, 7]
        excluded_user_ids = [9771, 9799, 10735, 9817, 100, 12113, 10860, 12216, 7185, 12218, 8819, 10635, 7921, 14480, 15416]
        
        df_calendesk = df_calendesk[
            ~df_calendesk['id_y'].isin(excluded_subscription_ids) &
            df_calendesk['status'].isin(['active', 'canceled']) &
            ~df_calendesk['user.id'].isin(excluded_user_ids)
        ]
        
        print(f"✓ Filtered and transformed Calendesk data: {len(df_calendesk)} rows")
        
        # Transform Calendesk data
        df_calendesk['Imię i nazwisko'] = df_calendesk['user.name'] + ' ' + df_calendesk['user.surname']
        df_calendesk['default_address_name'] = df_calendesk['user.default_address.name'].fillna('')
        df_calendesk['status'] = df_calendesk['status'].replace({'canceled': 'anulowana', 'active': 'aktywna'})
        df_calendesk['price.recurring_interval'] = df_calendesk['price.recurring_interval'].replace({'year': 'roczny', 'month': 'miesięczny'})
        
        # Select and rename columns for Calendesk
        calendesk_columns = {
            'id_x': 'ID Subskrypcji Klienta',
            'id_y': 'ID Subskrypcji',
            'status': 'Status',
            'created_at': 'Data zakupu',
            'subscription.name': 'Pakiet',
            'ends_at': 'Data wygaśnięcia',
            'canceled_at': 'Data anulowania',
            'user.id': 'ID Klienta',
            'Imię i nazwisko': 'Imię i Nazwisko Klienta',
            'user.email': 'Email',
            'price.recurring_interval': 'Typ pakietu',
            'stripe_subscription_id': 'ID Suba STRIPE',
            'user.default_address.tax_number': 'NIP',
            'default_address_name': 'Nazwa Firmy',
            'user_default_phone_e164': 'Telefon'
        }
        
        df_calendesk = df_calendesk[list(calendesk_columns.keys())].rename(columns=calendesk_columns)
        
        # Convert dates
        print("🔄 Converting dates...")
        df_calendesk['Data zakupu'] = pd.to_datetime(df_calendesk['Data zakupu']).dt.tz_localize(None) + pd.DateOffset(hours=2)
        df_calendesk['Data anulowania'] = pd.to_datetime(df_calendesk['Data anulowania'], errors='coerce').dt.tz_localize(None) + pd.DateOffset(hours=2)
        df_calendesk['Data wygaśnięcia'] = pd.to_datetime(df_calendesk['Data wygaśnięcia'], errors='coerce').dt.tz_localize(None) + pd.DateOffset(hours=2)
        
        # Update cancellation dates
        def update_cancellation_dates(df):
            mask = df['Data anulowania'].isna() & df['Data wygaśnięcia'].notna()
            df.loc[mask, 'Data anulowania'] = df['Data wygaśnięcia']
            return df, mask
        
        df_calendesk, _ = update_cancellation_dates(df_calendesk)
        print("✓ Date conversion and cancellation date update completed")
        
        # Process NIP column
        def process_nip(nip_value):
            """Convert NIP to number if it's purely numeric, otherwise keep as text"""
            if pd.isna(nip_value) or nip_value == '':
                return nip_value
            
            nip_str = str(nip_value).strip()
            
            # If it contains only digits, convert to number
            if nip_str.isdigit():
                return int(nip_str)
            
            # If it contains formatting characters (like dashes), keep as text
            return nip_str
        
        df_calendesk['NIP'] = df_calendesk['NIP'].apply(process_nip)
        
        print("✓ Calendesk data processing completed")
        
    except Exception as e:
        print(f"❌ Error processing Calendesk data: {e}")
        return
    
    # Fetch Stripe data
    print("🔄 Fetching Stripe data...")
    stripe_data = fetch_stripe_invoices_all()
    df_stripe = pd.DataFrame(stripe_data)
    
    if not df_stripe.empty:
        df_stripe['created'] = pd.to_datetime(df_stripe['created'])
        
        # Rename columns for Stripe data
        stripe_columns = {
            'id': 'ID_Faktury',
            'amount_due': 'Kwota Do Zapłaty',
            'amount_paid': 'Kwota Zapłacona',
            'amount_remaining': 'Pozostało Do Zapłaty',
            'created': 'Data Utworzenia',
            'customer': 'ID_Klienta',
            'lines_data_description': 'Pakiet',
            'plan_active': 'Sub Aktywny',
            'plan_interval': 'Okres Odnowienia',
            'subscription': 'ID_Subskrypcji',
            'attempt_count': 'Liczba Pobrań Kwoty Do Zapłaty',
            'payment_intent': 'ID_Payment_Intent',
            'status': 'Status Faktury',
            'paid': 'Faktura Opłacona'
        }
        
        df_stripe = df_stripe.rename(columns=stripe_columns)
        print(f"✓ Stripe: {len(df_stripe)} invoices")
    else:
        print("⚠ No Stripe data found")
    
    # Calculate custom columns
    print("🔄 Calculating custom columns...")
    
    # Apply custom calculations if we have Stripe data
    if not df_stripe.empty:
        df_calendesk['Invoice status in chosen month'] = df_calendesk.apply(
            lambda row: calculate_invoice_status_chosen_month(row, df_stripe), axis=1
        )
        df_calendesk['Invoice status in last 2 months'] = df_calendesk.apply(
            lambda row: calculate_invoice_status_last_2_months(row, df_stripe), axis=1
        )
        df_calendesk['Last invoice month'] = df_calendesk.apply(
            lambda row: calculate_last_invoice_month(row, df_stripe), axis=1
        )
    else:
        # Add empty columns if no Stripe data
        df_calendesk['Invoice status in chosen month'] = ""
        df_calendesk['Invoice status in last 2 months'] = ""
        df_calendesk['Last invoice month'] = ""
    
    # Calculate Status3 column (no Stripe data needed)
    df_calendesk['Status3'] = df_calendesk.apply(calculate_status3, axis=1)
    
    print("✓ Custom columns calculated")
    
    # Fetch and append CRM data
    print("\n🔄 Fetching and processing CRM data...")
    try:
        crm_nip_list = fetch_crm_data()
        df_crm = create_crm_dataframe(crm_nip_list)
        
        if not df_crm.empty:
            # Append CRM data to Calendesk data
            print(f"🔄 Appending {len(df_crm)} CRM records to dataset...")
            df_calendesk = pd.concat([df_calendesk, df_crm], ignore_index=True)
            print(f"✓ Combined dataset now has {len(df_calendesk)} total records")
        else:
            print("⚠ No CRM data to append")
    except Exception as e:
        print(f"❌ Error processing CRM data: {e}")
        print("⚠ Continuing with Calendesk data only")
    
    # Update Excel file directly in SharePoint
    print("🔄 Updating Excel file directly in SharePoint...")
    
    # Update A2 cell with current date
    date_update_success = update_current_date_cell(access_token, subscriptions_sheet_name)
    
    # Update Subskrypcje klientów sheet
    subscriptions_success = update_excel_worksheet_directly(access_token, subscriptions_sheet_name, df_calendesk)
    
    # Note: No longer updating Stripe sheet - we only fetch Stripe data for calculations
    print("📊 Stripe data fetched for calculations only (not written to Excel)")
    
    if subscriptions_success and date_update_success:
        print(f"🎉 Update completed! Processed {len(df_calendesk)} subscription records.")
        print("✅ All data updated directly in SharePoint - no file lock issues!")
    else:
        print("❌ Some updates failed. Check the logs above for details.")
        return

if __name__ == "__main__":
    main()