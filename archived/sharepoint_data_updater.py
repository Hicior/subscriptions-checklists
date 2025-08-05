import os
import requests
import pandas as pd
import datetime
import time
import json
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

if not calendesk_api_key:
    print("❌ No Calendesk API key found. Please set the CALENDESK_API_KEY environment variable.")
    exit()

if not stripe_api_key:
    print("❌ No Stripe API key found. Please set the STRIPE_API_KEY environment variable.")
    exit()

if not all([azure_app_id, azure_directory_id, azure_secret_value]):
    print("❌ Missing Azure credentials. Please set ApplicationID, DirectoryID, and SecretValue environment variables.")
    exit()

print("✅ API keys and Azure credentials loaded successfully")

# SharePoint configuration
sharepoint_site_id = "slawomirmentzen.sharepoint.com,95990869-0549-4ea1-b74d-937205ea3c78,2b2b1d2f-91a9-40f1-9c02-28d6d7204e3a"
sharepoint_drive_id = "b!aQiZlUkFoU63TZNyBeo8eC8dKyupkfFAnAIo1tcgTjodLd5FMwyAS7vEmsg7KfFs"
sharepoint_file_id = "01AXKB4J2RXU66BYASKJEYCOLJPXYVUUQF"  # Updated file ID from diagnostic (2025-08-04)

# Sheet names
calendesk_sheet_name = 'CalendeskSubs'
stripe_sheet_name = 'StripeInvoices'

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
    """Clear data from a worksheet (keeping headers)"""
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    
    # Get worksheet info to find data range
    worksheet_url = f"https://graph.microsoft.com/v1.0/sites/{sharepoint_site_id}/drives/{sharepoint_drive_id}/items/{sharepoint_file_id}/workbook/worksheets/{worksheet_name}/usedRange"
    
    response = requests.get(worksheet_url, headers=headers)
    if response.status_code == 200:
        range_info = response.json()
        row_count = range_info.get('rowCount', 0)
        
        if row_count > 1:  # More than just headers
            # Define the range to clear
            if column_range:
                # Clear specific column range (e.g., A2:R1000 for CalendeskSubs)
                clear_range = f"{column_range}2:{column_range.split(':')[1]}{row_count}"
            else:
                # Clear all columns from row 2 onwards
                clear_range = f"2:{row_count}"
            
            clear_url = f"https://graph.microsoft.com/v1.0/sites/{sharepoint_site_id}/drives/{sharepoint_drive_id}/items/{sharepoint_file_id}/workbook/worksheets/{worksheet_name}/range(address='{clear_range}')/clear"
            clear_response = requests.post(clear_url, headers=headers, json={"applyTo": "Contents"})
            return clear_response.status_code == 200
    
    return True  # If no data to clear, consider it successful

def update_excel_worksheet_directly(access_token, worksheet_name, data_df):
    """Update Excel worksheet directly in SharePoint using Graph API"""
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    
    print(f"📝 Updating {worksheet_name} directly in SharePoint...")
    
    # Clear existing data first - only A:R columns for CalendeskSubs
    if worksheet_name == 'CalendeskSubs':
        clear_excel_worksheet(access_token, worksheet_name, "A:R")
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
    
    end_col = get_column_letter(num_cols)
    range_address = f"A2:{end_col}{num_rows + 1}"
    
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

# =============================================================================
# UTILITY FUNCTIONS (Fetch all data)
# =============================================================================

def fetch_calendesk_data_all(url, headers, description="Fetching data"):
    """Fetch all data from a Calendesk API endpoint"""
    all_data = []
    current_page = 1
    
    print(f"🔄 {description} - fetching all pages...")
    with tqdm(desc=description, unit="page") as pbar:
        while True:
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
    
    print(f"✓ Fetched {len(all_data)} records from {current_page-1} pages")
    return all_data

def fetch_stripe_invoices_all():
    """Fetch all Stripe invoices with pagination"""
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
    
    print("🔄 Fetching Stripe invoices - all pages...")
    with tqdm(desc="Fetching Stripe invoices") as pbar:
        while True:
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
    
    print(f"✓ Fetched {len(processed_data)} invoices from {pages_fetched} pages")
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
    
    print("✓ Custom columns calculated")
    
    # Update Excel file directly in SharePoint
    print("🔄 Updating Excel file directly in SharePoint...")
    
    # Update Calendesk sheet
    calendesk_success = update_excel_worksheet_directly(access_token, calendesk_sheet_name, df_calendesk)
    
    # Update Stripe sheet
    stripe_success = True
    if not df_stripe.empty:
        stripe_success = update_excel_worksheet_directly(access_token, stripe_sheet_name, df_stripe)
    else:
        print("⚠ No Stripe data to write")
    
    if calendesk_success and stripe_success:
        print(f"🎉 Update completed! Processed {len(df_calendesk)} Calendesk and {len(df_stripe) if not df_stripe.empty else 0} Stripe records.")
        print("✅ All data updated directly in SharePoint - no file lock issues!")
    else:
        print("❌ Some updates failed. Check the logs above for details.")
        return

if __name__ == "__main__":
    main()