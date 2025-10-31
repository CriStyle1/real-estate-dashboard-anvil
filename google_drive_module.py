# google_drive_module.py

import os.path
import re
import io
import json
import asyncio
import pytz
import time
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

BUCHAREST_TZ = pytz.timezone("Europe/Bucharest")

# --- CONFIGURATION ---
SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.file"
]
TOKEN_FILE = "token.json"
CREDENTIALS_FILE = "credentials.json"

# Load sensitive configuration from a separate file
try:
    # Try to load from Streamlit secrets first (for cloud deployment)
    import streamlit as st
    UTILITIES_PARENT_FOLDER_ID = st.secrets["UTILITIES_PARENT_FOLDER_ID"]
    CLIENTS_PARENT_FOLDER_ID = st.secrets["CLIENTS_PARENT_FOLDER_ID"]
    print("Loaded folder IDs from Streamlit secrets.")
except:
    # Fallback to local config.json file (for running on your PC)
    print("Could not load from Streamlit secrets, falling back to local config.json.")
    try:
        with open('config.json', 'r') as f:
            config = json.load(f)
        UTILITIES_PARENT_FOLDER_ID = config.get("UTILITIES_PARENT_FOLDER_ID")
        CLIENTS_PARENT_FOLDER_ID = config.get("CLIENTS_PARENT_FOLDER_ID")
        if not UTILITIES_PARENT_FOLDER_ID or not CLIENTS_PARENT_FOLDER_ID:
            raise ValueError("Folder IDs are missing from config.json")
    except (FileNotFoundError, ValueError) as e:
        print(f"CRITICAL ERROR: Could not load configuration. Error: {e}")
        exit()

class GoogleSheet:
    def __init__(self, spreadsheet_name: str, sheet_names: list):
        """
        Initializes the GoogleSheet manager for multiple sheets.
        """
        self.spreadsheet_name = spreadsheet_name
        self.sheet_names = sheet_names
        self.creds = self._get_credentials()
        self.drive_service = self._get_drive_service()
        self.sheets_service = self._get_sheets_service()
        self.spreadsheet_id = None
        self.last_modified_time = None
        self.all_data = {}
        self.sheet_data_timestamp = None

        # --- NEW: CACHING PROPERTIES ---
        self.folder_cache = {} # Will store {parent_id: [list_of_folders]}
        self.folder_cache_timestamp = None    
        
        # --- NEW PERSISTENT SETTINGS LOGIC ---
        self.settings_file = "drive_module_settings.json"
        self.settings = self._load_settings()
        print("Initialized with settings:", self.settings)

    def _load_settings(self) -> dict:
        """Loads settings from the JSON file, or returns defaults."""
        defaults = {
            "gas_tariff": 3.37,
            "electricity_tariff": 1.68,
            "internet_fee_monthly": 35.0,
            "eur_to_ron_rate": 5.1
        }
        if os.path.exists(self.settings_file):
            try:
                with open(self.settings_file, 'r') as f:
                    settings = json.load(f)
                    # Ensure all default keys are present
                    defaults.update(settings)
                    return defaults
            except (json.JSONDecodeError, TypeError):
                print(f"Warning: Could not read '{self.settings_file}'. Using default settings.")
                return defaults
        return defaults

    def save_settings(self):
        """Saves the current settings to the JSON file."""
        try:
            with open(self.settings_file, 'w') as f:
                json.dump(self.settings, f, indent=4)
            print("[SUCCESS] Settings saved successfully.")
        except Exception as e:
            print(f"[ERROR] Error saving settings: {e}")
            self._log_error_to_drive(str(e), "save_settings")

    def sync_exchange_rate(self, silent: bool = False):
        """
        Fetches the latest EUR to RON exchange rate from cursbnr.ro,
        adds a margin, and updates the settings if the rate has changed.
        """
        if not silent:
            print("\n--- [Exchange Rate Sync] ---")
        try:
            url = "https://www.cursbnr.ro/"
            if not silent:
                print(f"Fetching latest exchange rate from {url}...")
            
            response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')
            eur_row = soup.find('td', string='EUR').find_parent('tr')
            rate_cell = eur_row.find_all('td')[2]
            base_rate = float(rate_cell.text.strip())
            rate_with_margin = round(base_rate * 1.003, 4) # Using 0.3% margin as per previous logic

            # --- NEW: ONLY UPDATE IF THE RATE IS DIFFERENT ---
            current_rate = self.settings.get("eur_to_ron_rate")
            if current_rate != rate_with_margin:
                print(f"  -> New exchange rate found! Official: {base_rate}, With Margin: {rate_with_margin}")
                print(f"  -> Updating setting from {current_rate} to {rate_with_margin}")
                self.settings["eur_to_ron_rate"] = rate_with_margin
                self.save_settings()
                return True, f"[SUCCESS] Exchange rate updated to {self.settings['eur_to_ron_rate']}."
            else:
                if not silent:
                    print(f"  -> Exchange rate is already up-to-date ({current_rate}). No changes made.")
                return True, f"[SUCCESS] Exchange rate is already up-to-date ({self.settings['eur_to_ron_rate']})."

        except Exception as e:
            error_message = f"[ERROR] Failed to sync exchange rate: {e}"
            print(error_message)
            self._log_error_to_drive(str(e), "sync_exchange_rate")
            return False, error_message

    def _get_credentials(self):
        creds = None
        if os.path.exists(TOKEN_FILE):
            creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
                creds = flow.run_local_server(port=0)
            with open(TOKEN_FILE, "w") as token:
                token.write(creds.to_json())
        return creds

    def _get_drive_service(self):
        try:
            return build("drive", "v3", credentials=self.creds)
        except HttpError as error:
            print(f"An error occurred while building the Drive service: {error}")
            return None

    def _get_sheets_service(self):
        try:
            return build("sheets", "v4", credentials=self.creds)
        except HttpError as error:
            print(f"An error occurred while building the Sheets service: {error}")
            return None
    
    def _log_error_to_drive(self, error_message: str, function_name: str):
        """
        Logs an error message to a file named 'dashboard_error_log.txt' in the user's root Google Drive.
        It prepends the new error to the top of the file.
        """
        if not self.drive_service:
            print("CRITICAL: Drive service not available. Cannot log error to Drive.")
            return

        log_file_name = "dashboard_error_log.txt"
        log_file_id = None
        
        try:
            # 1. Find the log file
            query = f"name = '{log_file_name}' and 'root' in parents and trashed = false"
            results = self.drive_service.files().list(q=query, fields="files(id)").execute()
            items = results.get("files", [])
            if items:
                log_file_id = items[0]["id"]
            
            # 2. Prepare the new log entry
            timestamp = datetime.now(BUCHAREST_TZ).strftime('%Y-%m-%d %H:%M:%S')
            new_log_entry = f"--- ERROR ---\nTimestamp: {timestamp}\nFunction: {function_name}\nError: {error_message}\n\n"

            # 3. Get existing content or start fresh
            existing_content = ""
            if log_file_id:
                request = self.drive_service.files().get_media(fileId=log_file_id)
                fh = io.BytesIO()
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while not done:
                    _, done = downloader.next_chunk()
                fh.seek(0)
                existing_content = fh.read().decode('utf-8')

            full_content = new_log_entry + existing_content

            # 4. Write the updated content back to Drive
            from googleapiclient.http import MediaIoBaseUpload
            fh = io.BytesIO(full_content.encode('utf-8'))
            media_body = MediaIoBaseUpload(fh, mimetype='text/plain', resumable=True)

            if log_file_id:
                # Update existing file
                self.drive_service.files().update(fileId=log_file_id, media_body=media_body).execute()
                print(f"✅ Successfully appended error to '{log_file_name}' in Google Drive.")
            else:
                # Create new file
                file_metadata = {'name': log_file_name}
                self.drive_service.files().create(body=file_metadata, media_body=media_body, fields='id').execute()
                print(f"✅ Successfully created '{log_file_name}' and logged error in Google Drive.")

        except Exception as e:
            # If logging itself fails, just print to the console to avoid a loop
            print(f"--- CRITICAL LOGGING FAILURE ---")
            print(f"Failed to write to '{log_file_name}' in Google Drive.")
            print(f"Original Error was in function '{function_name}': {error_message}")
            print(f"Logging Error: {e}")

    def _find_spreadsheet_id(self):
        if not self.drive_service: return None
        try:
            # MODIFIED: Ask for the 'id' and 'modifiedTime' of the file
            query = f"name = '{self.spreadsheet_name}' and trashed = false"
            results = self.drive_service.files().list(
                q=query, pageSize=1, fields="files(id, modifiedTime)"
            ).execute()
            items = results.get("files", [])
            if items:
                file_metadata = items[0]
                self.spreadsheet_id = file_metadata["id"]
                self.last_modified_time = file_metadata["modifiedTime"] # Save the timestamp
                print(f"Found spreadsheet '{self.spreadsheet_name}' with ID: {self.spreadsheet_id}")
                print(f"  -> Last modified at: {self.last_modified_time}")
                return self.last_modified_time
            else:
                print(f"Error: Spreadsheet '{self.spreadsheet_name}' not found.")
                return None
        except HttpError as error:
            print(f"An error occurred while searching for the spreadsheet: {error}")
            self._log_error_to_drive(str(error), "_find_spreadsheet_id")
            return None
    
    def reload_all_data(self):
        """
        Forces a full refresh of all data by clearing all caches and then
        re-loading the sheet data from the Google API.
        """
        print("\n--- [Data Refresh] Forcing a full refresh of all data... ---")
        
        # Clear all in-memory caches
        self.sheet_data_timestamp = None
        self.folder_cache = {}
        self.folder_cache_timestamp = None
        print("--- [Data Refresh] All caches have been cleared. ---")

        # Re-run the find function to get the latest spreadsheet ID
        self._find_spreadsheet_id() 
        
        # Reload all the sheet data from the API
        self.load_data()
        
        print("--- [Data Refresh] Refresh complete. ---")
        # Return a simple success message
        return True, "✅ All sheet data has been force-refreshed!"

    def load_data(self):
        if self.sheet_data_timestamp and datetime.now(pytz.utc) - self.sheet_data_timestamp.total_seconds() < 600: # 10 minute cache
            print("INFO: [Cache] Using fresh sheet data from memory.")
            return
        
        if not self.spreadsheet_id:
            self._find_spreadsheet_id()
        if not self.spreadsheet_id or not self.sheets_service:
            return
        try:
            print(f"Requesting data for sheets: {self.sheet_names}")
            result = self.sheets_service.spreadsheets().values().batchGet(
                spreadsheetId=self.spreadsheet_id,
                ranges=self.sheet_names
            ).execute()
            value_ranges = result.get('valueRanges', [])
            for value_range in value_ranges:
                sheet_name = value_range['range'].split('!')[0]
                all_values = value_range.get('values', [])
                if not all_values:
                    print(f"No data found in sheet '{sheet_name}'.")
                    continue
                header_row_index = -1
                if sheet_name == "APARTMENTS":
                    for i, row in enumerate(all_values[:10]):
                        if row and "AP CODE" in [str(c).strip().upper() for c in row]:
                            header_row_index = i
                            break
                else:
                    for i, row in enumerate(all_values[:10]):
                        if row and any(str(c).strip() for c in row):
                            header_row_index = i
                            break
                if header_row_index == -1:
                    print(f"CRITICAL ERROR: Could not find a valid header row in sheet '{sheet_name}'.")
                    continue
                self.all_data[sheet_name] = {
                    'header': all_values[header_row_index],
                    'data': all_values[header_row_index + 1:]
                }
                print(f"Successfully loaded {len(self.all_data[sheet_name]['data'])} rows from '{sheet_name}'.")
            self.sheet_data_timestamp = datetime.now(pytz.utc)

        except HttpError as error:
            print(f"An error occurred while reading the sheets: {error}")
            self._log_error_to_drive(str(error), "load_data")

    def reload_specific_sheet(self, sheet_name: str):
        """
        Fetches and updates the data for only one specific sheet.
        """
        if not self.spreadsheet_id or not self.sheets_service:
            print(f"ERROR: Cannot reload '{sheet_name}', services not initialized.")
            return False, f"❌ Services not ready."
        
        print(f"--- [Targeted Refresh] Requesting data for sheet: {sheet_name} ---")
        try:
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=self.spreadsheet_id,
                range=sheet_name
            ).execute()
            
            all_values = result.get('values', [])
            if not all_values:
                print(f"No data found in sheet '{sheet_name}'.")
                return False, f"ℹ️ No data in '{sheet_name}'."

            # Use the same header-finding logic as load_data
            header_row_index = -1
            for i, row in enumerate(all_values[:10]):
                if row and any(str(c).strip() for c in row):
                    header_row_index = i
                    break
            
            if header_row_index == -1:
                msg = f"CRITICAL ERROR: Could not find a valid header row in sheet '{sheet_name}'."
                print(msg)
                self._log_error_to_drive(msg, "reload_specific_sheet")
                return False, f"❌ No header in '{sheet_name}'."

            # Update the data for this specific sheet in our in-memory dictionary
            self.all_data[sheet_name] = {
                'header': all_values[header_row_index],
                'data': all_values[header_row_index + 1:]
            }
            print(f"✅ Successfully reloaded {len(self.all_data[sheet_name]['data'])} rows from '{sheet_name}'.")
            return True, f"✅ '{sheet_name}' data refreshed!"

        except HttpError as error:
            print(f"An error occurred while reloading '{sheet_name}': {error}")
            self._log_error_to_drive(str(error), f"reload_specific_sheet: {sheet_name}")
            return False, f"❌ Error reloading '{sheet_name}'."

    def get_row_by_code(self, ap_code: str, sheet_name: str, code_column_name: str) -> dict | None:
        if sheet_name not in self.all_data:
            print(f"Error: Data for sheet '{sheet_name}' is not loaded.")
            return None
        sheet_info = self.all_data[sheet_name]
        header, data = sheet_info['header'], sheet_info['data']
        try:
            code_column_index = [h.strip().upper() for h in header].index(code_column_name.strip().upper())
        except ValueError:
            print(f"Error: Column '{code_column_name}' not found in the header of sheet '{sheet_name}'.")
            return None
        for row in data:
            if len(row) > code_column_index and row[code_column_index].strip().upper() == ap_code.strip().upper():
                return {header[i]: (row[i] if i < len(row) else None) for i, h in enumerate(header)}
        return None

    def get_apartment_data(self, ap_code: str, sheet_name: str = "APARTMENTS") -> dict | None:
        return self.get_row_by_code(ap_code, sheet_name, "AP CODE")

    def format_apartment_info(self, ap_code: str) -> str | None:
        apartment_data = self.get_apartment_data(ap_code)
        if not apartment_data:
            return f"❌ Apartment with code `{ap_code}` not found in the APARTMENTS sheet."
        columns_to_display = [
            "AP CODE", "ADDRESS", "NR", "REALTOR", "PHONE_CL", "E-MAIL", "CONTRACT",
            "START", "END", "CONTRACT STATUS_CL", "ARENDA CLIENT", "RENT SUGGESTION",
            "DEPOSIT", "UTILITIES", "ELECTRICITY PRICE", "GAS PRICE", "INTERNET PRICE",
            "ADM PRICE", "DAYS_UT", "UTILITY TOTAL PRICE", "TOTAL PAY", "UPDATED DATE",
            "NOTES", "LAST PAY"
        ]
        report_parts = [f"📊 Apartment Details for {ap_code}\n"]
        for column_name in columns_to_display:
            value = apartment_data.get(column_name, "N/A")
            if not value or str(value).strip() == "": value = "N/A"
            report_parts.append(f"• {column_name}: {value}")
        return "\n".join(report_parts)

    def get_apartments_by_realtor(self, realtor_name: str) -> list:
        sheet_name = "APARTMENTS"
        if sheet_name not in self.all_data: return []
        sheet_info = self.all_data[sheet_name]
        header, data = sheet_info['header'], sheet_info['data']
        try:
            code_column_index = [h.upper() for h in header].index("AP CODE")
            realtor_column_index = [h.upper() for h in header].index("REALTOR")
        except ValueError as e:
            print(f"Error: A required column was not found: {e}")
            return []
        found_codes = []
        for row in data:
            if len(row) > realtor_column_index and len(row) > code_column_index:
                if row[realtor_column_index].strip().upper() == realtor_name.strip().upper():
                    found_codes.append(row[code_column_index])
        if not found_codes:
            print(f"No apartments found for realtor '{realtor_name}'.")
        return found_codes

    def generate_upcoming_events_report(self, realtor_name: str, upcoming_days: int = 5, contract_end_warning_days: int = 30) -> str:
        """
        Generates a report of upcoming payments and contract endings, now including
        the details and type of the last known payment for each apartment.
        """
        apartments_sheet = "APARTMENTS"
        mo_data_sheet = "MO_DATA"
        if apartments_sheet not in self.all_data or mo_data_sheet not in self.all_data:
            return f"❌ Data for required sheets (APARTMENTS, MO_DATA) is not loaded."

        ap_sheet_info = self.all_data[apartments_sheet]
        ap_header, ap_data = ap_sheet_info['header'], ap_sheet_info['data']
        
        mo_sheet_info = self.all_data[mo_data_sheet]
        mo_header, mo_data = mo_sheet_info['header'], mo_sheet_info['data']

        try:
            ap_header_upper = [str(h).strip().upper() for h in ap_header]
            ap_code_col = ap_header_upper.index("AP CODE")
            realtor_col = ap_header_upper.index("REALTOR")
            start_date_col = ap_header_upper.index("START")
            end_date_col = ap_header_upper.index("END")
            rent_col = ap_header_upper.index("ARENDA CLIENT")

            mo_header_upper = [str(h).strip().upper() for h in mo_header]
            mo_code_col = mo_header_upper.index("APARTMENT CODE")
            mo_date_col = mo_header_upper.index("SUBMISSION DATE")
            mo_amount_col = mo_header_upper.index("AMOUNT")
            mo_currency_col = mo_header_upper.index("CURRENCY")
            # --- ADD THIS LINE TO GET THE TASK TYPE COLUMN ---
            mo_task_col = mo_header_upper.index("TYPE OF MONEY TASK")

        except ValueError as e:
            return f"❌ A required column was not found in the headers: {e}"

        transactions_by_code = {}
        for row in mo_data:
            if len(row) > mo_code_col and row[mo_code_col]:
                code = row[mo_code_col].strip().upper()
                if code not in transactions_by_code:
                    transactions_by_code[code] = []
                transactions_by_code[code].append(row)

        today = datetime.now(pytz.utc)
        upcoming_payments = []
        ending_contracts = []

        for row in ap_data:
            def get_cell(r, index, default=""): return r[index] if len(r) > index and r[index] else default

            if get_cell(row, realtor_col).strip().lower() != realtor_name.strip().lower():
                continue
            ap_code = get_cell(row, ap_code_col)
            if not ap_code: continue

            start_date_str = get_cell(row, start_date_col)
            try:
                contract_start_date = datetime.strptime(start_date_str, '%d-%m-%Y')
                payment_day = contract_start_date.day
                is_upcoming = any((today + timedelta(days=i)).day == payment_day for i in range(upcoming_days + 1))
                
                if is_upcoming:
                    rent_amount = get_cell(row, rent_col, "N/A")
                    
                    last_payment_info = "No payment history"
                    if ap_code in transactions_by_code:
                        def parse_mo_date(date_str):
                            try: return datetime.strptime(date_str, '%d/%m/%Y')
                            except ValueError: return datetime.min
                        
                        transactions_by_code[ap_code].sort(key=lambda r: parse_mo_date(get_cell(r, mo_date_col)), reverse=True)
                        
                        latest_transaction = transactions_by_code[ap_code][0]
                        last_pay_date = get_cell(latest_transaction, mo_date_col)
                        last_pay_amount = get_cell(latest_transaction, mo_amount_col)
                        last_pay_currency = get_cell(latest_transaction, mo_currency_col)
                        # --- GET THE TASK TYPE ---
                        last_pay_task = get_cell(latest_transaction, mo_task_col)
                        
                        # --- THIS IS THE NEW, CLEANER STRING ---
                        last_payment_info = f"{last_pay_amount} {last_pay_currency} on {last_pay_date} ({last_pay_task})"
                    
                    upcoming_payments.append({
                        "code": ap_code, "payment_day": payment_day,
                        "rent": rent_amount, "last_payment": last_payment_info
                    })
            except ValueError:
                pass

            end_date_str = get_cell(row, end_date_col)
            try:
                contract_end_date = datetime.strptime(end_date_str, '%d-%m-%Y')
                days_until_end = (contract_end_date - today).days
                if 0 <= days_until_end <= contract_end_warning_days:
                    ending_contracts.append({
                        "code": ap_code, "end_date": contract_end_date.strftime('%d-%b-%Y'),
                        "days_left": days_until_end
                    })
            except ValueError:
                pass

        # --- STEP 4: ASSEMBLE THE FINAL REPORT ---
        report_parts = [f"🗓️ **Upcoming Events Report for `{realtor_name}`**\n"]

        if upcoming_payments:
            report_parts.append("💰 Upcoming Payments (Next 5 Days):")
            upcoming_payments.sort(key=lambda x: x['payment_day'])
            for item in upcoming_payments:
                # --- THIS IS THE NEW, COMPACT SINGLE-LINE FORMAT ---
                report_parts.append(
                    f"  • {item['code']} - Pay Day: {item['payment_day']} (Rent: {item['rent']} EUR) (Last: {item['last_payment']})"
                )
        else:
            report_parts.append("✅ No payments due in the next 5 days.")

        report_parts.append("") # Spacer

        if ending_contracts:
            report_parts.append("❗ **Contracts Ending Soon (Next 30 Days):**")
            ending_contracts.sort(key=lambda x: x['days_left'])
            for item in ending_contracts:
                # Also make this a clean single line
                report_parts.append(f"  • `{item['code']}` - Ends on **{item['end_date']}** ({item['days_left']} days left)")
        else:
            report_parts.append("✅ No contracts are ending in the next 30 days.")

        return "\n".join(report_parts)

    def generate_upcoming_payments_report(self, realtor_name: str, upcoming_days: int) -> str:
        """
        Generates a clean, text-based report of upcoming rent payments for a specific realtor.
        Includes the date of the last utility reading for context.
        """
        apartments_sheet = "APARTMENTS"
        ut_data_sheet = "UT_DATA"
        if apartments_sheet not in self.all_data or ut_data_sheet not in self.all_data:
            return f"Error: Data for required sheets (APARTMENTS, UT_DATA) is not loaded."

        ap_sheet_info = self.all_data[apartments_sheet]
        ap_header, ap_data = ap_sheet_info['header'], ap_sheet_info['data']
        
        ut_sheet_info = self.all_data[ut_data_sheet]
        ut_header, ut_data = ut_sheet_info['header'], ut_sheet_info['data']

        try:
            # Find columns for APARTMENTS sheet
            ap_header_upper = [str(h).strip().upper() for h in ap_header]
            ap_code_col = ap_header_upper.index("AP CODE")
            realtor_col = ap_header_upper.index("REALTOR")
            start_date_col = ap_header_upper.index("START")
            rent_col = ap_header_upper.index("ARENDA CLIENT")

            # Find columns for UT_DATA sheet
            ut_header_upper = [str(h).strip().upper() for h in ut_header]
            ut_code_col = ut_header_upper.index("APARTMENT CODE")
            ut_date_col = ut_header_upper.index("DATE OF READING")
        except ValueError as e:
            return f"Error: A required column was not found in the headers: {e}"

        # --- STEP 1: Pre-process UT_DATA to find the latest reading for each code ---
        latest_ut_dates = {}
        def parse_ut_date(date_str):
            try: return datetime.strptime(date_str, '%d-%m-%Y')
            except ValueError:
                try: return datetime.strptime(date_str, '%d/%m/%Y %H:%M:%S')
                except ValueError: return datetime.min
        
        for row in ut_data:
            if len(row) > ut_code_col and row[ut_code_col]:
                code = row[ut_code_col].strip().upper()
                date_str = row[ut_date_col] if len(row) > ut_date_col else ''
                date_obj = parse_ut_date(date_str)
                
                if code not in latest_ut_dates or date_obj > latest_ut_dates[code]['date_obj']:
                    latest_ut_dates[code] = {'date_str': date_str, 'date_obj': date_obj}

        # --- STEP 2: Process APARTMENTS and find upcoming payments ---
        today = datetime.now(pytz.utc)
        upcoming_payments = []

        for row in ap_data:
            def get_cell(r, index, default=""): return r[index] if len(r) > index and r[index] else default

            if get_cell(row, realtor_col).strip().lower() != realtor_name.strip().lower():
                continue
            ap_code = get_cell(row, ap_code_col)
            if not ap_code: continue

            start_date_str = get_cell(row, start_date_col)
            try:
                contract_start_date = datetime.strptime(start_date_str, '%d-%m-%Y')
                payment_day = contract_start_date.day
                
                is_upcoming = any((today + timedelta(days=i)).day == payment_day for i in range(upcoming_days + 1))
                
                if is_upcoming:
                    rent_amount = get_cell(row, rent_col, "N/A")
                    last_ut_date = latest_ut_dates.get(ap_code, {}).get('date_str', "No Reading Found")
                    
                    upcoming_payments.append({
                        "code": ap_code,
                        "payment_day": payment_day,
                        "rent": rent_amount,
                        "last_ut_date": last_ut_date
                    })
            except ValueError:
                pass

        # --- STEP 3: ASSEMBLE THE FINAL, CLEAN TEXT REPORT ---
        report_parts = [
            f"--- Upcoming Payments Report for {realtor_name} ---",
            f"--- Window: Next {upcoming_days} days ---\n"
        ]
        
        if upcoming_payments:
            upcoming_payments.sort(key=lambda x: x['payment_day'])
            for item in upcoming_payments:
                report_parts.append(f"Apartment Code: {item['code']}")
                report_parts.append(f"  - Pay Day:        {item['payment_day']} of the month")
                report_parts.append(f"  - Rent Amount:    {item['rent']} EUR")
                report_parts.append(f"  - Last UT Reading: {item['last_ut_date']}")
                report_parts.append("-" * 20) # Separator
        else:
            report_parts.append("No payments due in the specified window.")

        return "\n".join(report_parts)

    def find_ap_code_by_email(self, email: str) -> str | None:
        """
        Finds an apartment code by searching for a client's email address.
        It performs a cross-reference check with the CL_DATA sheet for verification.

        Args:
            email: The client's email address to search for (case-insensitive).

        Returns:
            The found AP CODE as a string, or a message if not found or if there's a mismatch.
        """
        search_email = email.strip().lower()
        
        # --- STEP 1: Search the APARTMENTS sheet ---
        apartments_sheet = "APARTMENTS"
        if apartments_sheet not in self.all_data:
            return f"❌ Data for sheet '{apartments_sheet}' is not loaded."

        sheet_info = self.all_data[apartments_sheet]
        header = sheet_info['header']
        data = sheet_info['data']

        try:
            header_upper = [str(h).strip().upper() for h in header]
            code_col = header_upper.index("AP CODE")
            email_col = header_upper.index("E-MAIL")
            cl_code_col = header_upper.index("CL_CODE") # We need the client code for verification
        except ValueError as e:
            return f"❌ A required column was not found in the '{apartments_sheet}' header: {e}"

        found_apartment_row = None
        for row in data:
            if len(row) > email_col and row[email_col].strip().lower() == search_email:
                found_apartment_row = row
                break
        
        if not found_apartment_row:
            return f"ℹ️ No active contract found for email '{email}' in the {apartments_sheet} sheet."

        ap_code = found_apartment_row[code_col]
        cl_code = found_apartment_row[cl_code_col] if len(found_apartment_row) > cl_code_col else None

        if not cl_code:
            return f"⚠️ Found apartment `{ap_code}` for email '{email}', but no CL_CODE was present for verification."

        # --- STEP 2: Verify with the CL_DATA sheet ---
        cl_data_sheet = "CL_DATA"
        if cl_data_sheet not in self.all_data:
            return f"⚠️ Found apartment `{ap_code}`, but could not verify in '{cl_data_sheet}' (sheet not loaded)."

        cl_sheet_info = self.all_data[cl_data_sheet]
        cl_header = cl_sheet_info['header']
        cl_data = cl_sheet_info['data']

        try:
            cl_header_upper = [str(h).strip().upper() for h in cl_header]
            def find_col_by_keyword(keyword):
                for i, h in enumerate(cl_header_upper):
                    if keyword in h: return i
                raise ValueError(f"'{keyword}'")
            
            cl_data_code_col = find_col_by_keyword("CL_CODE")
            cl_data_email_col = find_col_by_keyword("EMAIL") # Now flexibly searches for "EMAIL"
        except ValueError as e:
            return f"⚠️ Found apartment `{ap_code}`, but a required column containing {e} was not found in '{cl_data_sheet}'."

        verification_passed = False
        for cl_row in cl_data:
            if len(cl_row) > cl_data_code_col and cl_row[cl_data_code_col].strip() == cl_code:
                # Found the matching client in CL_DATA. Now check the email.
                if len(cl_row) > cl_data_email_col and cl_row[cl_data_email_col].strip().lower() == search_email:
                    verification_passed = True
                    break
                else:
                    # Emails do not match! This is a data integrity issue.
                    email_in_cl_data = cl_row[cl_data_email_col] if len(cl_row) > cl_data_email_col else "EMPTY"
                    return (f"❌ **Data Mismatch!**\n"
                            f"  - Found apartment `{ap_code}` for email '{email}'.\n"
                            f"  - However, the corresponding client code `{cl_code}` in `CL_DATA` has a different email: `{email_in_cl_data}`.")
        
        if verification_passed:
            return f"✅ **Success!**\n  - Email: `{email}`\n  - Verified Client Code: `{cl_code}`\n  - Found Apartment Code: **`{ap_code}`**"
        else:
            return f"⚠️ Found apartment `{ap_code}` for email '{email}', but could not find a matching client with code `{cl_code}` in the `CL_DATA` sheet."

    def find_email_by_ap_code(self, ap_code: str) -> str | None:
        """
        Finds a client's email address by searching for an apartment code.
        It cross-references with CL_DATA for the most reliable email.
        """
        search_code = ap_code.strip().upper()
        
        # --- STEP 1: Find the CL_CODE in the APARTMENTS sheet ---
        apartment_details = self.get_apartment_data(search_code, sheet_name="APARTMENTS")
        if not apartment_details:
            return f"ℹ️ No active contract found for apartment code `{search_code}` in the APARTMENTS sheet."

        cl_code = apartment_details.get("CL_CODE")
        if not cl_code:
            # Fallback: Check the E-MAIL column in the APARTMENTS sheet directly
            email_in_apartments = apartment_details.get("E-MAIL")
            if email_in_apartments:
                return f"✅ Found Email (from APARTMENTS sheet): `{email_in_apartments}`\n  (No CL_CODE was present for cross-referencing)."
            return f"⚠️ Found apartment `{search_code}`, but it has no CL_CODE or E-MAIL listed."

        # --- STEP 2: Use the CL_CODE to find the definitive email in CL_DATA ---
        cl_data_sheet = "CL_DATA"
        if cl_data_sheet not in self.all_data:
            return f"⚠️ Found apartment `{search_code}`, but could not look up email in '{cl_data_sheet}' (sheet not loaded)."

        client_row = self.get_row_by_code(cl_code, sheet_name="CL_DATA", code_column_name="CL_CODE")
        
        if not client_row:
            return f"⚠️ Found apartment `{search_code}` with client code `{cl_code}`, but this client was not found in the `CL_DATA` sheet."

        email_in_cl_data = client_row.get("Email Address")
        if email_in_cl_data:
            return f"✅ **Success!**\n  - Apartment Code: `{search_code}`\n  - Verified Client Code: `{cl_code}`\n  - Found Email: **`{email_in_cl_data}`**"
        else:
            return f"ℹ️ Found client `{cl_code}` for apartment `{search_code}`, but their email is not listed in `CL_DATA`."


    def format_ut_data(self, ap_code: str) -> str | None:
        """
        Finds the latest utility readings and formats a report, including Drive links
        and now also showing the meter readings.
        """
        sheet_name = "UT_DATA"
        if sheet_name not in self.all_data:
            return f"❌ Data for sheet '{sheet_name}' is not loaded."

        sheet_info = self.all_data[sheet_name]
        header, data = sheet_info['header'], sheet_info['data']
        try:
            header_upper = [str(h).strip().upper() for h in header]
            def find_col_by_keyword(keyword):
                for i, h in enumerate(header_upper):
                    if keyword in h: return i
                raise ValueError(f"'{keyword}'")
            code_col, date_col, elec_col, gas_col = (
                find_col_by_keyword("APARTMENT CODE"), find_col_by_keyword("DATE OF READING"),
                find_col_by_keyword("ELECTRICITY"), find_col_by_keyword("GAS")
            )
        except ValueError as e:
            return f"❌ A required column containing {e} was not found in the '{sheet_name}' header."

        relevant_rows = [row for row in data if len(row) > code_col and row[code_col].strip().upper() == ap_code.strip().upper()]
        
        report_parts = [f"🧾 Utility Consumption Report for `{ap_code}`\n"]

        def get_cell_value(row, index, as_type=str, default=None):
            try:
                value = row[index] if len(row) > index and row[index] else default
                if value is None: return default
                if isinstance(value, str): value = value.replace(',', '.')
                return as_type(value)
            except (ValueError, TypeError): return default

        def parse_date(date_str):
            try: return datetime.strptime(date_str, '%d-%m-%Y')
            except ValueError:
                try: return datetime.strptime(date_str, '%d/%m/%Y %H:%M:%S')
                except ValueError: return datetime.min
        
        relevant_rows.sort(key=lambda r: parse_date(r[date_col] if len(r) > date_col else ''), reverse=True)

        if len(relevant_rows) < 2:
            report_parts.append("ℹ️ **Only one utility reading found.** (Cannot calculate consumption).")
            if relevant_rows:
                latest_row = relevant_rows[0]
                date_of_reading = get_cell_value(latest_row, date_col, str, "N/A")
                report_parts.append(f"   • Date of this reading: `{date_of_reading}`")
        else:
            latest_row, previous_row = relevant_rows[0], relevant_rows[1]
            latest_elec, latest_gas = get_cell_value(latest_row, elec_col, float, 0.0), get_cell_value(latest_row, gas_col, float, 0.0)
            previous_elec, previous_gas = get_cell_value(previous_row, elec_col, float, 0.0), get_cell_value(previous_row, gas_col, float, 0.0)
            elec_consumption, gas_consumption = latest_elec - previous_elec, latest_gas - previous_gas
            latest_date_str, previous_date_str = get_cell_value(latest_row, date_col, str, "N/A"), get_cell_value(previous_row, date_col, str, "N/A")

            # --- THIS IS THE NEW, MORE DETAILED REPORT ---
            report_parts.append(f"**Comparison from `{previous_date_str}` to `{latest_date_str}`:**\n")
            report_parts.append("⚡ **Electricity:**")
            report_parts.append(f"  • Readings: `{previous_elec}` → `{latest_elec}` kWh")
            report_parts.append(f"  • Consumption: **`{elec_consumption:.2f}` kWh**\n")
            
            report_parts.append("🔥 **Gas:**")
            report_parts.append(f"  • Readings: `{previous_gas}` → `{latest_gas}`")
            report_parts.append(f"  • Consumption: **`{gas_consumption:.2f}`**")
            # --- END OF NEW REPORT ---
        
        report_parts.append("")
        report_parts.append("🔗 Recent Drive Folders:")
        all_matching_folders = self.find_all_matching_folders_sorted(ap_code)

        if not all_matching_folders:
            report_parts.append("  • No matching Drive folders found.")
        else:
            for i, folder in enumerate(all_matching_folders[:3]):
                folder_id, folder_name = folder.get('id'), folder.get('name')
                folder_link = f"https://drive.google.com/drive/folders/{folder_id}"
                folder_type = "Contract" if "CTR" in folder_name.upper() else "Utility"
                report_parts.append(f"  {i+1}. {folder_type}: [{folder_name}]({folder_link})")

        return "\n".join(report_parts)

    
    
    
    
    
    def calculate_and_format_utilities(self, ap_code: str, include_rent: bool = False):
        """
        Finds latest readings, performs a full utility calculation, and now
        also finds and includes the top 3 relevant Drive folder links.
        """
        rent_eur = None
        if include_rent:
            apartment_details = self.get_apartment_data(ap_code, sheet_name="APARTMENTS")
            if not apartment_details:
                return f"❌ Could not find apartment '{ap_code}' in APARTMENTS sheet to get rent.", None
            try:
                rent_eur = float(apartment_details.get("ARENDA CLIENT"))
            except (ValueError, TypeError):
                return f"❌ Rent value for '{ap_code}' is not a valid number in APARTMENTS sheet.", None

        sheet_name = "UT_DATA"
        if sheet_name not in self.all_data:
            return f"❌ Data for sheet '{sheet_name}' is not loaded.", None
        
        sheet_info = self.all_data[sheet_name]
        header, data = sheet_info['header'], sheet_info['data']
        try:
            header_upper = [str(h).strip().upper() for h in header]
            def find_col_by_keyword(keyword):
                for i, h in enumerate(header_upper):
                    if keyword in h: return i
                raise ValueError(f"'{keyword}'")
            code_col, date_col, elec_col, gas_col, admin_col = (
                find_col_by_keyword("APARTMENT CODE"), find_col_by_keyword("DATE OF READING"),
                find_col_by_keyword("ELECTRICITY"), find_col_by_keyword("GAS"),
                find_col_by_keyword("TOTAL AMOUNT")
            )
        except ValueError as e:
            return f"❌ A required column containing {e} was not found.", None

        def get_cell_value(row, index, as_type=str, default=None):
            try:
                value = row[index] if len(row) > index and row[index] else default
                if value is None: return default
                if isinstance(value, str): value = value.replace(',', '.')
                return as_type(value)
            except (ValueError, TypeError): return default

        # FIXED: Robust date parsing that ALWAYS returns timezone-aware dates
        def parse_and_make_aware(date_str):
            """Parse date string and ALWAYS return timezone-aware datetime."""
            if not date_str or not isinstance(date_str, str):
                return None
            
            date_str = date_str.strip()
            parsed_date = None
            
            # Try all possible formats
            formats = [
                '%d-%m-%Y',
                '%d/%m/%Y %H:%M:%S',
                '%d/%m/%Y',
                '%Y-%m-%d',
                '%Y-%m-%d %H:%M:%S'
            ]
            
            for fmt in formats:
                try:
                    parsed_date = datetime.strptime(date_str, fmt)
                    break
                except (ValueError, TypeError):
                    continue
            
            if parsed_date is None:
                return None

            # CRITICAL: Always ensure timezone awareness
            if parsed_date.tzinfo is None:
                # No timezone - add Bucharest timezone
                try:
                    return BUCHAREST_TZ.localize(parsed_date)
                except:
                    # Fallback if localize fails
                    return parsed_date.replace(tzinfo=BUCHAREST_TZ)
            else:
                # Already has timezone - convert to Bucharest
                return parsed_date.astimezone(BUCHAREST_TZ)

        relevant_rows = [row for row in data if len(row) > code_col and row[code_col].strip().upper() == ap_code.strip().upper()]

        # FIXED: Parse all rows with robust timezone handling
        parsed_rows = []
        for r in relevant_rows:
            date_str = get_cell_value(r, date_col, str, "")
            date_obj = parse_and_make_aware(date_str)
            if date_obj:
                parsed_rows.append((date_obj, r))

        if not parsed_rows:
            return f"ℹ️ No valid utility readings found for `{ap_code}`.", None

        # Sort by date (all dates are now timezone-aware, so this is safe)
        parsed_rows.sort(key=lambda x: x[0], reverse=True)

        if len(parsed_rows) < 2:
            latest_date, latest_row = parsed_rows[0]
            date_of_reading = get_cell_value(latest_row, date_col, str, "N/A")
            return f"ℹ️ Only one valid utility reading found for {ap_code} (on {date_of_reading}).", None

        latest_date, latest_row = parsed_rows[0]
        previous_date, previous_row = parsed_rows[1]
        
        prev_gas, curr_gas = get_cell_value(previous_row, gas_col, float, 0.0), get_cell_value(latest_row, gas_col, float, 0.0)
        prev_elec, curr_elec = get_cell_value(previous_row, elec_col, float, 0.0), get_cell_value(latest_row, elec_col, float, 0.0)
        admin_fee = get_cell_value(latest_row, admin_col, float, 0.0)
        internet_fee = self.settings["internet_fee_monthly"]

        try:
            start_date, end_date = previous_date, latest_date
            
            # FIXED: Both dates are now guaranteed to be timezone-aware
            billing_days = (end_date - start_date).days
            
            if billing_days <= 0: 
                raise ValueError("Billing period is zero or negative.")
                
            gas_consumption, elec_consumption = curr_gas - prev_gas, curr_elec - prev_elec
            gas_cost = gas_consumption * self.settings["gas_tariff"]
            electricity_cost = elec_consumption * self.settings["electricity_tariff"]
            prorated_admin_fee = (admin_fee / 30) * billing_days
            prorated_internet_fee = (internet_fee / 30) * billing_days
            total_utilities_cost = gas_cost + electricity_cost + prorated_admin_fee + prorated_internet_fee
            total_rent_ron = 0
            if include_rent and rent_eur is not None:
                total_rent_ron = rent_eur * self.settings["eur_to_ron_rate"]
            grand_total = total_utilities_cost + total_rent_ron

            billing_period_str = f"{billing_days} days ({start_date.strftime('%d-%b-%Y')} to {end_date.strftime('%d-%b-%Y')})"
            drive_links_report_parts = ["\n🔗 **Recent Drive Folders:**"]
            all_matching_folders = self.find_all_matching_folders_sorted(ap_code)
            if not all_matching_folders:
                drive_links_report_parts.append("  • No matching Drive folders found.")
            else:
                for i, folder in enumerate(all_matching_folders[:3]):
                    folder_id, folder_name = folder.get('id'), folder.get('name')
                    folder_link = f"https://drive.google.com/drive/folders/{folder_id}"
                    folder_type = "Contract" if "CTR" in folder_name.upper() else "Utility"
                    drive_links_report_parts.append(f"  {i+1}. **{folder_type}:** [{folder_name}]({folder_link})")
            drive_links_report_str = "\n".join(drive_links_report_parts)

            diagnostic_report = (
                f"📊 Full Calculation Breakdown for Ap {ap_code}\n"
                f"Billing Period: {billing_period_str}\n\n"
                f"🔥 Gas Meter:\n"
                f"  • Previous: {prev_gas}\n"
                f"  • Current:  {curr_gas}\n"
                f"  • Consumption: {gas_consumption:.2f} m³ (Cost: {gas_cost:.2f} RON)\n\n"
                f"⚡ Electricity Meter:\n"
                f"  • Previous: {prev_elec}\n"
                f"  • Current:  {curr_elec}\n"
                f"  • Consumption: {elec_consumption:.2f} kWh (Cost: {electricity_cost:.2f} RON)\n"
                f"{drive_links_report_str}"
            )
            
            summary_parts = [f"🧾 {ap_code} ({billing_period_str})\n"]
            summary_parts.append(f"💰 Total Utilities: {total_utilities_cost:.2f} RON")
            summary_parts.append(f"  • ⚡ Electricity: {electricity_cost:.2f}")
            summary_parts.append(f"  • 🔥 Gas: {gas_cost:.2f}")
            summary_parts.append(f"  • 🏛️ Administration: {prorated_admin_fee:.2f} (Prorated from {admin_fee})")
            summary_parts.append(f"  • 🌐 Internet: {prorated_internet_fee:.2f} (Prorated from {internet_fee})")
            if include_rent:
                summary_parts.append(f"\n🏠 Total Rent: {total_rent_ron:.2f} RON (€{rent_eur} @ {self.settings['eur_to_ron_rate']})")
                summary_parts.append(f"\n✨ GRAND TOTAL: {grand_total:.2f} RON")
            summary_report = "\n".join(summary_parts)
            
            return diagnostic_report, summary_report
            
        except Exception as e:
            error_message = f"Error during utility calculation for '{ap_code}': {e}"
            print(error_message)
            import traceback
            traceback.print_exc()
            self._log_error_to_drive(str(e), "calculate_and_format_utilities")
            return error_message, None

  

    def download_drive_folder_contents(self, drive_link: str | None, ap_code: str, destination_base_path: str, subfolder_name: str) -> str | None:
        if not self.drive_service:
            print("Error: Google Drive service is not available.")
            return None

        folder_id = None
        
        if drive_link and 'drive.google.com' in drive_link:
            match = re.search(r'/folders/([a-zA-Z0-9_-]+)', drive_link)
            if match:
                folder_id = match.group(1)
                print(f"Extracted Folder ID from provided link: {folder_id}")
        
        if not folder_id:
            print("No valid link in sheet. Performing intelligent fallback search...")
            
            # --- THIS IS THE FIX ---
            # 1. Call the CORRECTLY named function.
            all_matches = self.find_all_matching_folders_sorted(ap_code)
            
            # 2. Check if the returned list is not empty.
            if all_matches:
                # 3. Get the first item from the list (which is the newest) and extract its ID.
                folder_id = all_matches[0].get('id')

        if not folder_id:
            print(f"Error: Could not find any Drive folder for '{ap_code}'.")
            return None
        
        # ... (The rest of the download logic remains exactly the same)
        print(f"Listing files inside Folder ID: {folder_id}...")
        try:
            # ... (the rest of the function is unchanged)
            query = f"'{folder_id}' in parents and trashed = false"
            results = self.drive_service.files().list(q=query, pageSize=20, fields="files(id, name)").execute()
            files_to_download = results.get("files", [])
            if not files_to_download:
                print("No files found inside the Drive folder.")
                return None
            destination_folder = os.path.join(destination_base_path, subfolder_name)
            os.makedirs(destination_folder, exist_ok=True)
            print(f"Downloading {len(files_to_download)} file(s) to '{destination_folder}'...")
            for file_item in files_to_download:
                file_id, file_name = file_item.get('id'), file_item.get('name')
                request = self.drive_service.files().get_media(fileId=file_id)
                fh = io.BytesIO()
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while not done:
                    status, done = downloader.next_chunk()
                    print(f"  -> Downloading '{file_name}' {int(status.progress() * 100)}%.")
                file_path = os.path.join(destination_folder, file_name)
                with open(file_path, 'wb') as f:
                    f.write(fh.getvalue())
            print("✅ Download complete.")
            return destination_folder
        except HttpError as error:
            print(f"An error occurred during the download process: {error}")
            self._log_error_to_drive(str(error), "download_drive_folder_contents")
            return None

    def format_email_log(self, ap_code: str, limit: int = 3) -> str | None:
        """
        Finds the latest emails sent regarding a specific apartment and formats
        them into a report.

        Args:
            ap_code: The apartment code to search for.
            limit: The maximum number of recent emails to show.

        Returns:
            A formatted string with the email history, or a message if not found.
        """
        sheet_name = "EMAIL_LOG"
        if sheet_name not in self.all_data:
            return f"❌ Data for sheet '{sheet_name}' is not loaded."

        sheet_info = self.all_data[sheet_name]
        header = sheet_info['header']
        data = sheet_info['data']

        try:
            header_upper = [str(h).strip().upper() for h in header]
            # Find columns by keyword for robustness
            def find_col_by_keyword(keyword):
                for i, h in enumerate(header_upper):
                    if keyword in h: return i
                raise ValueError(f"'{keyword}'")
            
            date_col = 0
            code_col = find_col_by_keyword("AP CODE")
            subject_col = find_col_by_keyword("SUBJECT")
            summary_col = find_col_by_keyword("SUMMARY")
            url_col = find_col_by_keyword("MESSAGE URL")

        except ValueError as e:
            return f"❌ A required column containing {e} was not found in the '{sheet_name}' header."

        # Find all entries where the AP CODE is present in the 'AP CODE' column
        relevant_rows = []
        for row in data:
            if len(row) > code_col and ap_code.strip().upper() in row[code_col].strip().upper():
                relevant_rows.append(row)
        
        if not relevant_rows:
            return f"ℹ️ No email logs found for apartment code `{ap_code}`."

        # Sort the rows by date to find the most recent ones.
        def parse_date(date_str):
            try: return datetime.strptime(date_str, '%d/%m/%Y')
            except ValueError: return datetime.min
        
        relevant_rows.sort(key=lambda r: parse_date(r[date_col] if len(r) > date_col else ''), reverse=True)
        
        # --- BUILD THE FORMATTED STRING ---
        report_parts = [f"📧 Email Log for {ap_code} (showing latest {limit})\n"]
        
        for i, row in enumerate(relevant_rows[:limit]):
            # Helper to safely get data from the row
            def get_cell(index, default="N/A"):
                return row[index] if len(row) > index and row[index] else default

            date_str = get_cell(date_col)
            subject = get_cell(subject_col)
            summary = get_cell(summary_col)
            url = get_cell(url_col)

            report_parts.append(f"--- Email {i+1} ---")
            report_parts.append(f"• Date: {date_str}")
            report_parts.append(f"• Subject: {subject}")
            report_parts.append(f"• Summary:\n```\n{summary}\n```")
            if url != "N/A":
                report_parts.append(f"• Link: [View Email]({url})")
            report_parts.append("") # Add a blank line for spacing

        return "\n".join(report_parts)

    def format_mo_data(self, ap_code: str, limit: int = 3) -> str | None:
        """
        Finds the latest financial transactions for a specific apartment in the
        'MO_DATA' sheet and formats them into a report.
        """
        sheet_name = "MO_DATA"
        if sheet_name not in self.all_data:
            return f"❌ Data for sheet '{sheet_name}' is not loaded."

        sheet_info = self.all_data[sheet_name]
        header = sheet_info['header']
        data = sheet_info['data']

        try:
            header_upper = [str(h).strip().upper() for h in header]
            def find_col_by_keyword(keyword):
                for i, h in enumerate(header_upper):
                    if keyword in h: return i
                raise ValueError(f"'{keyword}'")
            
            code_col = find_col_by_keyword("APARTMENT CODE")
            date_col = find_col_by_keyword("SUBMISSION DATE")
            task_col = find_col_by_keyword("TYPE OF MONEY TASK")
            specific_task_col = find_col_by_keyword("SPECIFIC MONEY TASK")
            amount_col = find_col_by_keyword("AMOUNT")
            currency_col = find_col_by_keyword("CURRENCY")

        except ValueError as e:
            return f"❌ A required column containing {e} was not found in the '{sheet_name}' header."

        relevant_rows = [row for row in data if len(row) > code_col and row[code_col].strip().upper() == ap_code.strip().upper()]
        if not relevant_rows:
            return f"ℹ️ No transaction data found for apartment code `{ap_code}`."

        def parse_date(date_str):
            try: return datetime.strptime(date_str, '%d/%m/%Y')
            except ValueError: return datetime.min
        
        relevant_rows.sort(key=lambda r: parse_date(r[date_col] if len(r) > date_col else ''), reverse=True)
        
        report_parts = [f"💰 Latest Transactions for {ap_code} (showing last {limit})\n"]
        
        for i, row in enumerate(relevant_rows[:limit]):
            def get_cell(index, default="N/A"):
                return row[index] if len(row) > index and row[index] else default

            date_str = get_cell(date_col)
            task = get_cell(task_col)
            specific_task = get_cell(specific_task_col)
            amount = get_cell(amount_col)
            currency = get_cell(currency_col)

            report_parts.append(f"--- Transaction {i+1} ---")
            report_parts.append(f"• Date: {date_str}")
            report_parts.append(f"• Task: {task} - {specific_task}")
            report_parts.append(f"• Amount: {amount} {currency}")
            report_parts.append("")

        return "\n".join(report_parts)

    def generate_master_report(self, ap_code: str) -> str:
        print(f"\n--- Generating Master Report for {ap_code} ---")
        
        apartment_details_dict = self.get_apartment_data(ap_code)
        if not apartment_details_dict:
            return f"❌ Apartment with code `{ap_code}` not found. Cannot generate report."
        
        realtor_name = apartment_details_dict.get("REALTOR", "Unknown")
        apartment_details_str = self.format_apartment_info(ap_code)
        diag_report, _ = self.calculate_and_format_utilities(ap_code, include_rent=False)
        utility_report_str = diag_report if diag_report else f"ℹ️ Could not calculate utility report for `{ap_code}`."
        email_log_str = self.format_email_log(ap_code)
        mo_data_str = self.format_mo_data(ap_code)

        # --- NEW: Get and format contract info ---
        contract_info = self.get_latest_contract_info(ap_code)
        contract_report_parts = ["\n📜 Latest Contract Info:"]
        if contract_info:
            contract_report_parts.append(f"  • Folder: [{contract_info['name']}]({contract_info['link']})")
            contract_report_parts.append(f"  • Period: {contract_info['start_date']} to {contract_info['end_date']}")
        else:
            contract_report_parts.append("  • No contract folder found.")
        contract_report_str = "\n".join(contract_report_parts)
        # --- END OF NEW LOGIC ---

        final_report = (
            f"- - - - - - - - - - - - - - - - - - - -\n"
            f"MASTER REPORT\n"
            f" Apartment Code: {ap_code}\n"
            f" Realtor: {realtor_name}\n"
            f"- - - - - - - - - - - - - - - - - - - -\n\n"
            f"{apartment_details_str}\n\n"
            f"- - - - - - - - - - - - - - - - - - - -\n\n"
            f"{utility_report_str}\n\n"
            f"- - - - - - - - - - - - - - - - - - - -\n\n"
            f"{contract_report_str}\n\n" # ADDED CONTRACT REPORT
            f"- - - - - - - - - - - - - - - - - - - -\n\n"
            f"{email_log_str}\n\n"
            f"- - - - - - - - - - - - - - - - - - - -\n\n"
            f"{mo_data_str}\n"
            f"- - - - - - - - - - - - - - - - - - - -"
        )
        return final_report

    def generate_batch_report(self, ap_codes: list) -> str:
        print(f"\n--- Generating Batch Report for {len(ap_codes)} codes ---")
        final_report_parts = []
        for i, code in enumerate(ap_codes):
            code = code.strip().upper()
            if not code: continue
            print(f"  -> Processing code {i+1}/{len(ap_codes)}: {code}")

            single_code_report = [
                "═" * 100,
                f"📊 Consolidated Report for {code}",
                "═" * 100 + "\n"
            ]

            # --- NEW: Get Contract and Rent info first ---
            contract_info = self.get_latest_contract_info(code)
            apartment_details = self.get_apartment_data(code)
            rent_eur = "N/A"
            if apartment_details:
                try:
                    rent_eur = float(apartment_details.get("ARENDA CLIENT", "N/A"))
                except (ValueError, TypeError):
                    rent_eur = "Invalid"
            
            contract_report_parts = ["📜 Latest Contract Info:"]
            if contract_info:
                contract_report_parts.append(f"  • Folder: [{contract_info['name']}]({contract_info['link']})")
                contract_report_parts.append(f"  • Period: {contract_info['start_date']} to {contract_info['end_date']}")
                contract_report_parts.append(f"  • Rent:`{rent_eur} EUR")
            else:
                contract_report_parts.append("  • No contract folder found.")
            single_code_report.append("\n".join(contract_report_parts))
            # --- END OF NEW LOGIC ---

            single_code_report.append("\n" + "─" * 40 + "\n")

            # Get Full Utility Calculation (Diagnostic AND Summary)
            diag_report, summary_report = self.calculate_and_format_utilities(code, include_rent=True)
            if summary_report:
                single_code_report.append(diag_report)
                single_code_report.append("\n--- FINAL SUMMARY ---")
                single_code_report.append(summary_report)
            else:
                single_code_report.append(f"ℹ️ Could not generate utility report for {code}: {diag_report}")
            
            single_code_report.append("\n" + "─" * 40 + "\n")

            # Get Latest Email Logs
            email_log_str = self.format_email_log(code)
            if email_log_str:
                single_code_report.append(email_log_str)

            single_code_report.append("\n" + "─" * 40 + "\n")

            # Get Latest Transactions
            mo_data_str = self.format_mo_data(code)
            if mo_data_str:
                single_code_report.append(mo_data_str)

            final_report_parts.append("\n".join(single_code_report))

        return "\n\n".join(final_report_parts)

    def _get_folders_from_cache_or_api(self, parent_id: str, folder_type: str) -> list:
        """
        An intelligent caching layer for Drive folder listings.
        It fetches all folders from a parent directory only once per session.
        """
        # If the cache is empty or older than, say, 5 minutes, we refresh it.
        # (You can adjust the cache duration as needed)
        refresh_needed = not self.folder_cache_timestamp or (datetime.now(pytz.utc) - self.folder_cache_timestamp).total_seconds() > 300
        if parent_id not in self.folder_cache or refresh_needed:
            print(f"  -> Caching/Refreshing ALL folders in '{folder_type}'...")
            query = f"'{parent_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
            
            all_folders = []
            page_token = None
            while True:
                response = self.drive_service.files().list(
                    q=query, spaces='drive', fields='nextPageToken, files(id, name, createdTime)', pageToken=page_token
                ).execute()
                all_folders.extend(response.get('files', []))
                page_token = response.get('nextPageToken', None)
                if page_token is None: break
            
            self.folder_cache[parent_id] = all_folders
            self.folder_cache_timestamp = datetime.now(pytz.utc)
            print(f"  -> Cached {len(all_folders)} folders from '{folder_type}'.")
        else:
            print(f"  -> Using cached folder list for '{folder_type}'.")

        return self.folder_cache[parent_id]

    def find_all_matching_folders_sorted(self, ap_code: str) -> list:
        """
        Performs a definitive search using the new caching layer for maximum efficiency.
        """
        if not self.drive_service: return []
        
        normalized_search_term = ap_code.upper().replace('-', '')
        if "TI117" in normalized_search_term:
            normalized_search_term = normalized_search_term.replace("TI117", "TI17")
        
        print(f"Intelligent Search: Searching cache for folders matching '{ap_code}'...")

        try:
            all_folders_from_drive = []
            # --- THIS IS THE NEW, EFFICIENT PART ---
            # Get folders from the cache (or API if cache is empty/stale)
            all_folders_from_drive.extend(self._get_folders_from_cache_or_api(UTILITIES_PARENT_FOLDER_ID, "UTILITIES"))
            all_folders_from_drive.extend(self._get_folders_from_cache_or_api(CLIENTS_PARENT_FOLDER_ID, "CLIENTS"))
            # --- END OF NEW PART ---

            matching_folders = []
            for folder in all_folders_from_drive:
                folder_name = folder.get('name', '')
                normalized_folder_name = folder_name.upper().replace('-', '')
                if normalized_search_term in normalized_folder_name:
                    matching_folders.append(folder)

            if not matching_folders:
                print(f"No matching folders found for '{ap_code}'.")
                return []

            matching_folders.sort(
                key=lambda f: datetime.strptime(f.get('createdTime'), '%Y-%m-%dT%H:%M:%S.%fZ'),
                reverse=True
            )
            
            print(f"Found {len(matching_folders)} total match(es). Returning them sorted by creation date.")
            return matching_folders

        except HttpError as error:
            print(f"An error occurred while searching for the folder: {error}")
            return []

    def get_latest_contract_info(self, ap_code: str) -> dict | None:
        """
        Finds the latest contract info for an apartment by reading data from the
        APARTMENTS sheet and finding the corresponding folder in Drive.
        """
        # --- STEP 1: Get the apartment's data from the APARTMENTS sheet ---
        apartment_details = self.get_apartment_data(ap_code, sheet_name="APARTMENTS")
        if not apartment_details:
            # If we can't find the main entry, we can't get contract info.
            return None

        # --- STEP 2: Extract the dates directly from the sheet data ---
        start_date_str = apartment_details.get("START", "N/A")
        end_date_str = apartment_details.get("END", "N/A")

        # --- STEP 3: Find the contract folder using our intelligent search ---
        # This part remains the same, as it's the best way to find the folder.
        newest_contract_folder = None
        all_matches = self.find_all_matching_folders_sorted(ap_code)
        if all_matches:
            # Filter this list to find only the contract folders
            contract_folders = [f for f in all_matches if "CTR" in f.get("name", "").upper()]
            if contract_folders:
                newest_contract_folder = contract_folders[0]
        
        # --- STEP 4: Assemble the final dictionary ---
        if newest_contract_folder:
            return {
                "name": newest_contract_folder.get("name"),
                "id": newest_contract_folder.get("id"),
                "link": f"https://drive.google.com/drive/folders/{newest_contract_folder.get('id')}",
                "start_date": start_date_str,
                "end_date": end_date_str
            }
        else:
            # If no folder was found, we can still return the dates from the sheet
            return {
                "name": "No folder found",
                "id": None,
                "link": "N/A",
                "start_date": start_date_str,
                "end_date": end_date_str
            }

    def calculate_key_metrics(self, realtor_name: str = None):
        """
        Calculates high-level business metrics from the APARTMENTS sheet.
        If a realtor_name is provided, it filters the metrics for that specific realtor.
        """
        apartments_sheet = "APARTMENTS"
        if apartments_sheet not in self.all_data:
            return None

        sheet_info = self.all_data[apartments_sheet]
        header, data = sheet_info['header'], sheet_info['data']

        try:
            header_upper = [str(h).strip().upper() for h in header]
            code_col = header_upper.index("AP CODE")
            status_col = header_upper.index("CONTRACT STATUS_CL")
            rent_col = header_upper.index("ARENDA CLIENT")
            end_date_col = header_upper.index("END")
            realtor_col = header_upper.index("REALTOR") # NEW: Get the realtor column
        except ValueError as e:
            error_msg = f"Missing a required column in APARTMENTS sheet: {e}"
            print(error_msg)
            self._log_error_to_drive(error_msg, "calculate_key_metrics")
            return None

        total_apartments = 0
        active_contracts = 0
        total_monthly_rent = 0
        upcoming_checkouts = 0
        
        today = datetime.now(pytz.utc).date()
        thirty_days_from_now = today + timedelta(days=30)

        for row in data:
            ap_code = row[code_col].strip() if len(row) > code_col and row[code_col] else None
            if not ap_code:
                continue

            # --- THIS IS THE NEW LOGIC ---
            # If a realtor_name is specified, and this row doesn't match, skip it.
            current_realtor = row[realtor_col].strip() if len(row) > realtor_col else ""
            if realtor_name and realtor_name.lower() != current_realtor.lower():
                continue
            # --- END OF NEW LOGIC ---

            total_apartments += 1

            status = row[status_col] if len(row) > status_col else ""
            if "EXP" not in status.upper():
                active_contracts += 1
                try:
                    rent = float(row[rent_col]) if len(row) > rent_col and row[rent_col] else 0
                    total_monthly_rent += rent
                except (ValueError, TypeError):
                    pass 

            end_date_str = row[end_date_col] if len(row) > end_date_col else ""
            try:
                end_date = datetime.strptime(end_date_str, '%d-%m-%Y').date()
                if today <= end_date <= thirty_days_from_now:
                    upcoming_checkouts += 1
            except (ValueError, TypeError):
                pass

        return {
            "total_apartments": total_apartments,
            "active_contracts": active_contracts,
            "total_monthly_rent": total_monthly_rent,
            "upcoming_checkouts": upcoming_checkouts
        }


class EmailWatcher:
        def __init__(self, sheet_manager: GoogleSheet):
            self.sheet_manager = sheet_manager
            self.watchlist_file = "email_watchlist.json"
            self.watchlist = self._load_watchlist()
            self.last_check_timestamp = datetime.now(pytz.utc)
            self.task = None

        def _load_watchlist(self) -> list:
            """Loads the list of codes to watch from a JSON file."""
            if os.path.exists(self.watchlist_file):
                try:
                    with open(self.watchlist_file, 'r') as f:
                        data = json.load(f)
                        # Ensure it's a list
                        return data if isinstance(data, list) else []
                except (json.JSONDecodeError, TypeError):
                    print("Warning: Could not read email_watchlist.json. Starting with an empty list.")
            return []

        def _save_watchlist(self):
            """Saves the current watchlist to the JSON file."""
            with open(self.watchlist_file, 'w') as f:
                json.dump(self.watchlist, f, indent=4)
            print("INFO: [Email Watcher] Watchlist saved.")

        def add_code(self, ap_code: str):
            """Adds a code to the watchlist."""
            normalized_code = ap_code.strip().upper()
            if normalized_code and normalized_code not in self.watchlist:
                self.watchlist.append(normalized_code)
                self._save_watchlist()
                print(f"✅ '{normalized_code}' added to the email watchlist.")
            elif not normalized_code:
                print("❌ Cannot add an empty code.")
            else:
                print(f"ℹ️ '{normalized_code}' is already on the watchlist.")

        def remove_code(self, ap_code: str):
            """Removes a code from the watchlist (checks it off)."""
            normalized_code = ap_code.strip().upper()
            if normalized_code in self.watchlist:
                self.watchlist.remove(normalized_code)
                self._save_watchlist()
                print(f"✅ '{normalized_code}' removed (checked off) from the email watchlist.")
            else:
                print(f"❌ '{normalized_code}' was not found on the watchlist.")

        async def check_for_new_emails(self):
            """
            Scans the EMAIL_LOG for new entries and provides context by showing
            the previous email for the same code.
            """
            print(f"\n--- [Email Watcher] Checking for new emails at {datetime.now(pytz.utc).strftime('%H:%M:%S')} ---")
            
            self.sheet_manager.load_data()
            
            sheet_name = "EMAIL_LOG"
            if sheet_name not in self.sheet_manager.all_data:
                print("ERROR: [Email Watcher] EMAIL_LOG sheet not loaded. Skipping check.")
                return

            sheet_info = self.sheet_manager.all_data[sheet_name]
            header, data = sheet_info['header'], sheet_info['data']

            try:
                header_upper = [str(h).strip().upper() for h in header]
                date_col = header_upper.index("DATE")
                code_col = header_upper.index("AP CODE")
                subject_col = header_upper.index("SUBJECT")
                summary_col = header_upper.index("SUMMARY")
            except ValueError as e:
                print(f"ERROR: [Email Watcher] A required column was not found: {e}")
                return

            found_new_email = False
            # --- NEW: Keep track of which codes we've already alerted for in this run ---
            alerted_codes_this_run = set()

            for row in data:
                try:
                    date_str = row[date_col] if len(row) > date_col else ''
                    email_date = datetime.strptime(date_str, '%d/%m/%Y')

                    if email_date.date() >= self.last_check_timestamp.date():
                        ap_codes_in_row = [c.strip().upper() for c in row[code_col].split(',')] if len(row) > code_col else []
                        
                        for watched_code in self.watchlist:
                            if watched_code in ap_codes_in_row and watched_code not in alerted_codes_this_run:
                                
                                # --- NEW CONTEXT-AWARE LOGIC ---
                                # We found a new email for a watched code.
                                # Now, let's find the previous email for this same code.
                                all_emails_for_this_code = []
                                for past_row in data:
                                    past_codes = [c.strip().upper() for c in past_row[code_col].split(',')] if len(past_row) > code_col else []
                                    if watched_code in past_codes:
                                        all_emails_for_this_code.append(past_row)
                                
                                # Sort them by date to find the history
                                def parse_date(r):
                                    try: return datetime.strptime(r[date_col], '%d/%m/%Y')
                                    except (ValueError, IndexError): return datetime.min
                                all_emails_for_this_code.sort(key=parse_date, reverse=True)

                                # The newest email is at index 0. The previous one is at index 1.
                                newest_email = all_emails_for_this_code[0]
                                previous_email = all_emails_for_this_code[1] if len(all_emails_for_this_code) > 1 else None
                                
                                # Format the alert
                                new_subject = newest_email[subject_col] if len(newest_email) > subject_col else 'N/A'
                                
                                print("\n" + "="*50)
                                print(f"🚨 NEW EMAIL ALERT 🚨")
                                print(f"  -> For Watched Code: {watched_code}")
                                print(f"  -> Date: {newest_email[date_col]}")
                                print(f"  -> Subject: {new_subject}")
                                
                                if previous_email:
                                    prev_date = previous_email[date_col] if len(previous_email) > date_col else 'N/A'
                                    prev_subject = previous_email[subject_col] if len(previous_email) > subject_col else 'N/A'
                                    print("\n  --- Previous Email for Context ---")
                                    print(f"  -> Previous Date: {prev_date}")
                                    print(f"  -> Previous Subject: {prev_subject}")
                                else:
                                    print("\n  -> No previous email history found for this code.")

                                print("="*50 + "\n")
                                # --- END OF NEW LOGIC ---

                                found_new_email = True
                                alerted_codes_this_run.add(watched_code)

                except (ValueError, IndexError):
                    continue

            if not found_new_email:
                print("--- [Email Watcher] No new emails found for watched codes. ---")
            
            self.last_check_timestamp = datetime.now(pytz.utc)

        async def start_watcher_task(self, interval_minutes: int = 30):
            """Starts the background task."""
            if self.task and not self.task.done():
                print("INFO: [Email Watcher] Task is already running.")
                return

            async def loop():
                # Initial check shortly after startup
                await asyncio.sleep(10) 
                while True:
                    await self.check_for_new_emails()
                    await asyncio.sleep(interval_minutes * 60)
            
            self.task = asyncio.create_task(loop())
            print(f"✅ Email Watcher background task started. Checking every {interval_minutes} minutes.")    

# --- THIS IS THE STANDALONE MENU-DRIVEN TOOL ---
async def main_tool():
    print("--- Google Sheets Standalone Tool ---")

    print("\nInitializing and loading all sheet data...")
    try:
        sheets_to_load = ["APARTMENTS", "UT_DATA", "EMAIL_LOG", "ZET", "STR", "MO_DATA", "CL_DATA"]
        sheet_manager = GoogleSheet(spreadsheet_name="APARTMENTS", sheet_names=sheets_to_load)
        sheet_manager.sync_exchange_rate()# Sync on startup
        sheet_manager.load_data()
        
        email_watcher = EmailWatcher(sheet_manager)
        await email_watcher.start_watcher_task(interval_minutes=30)
        
        DOWNLOADS_BASE_DIR = "drive_downloads"
        os.makedirs(DOWNLOADS_BASE_DIR, exist_ok=True)
        BATCH_REPORTS_DIR = "Batch_Reports"
        os.makedirs(BATCH_REPORTS_DIR, exist_ok=True)
        print("-" * 50)
    except Exception as e:
        print(f"\nCRITICAL ERROR during initialization: {e}")
        import traceback
        traceback.print_exc()
        exit()

    # --- MAIN MENU LOOP ---
    while True:
        print("\n--- Main Menu ---")
        print("  [1] 📜 Generate Reports")
        print("  [2] 🖩 Utilities")
        print("  [3] 🕵️ Apartment & Realtor Info")
        print("  [4] 📧 Client Communication")
        print("  [5] ⚙️ System & Settings")
        print("  [q] ❌ Exit")
        
        choice = input("> ").lower()

        if choice == '1': # Generate Reports Sub-Menu
            while True:
                print("\n--- 📜 Generate Reports ---")
                print("  [1] Generate MASTER REPORT for a single apartment")
                print("  [2] Generate BATCH REPORT for multiple apartments")
                print("  [b] Back to Main Menu")
                sub_choice = input("> ").lower()
                if sub_choice == '1':
                    code = input("Enter the Apartment Code for the MASTER REPORT: ")
                    if code: 
                        sheet_manager.sync_exchange_rate(silent=True)
                        print("\n" + sheet_manager.generate_master_report(code) + "\n")
                elif sub_choice == '2':
                    print("\n--- Generate Batch Report ---")
                    print("Paste apartment codes, one per line. Press Enter on an empty line to finish.")
                    codes_to_process = []
                    while True:
                        line = input()
                        if not line: break
                        codes_to_process.append(line)
                    if codes_to_process:
                        sheet_manager.sync_exchange_rate(silent=True)
                        report_content = sheet_manager.generate_batch_report(codes_to_process)
                        timestamp = datetime.now(pytz.utc).strftime("%Y-%m-%d_%H-%M-%S")
                        filename = f"Batch_Report_{timestamp}.txt"
                        file_path = os.path.join(BATCH_REPORTS_DIR, filename)
                        try:
                            with open(file_path, 'w', encoding='utf-8') as f: f.write(report_content)
                            print("\n" + "="*50 + f"\n✅ Batch Report successfully exported to:\n   {os.path.abspath(file_path)}\n" + "="*50 + "\n")
                        except Exception as e:
                            print(f"\n❌ ERROR: Could not write report to file: {e}")
                    else:
                        print("No codes entered.")
                elif sub_choice == 'b':
                    break
                else:
                    print("Invalid choice.")

        elif choice == '2': # Utilities Sub-Menu
            while True:
                print("\n--- 🖩 Utilities ---")
                print("  [1] Run Full Utilities Calculator (Utilities Only)")
                print("  [2] Run Full Utilities + Rent Calculator")
                print("  [3] Get simple utility consumption report")
                print("  [4] Download utility photos for an apartment")
                print("  [b] Back to Main Menu")
                sub_choice = input("> ").lower()
                if sub_choice == '1' or sub_choice == '2':
                    include_rent_flag = (sub_choice == '2')
                    title = "Utilities + Rent" if include_rent_flag else "Utilities Only"
                    print(f"\n--- Auto-Calculate {title} from Sheet Data ---")
                    code = input("Enter the Apartment Code to calculate: ")
                    if code:
                        sheet_manager.sync_exchange_rate(silent=True)
                        diag_report, summary_report = sheet_manager.calculate_and_format_utilities(code, include_rent=include_rent_flag)
                        if summary_report:
                            print("\n--- [DIAGNOSTIC REPORT] ---\n" + diag_report)
                            print("\n--- [FINAL SUMMARY] ---\n" + summary_report + "\n" + "-" * 25 + "\n")
                        else:
                            print("\n" + diag_report + "\n")
                elif sub_choice == '3':
                    code = input("Enter the Apartment Code to get its utility consumption report: ")
                    if code:
                        report = sheet_manager.format_ut_data(code)
                        print("\n" + report + "\n")
                elif sub_choice == '4':
                    code = input("Enter the Apartment Code to download its utility photos: ")
                    if code:
                        ut_data_row = sheet_manager.get_row_by_code(code, sheet_name="UT_DATA", code_column_name="Apartment Code")
                        if ut_data_row:
                            drive_link = ut_data_row.get("LINK_DRIVE")
                            date_str = ut_data_row.get("Date of Reading", "").split(" ")[0]
                            safe_date_str = date_str.replace("/", "-").replace("\\", "-")
                            subfolder = f"{code}_{safe_date_str}"
                            download_path = sheet_manager.download_drive_folder_contents(drive_link, code, DOWNLOADS_BASE_DIR, subfolder)
                            if download_path:
                                print(f"✅ Successfully downloaded files to: {os.path.abspath(download_path)}")
                        else:
                            print(f"Could not find utility data for code '{code}'.")
                elif sub_choice == 'b':
                    break
                else:
                    print("Invalid choice.")

        elif choice == '3': # Apartment & Realtor Info Sub-Menu
            while True:
                print("\n--- 🕵️ Apartment & Realtor Info ---")
                print("  [1] Get full details for a specific apartment")
                print("  [2] List all apartments for a specific realtor")
                print("  [3] 🗂️ Generate BATCH REPORT for a realtor's apartments")
                print("  [4] 🗓️ Generate Upcoming Events Report")
                print("  [5] 💰 Generate Upcoming Payments Report") # NEW
                print("  [b] Back to Main Menu")
                sub_choice = input("> ").lower()

                if sub_choice == '1':
                    code = input("Enter the Apartment Code: ")
                    if code:
                        report = sheet_manager.format_apartment_info(code)
                        print("\n" + report + "\n")
                
                elif sub_choice == '2':
                    realtor = input("Enter the Realtor's full name: ")
                    if realtor:
                        codes = sheet_manager.get_apartments_by_realtor(realtor)
                        if codes:
                            print(f"\nFound {len(codes)} apartments for '{realtor}':")
                            for c in sorted(codes): print(f"  - {c}")
                            print("")
                
                elif sub_choice == '3':
                    # ... (Batch Report for Realtor logic remains the same)
                    print("\nSelect a realtor to generate a batch report for:")
                    print("  [1] Khrystyna Markin")
                    print("  [2] Kristina Fedina")
                    realtor_choice = input("> ")
                    realtor_name = "Khrystyna Markin" if realtor_choice == '1' else "Kristina Fedina" if realtor_choice == '2' else None
                    if realtor_name:
                        print(f"Finding all apartments for '{realtor_name}'...")
                        codes_to_process = sheet_manager.get_apartments_by_realtor(realtor_name)
                        if codes_to_process:
                            print(f"Found {len(codes_to_process)} apartments. Generating batch report...")
                            report_content = sheet_manager.generate_batch_report(codes_to_process)
                            timestamp = datetime.now(pytz.utc).strftime("%Y-%m-%d_%H-%M-%S")
                            filename = f"Batch_Report_{realtor_name.replace(' ', '_')}_{timestamp}.txt"
                            file_path = os.path.join(BATCH_REPORTS_DIR, filename)
                            try:
                                with open(file_path, 'w', encoding='utf-8') as f: f.write(report_content)
                                print("\n" + "="*50 + f"\n✅ Batch Report successfully exported to:\n   {os.path.abspath(file_path)}\n" + "="*50 + "\n")
                            except Exception as e:
                                print(f"\n❌ ERROR: Could not write report to file: {e}")
                        else:
                            print(f"No apartments found for '{realtor_name}'.")
                    else:
                        print("Invalid choice.")

                elif sub_choice == '4':
                    # ... (Upcoming Events logic remains the same)
                    print("\nSelect a realtor for the events report:")
                    print("  [1] Khrystyna Markin")
                    print("  [2] Kristina Fedina")
                    realtor_choice = input("> ")
                    realtor_name = "Khrystyna Markin" if realtor_choice == '1' else "Kristina Fedina" if realtor_choice == '2' else None
                    if realtor_name:
                        report = sheet_manager.generate_upcoming_events_report(realtor_name)
                        print("\n" + report + "\n")
                    else:
                        print("Invalid choice.")

                elif sub_choice == '5':
                    # --- NEW: UPCOMING PAYMENTS REPORT LOGIC ---
                    try:
                        days_str = input("Enter the upcoming day window (e.g., 7 for the next 7 days): ")
                        days_window = int(days_str)
                        
                        print("\nSelect a realtor for the report:")
                        print("  [1] Khrystyna Markin")
                        print("  [2] Kristina Fedina")
                        realtor_choice = input("> ")
                        
                        realtor_name = None
                        if realtor_choice == '1':
                            realtor_name = "Khrystyna Markin"
                        elif realtor_choice == '2':
                            realtor_name = "Kristina Fedina"
                        
                        if realtor_name:
                            report = sheet_manager.generate_upcoming_payments_report(realtor_name, days_window)
                            print("\n" + report + "\n")
                        else:
                            print("Invalid choice.")
                    except (ValueError, TypeError):
                        print("❌ Invalid number for days. Please enter a whole number.")
                    # --- END OF NEW LOGIC ---

                elif sub_choice == 'b':
                    break
                else:
                    print("Invalid choice.")

        elif choice == '4': # Client Communication Sub-Menu
            while True:
                print("\n--- 📧 Client Communication ---")
                print("  [1] Get latest email logs for an apartment")
                print("  [2] Find AP CODE by Client Email")
                print("  [3] Find Email by AP CODE")
                print("  [4] Manage Email Watchlist")
                print("  [b] Back to Main Menu")
                sub_choice = input("> ").lower()
                if sub_choice == '1':
                    code = input("Enter the Apartment Code to get its email history: ")
                    if code:
                        report = sheet_manager.format_email_log(code)
                        print("\n" + report + "\n")
                elif sub_choice == '2':
                    email = input("Enter the client's email address to search for: ")
                    if email:
                        result = sheet_manager.find_ap_code_by_email(email)
                        print("\n" + result + "\n")
                elif sub_choice == '3':
                    code = input("Enter the Apartment Code to find the client's email: ")
                    if code:
                        result = sheet_manager.find_email_by_ap_code(code)
                        print("\n" + result + "\n")
                elif sub_choice == '4':
                    # This is the existing watchlist sub-sub-menu
                    while True:
                        print("\n--- Email Watchlist Management ---")
                        print("Current watchlist:", email_watcher.watchlist if email_watcher.watchlist else "Empty")
                        print("\nOptions:\n  [1] Add a code\n  [2] Remove a code\n  [3] Force a manual check\n  [4] Return")
                        ss_choice = input(">> ")
                        if ss_choice == '1':
                            code_to_add = input("Enter code to add: ")
                            if code_to_add: email_watcher.add_code(code_to_add)
                        elif ss_choice == '2':
                            code_to_remove = input("Enter code to remove: ")
                            if code_to_remove: email_watcher.remove_code(code_to_remove)
                        elif ss_choice == '3':
                            await email_watcher.check_for_new_emails()
                        elif ss_choice == '4':
                            break
                        else:
                            print("Invalid choice.")
                elif sub_choice == 'b':
                    break
                else:
                    print("Invalid choice.")

        elif choice == '5': # System & Settings Sub-Menu
            while True:
                print("\n--- ⚙️ System & Settings ---")
                print("  [1] View/Change Settings (Tariffs & Rates)")
                print("  [2] 💶 Sync EUR-RON Exchange Rate from BNR (+0.3%)")
                print("  [3] 🔄 Refresh All Sheet Data")
                print("  [b] Back to Main Menu")
                sub_choice = input("> ").lower()

                if sub_choice == '1':
                    print("\n--- Current Settings ---")
                    for key, value in sheet_manager.settings.items(): print(f"  - {key}: {value}")
                    key_to_change = input("\nEnter setting to change (or press Enter to cancel): ").strip()
                    if key_to_change in sheet_manager.settings:
                        try:
                            new_value = float(input(f"Enter new value for '{key_to_change}': "))
                            sheet_manager.settings[key_to_change] = new_value
                            sheet_manager.save_settings()
                        except ValueError: print("❌ Invalid number.")
                    elif key_to_change: print("❌ Invalid setting name.")
                
                elif sub_choice == '2':
                    success, message = sheet_manager.sync_exchange_rate()
                    print(message)

                elif sub_choice == '3':
                    sheet_manager.reload_all_data()
                    if sheet_manager.last_modified_time:
                        print(f"\n✅ Data reloaded. Spreadsheet was last modified at: {sheet_manager.last_modified_time}")
                
                elif sub_choice == 'b':
                    break
                else:
                    print("Invalid choice.")

        elif choice == 'q':
            print("Exiting tool.")
            if email_watcher.task: email_watcher.task.cancel()
            break
        
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    try:
        asyncio.run(main_tool())
    except KeyboardInterrupt:

        print("\nExiting tool.")

BUCHAREST_TZ = pytz.timezone("Europe/Bucharest")

class TodoManager:
    def __init__(self, sheet_manager: GoogleSheet):
        self.sheet_manager = sheet_manager
        self.todo_file_name = "todo_list.json"
        self.local_cache_file = "todo_list_cache.json"
        self.file_id = None
        self.todo_list = self._load_list_from_drive()
        self.trash_bin = []
        self.is_dirty = False
        self.last_sync_time = None
        

    def _find_file_id(self):
        """Finds the file ID of the to-do list in Google Drive."""
        if self.file_id: return self.file_id
        try:
            # Search for the file in the root of the user's Drive
            query = f"name = '{self.todo_file_name}' and 'root' in parents and trashed = false"
            results = self.sheet_manager.drive_service.files().list(q=query, fields="files(id)").execute()
            items = results.get("files", [])
            if items:
                self.file_id = items[0]["id"]
                print(f"Found to-do list file in Drive with ID: {self.file_id}")
                return self.file_id
        except Exception as e:
            print(f"Error finding to-do list file: {e}")
            self.sheet_manager._log_error_to_drive(str(e), "TodoManager._find_file_id")
        return None

    def _load_list_from_drive(self) -> list:
        """Loads the to-do list from the JSON file in Google Drive with local caching."""
        # First, try to load from local cache if it's recent
        if os.path.exists(self.local_cache_file):
            try:
                with open(self.local_cache_file, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
                cache_time = cache_data.get('timestamp')
                if cache_time and (time.time() - cache_time) < 300:  # 5 minutes cache
                    print("Loading to-do list from local cache.")
                    self.todo_list = cache_data.get('todo_list', [])
                    self.trash_bin = cache_data.get('trash_bin', [])
                    self.last_sync_time = cache_time
                    return self.todo_list
            except Exception as e:
                print(f"Error reading local cache: {e}")

        # If cache is stale or missing, load from Drive
        file_id = self._find_file_id()
        if not file_id:
            print("todo_list.json not found in Google Drive. Starting with an empty list.")
            return []
        try:
            request = self.sheet_manager.drive_service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                _, done = downloader.next_chunk()

            fh.seek(0)
            data = json.load(fh)
            print("Successfully loaded to-do list from Google Drive.")
            # Load both todo_list and trash_bin
            self.todo_list = data.get('todo_list', [])
            self.trash_bin = data.get('trash_bin', [])
            self.last_sync_time = time.time()

            # Save to local cache
            self._save_to_local_cache()
            return self.todo_list
        except Exception as e:
            print(f"Error loading to-do list from Drive: {e}")
            self.sheet_manager._log_error_to_drive(str(e), "TodoManager._load_list_from_drive")
            return []

    def save_list_to_drive(self):
        """Saves the current to-do list and trash bin to the JSON file in Google Drive with local caching."""
        from googleapiclient.http import MediaFileUpload

        # Save to a temporary local file first
        data_to_save = {
            'todo_list': self.todo_list,
            'trash_bin': self.trash_bin
        }
        with open(self.todo_file_name, 'w', encoding='utf-8') as f:
            json.dump(data_to_save, f, indent=4, ensure_ascii=False)

        media = MediaFileUpload(self.todo_file_name, mimetype='application/json', resumable=True)

        file_id = self._find_file_id()
        try:
            if file_id:
                # Update existing file
                self.sheet_manager.drive_service.files().update(fileId=file_id, media_body=media).execute()
                self.is_dirty = False
                print("✅ To-Do list and trash bin updated in Google Drive.")
            else:
                # Create new file in the root directory
                file_metadata = {'name': self.todo_file_name}
                file = self.sheet_manager.drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
                self.file_id = file.get('id')
                self.is_dirty = False
                print("✅ To-Do list and trash bin created in Google Drive.")

            # Update local cache and sync time
            self.last_sync_time = time.time()
            self._save_to_local_cache()

            if os.path.exists(self.todo_file_name):
                os.remove(self.todo_file_name)
        except Exception as e:
            print(f"❌ Error saving to-do list to Drive: {e}")
            self.sheet_manager._log_error_to_drive(str(e), "TodoManager.save_list_to_drive")

    def generate_list(self, start_date, end_date):
        """
        Generates a smart to-do list, now including check-out indicators on unpaid tasks.
        """
        print(f"--- Generating Smart To-Do List from {start_date} to {end_date} ---")
        # (Data loading and transaction processing remains the same)
        apartments_sheet = "APARTMENTS"
        mo_data_sheet = "MO_DATA"
        ut_data_sheet = "UT_DATA"
        if not all(s in self.sheet_manager.all_data for s in [apartments_sheet, mo_data_sheet, ut_data_sheet]):
            print("❌ Required sheets not loaded.")
            return

        transactions = {}
        mo_header_upper = [str(h).strip().upper() for h in self.sheet_manager.all_data[mo_data_sheet]['header']]
        try:
            mo_code_col = mo_header_upper.index("APARTMENT CODE")
            mo_date_col = mo_header_upper.index("SUBMISSION DATE")
            mo_type_col = mo_header_upper.index("TYPE OF MONEY TASK")
            mo_specific_task_col = mo_header_upper.index("SPECIFIC MONEY TASK")
        except ValueError as e:
            print(f"❌ Missing required column in MO_DATA: {e}")
            return

        for row in self.sheet_manager.all_data[mo_data_sheet]['data']:
            try:
                if len(row) > max(mo_code_col, mo_date_col, mo_type_col, mo_specific_task_col):
                    ap_code = row[mo_code_col].strip()
                    if not ap_code: continue
                    date_str = row[mo_date_col].strip()
                    trans_type = row[mo_type_col].strip()
                    specific_trans_type = row[mo_specific_task_col].strip()
                    parsed_date = None
                    for fmt in ('%Y-%m-%d %H:%M', '%d/%m/%Y %H:%M:%S', '%Y-%m-%d', '%d/%m/%Y'):
                        try:
                            parsed_date = datetime.strptime(date_str, fmt).date()
                            break 
                        except ValueError: continue
                    if parsed_date:
                        if ap_code not in transactions:
                            transactions[ap_code] = [] 
                        transactions[ap_code].append({'date': parsed_date, 'type': trans_type, 'specific': specific_trans_type})
            except (ValueError, IndexError): continue
        
        for ap_code in transactions:
            transactions[ap_code].sort(key=lambda x: x['date'], reverse=True)

        last_ut_dates = {}
        ut_header = self.sheet_manager.all_data[ut_data_sheet]['header']
        ut_code_col = ut_header.index("Apartment Code")
        ut_date_col = ut_header.index("Date of Reading")
        for row in self.sheet_manager.all_data[ut_data_sheet]['data']:
            try:
                ap_code = row[ut_code_col]
                date_str = row[ut_date_col]
                reading_date = None
                for fmt in ('%d/%m/%Y %H:%M:%S', '%d-%m-%Y', '%d/%m/%Y'):
                    try:
                        reading_date = datetime.strptime(date_str, fmt).date()
                        break
                    except (ValueError, TypeError): continue
                if reading_date and (ap_code not in last_ut_dates or reading_date > last_ut_dates[ap_code]):
                    last_ut_dates[ap_code] = reading_date
            except (ValueError, IndexError): continue

        old_state = {item['ap_code'] + (item.get('due_date') or ''): item for item in self.todo_list}
        new_list = []
        
        ap_header = self.sheet_manager.all_data[apartments_sheet]['header']
        ap_code_col = ap_header.index("AP CODE")
        ap_start_col = ap_header.index("START")
        ap_end_col = ap_header.index("END")
        ap_realtor_col = ap_header.index("REALTOR")
        ap_checkout_col = ap_header.index("CHECK_OUT")

        for row in self.sheet_manager.all_data[apartments_sheet]['data']:
            ap_code = row[ap_code_col] if len(row) > ap_code_col else None
            if not ap_code: continue

            realtor_name = row[ap_realtor_col] if len(row) > ap_realtor_col else "Unknown"
            latest_transaction = transactions.get(ap_code, [None])[0]
            last_trans_date = latest_transaction['date'] if latest_transaction else None
            last_trans_type = latest_transaction['type'] if latest_transaction else None
            last_ut_date = last_ut_dates.get(ap_code)

            # --- THIS IS THE NEW LOGIC (Part 1) ---
            # First, determine if there is an upcoming check-out for this apartment
            is_upcoming_checkout = False
            try:
                checkout_str = row[ap_checkout_col] if len(row) > ap_checkout_col else None
                if checkout_str:
                    checkout_date = datetime.strptime(checkout_str, '%d.%m.%Y').date()
                    if start_date <= checkout_date <= end_date:
                        is_upcoming_checkout = True
            except (ValueError, IndexError): pass
            try:
                end_date_str = row[ap_end_col] if len(row) > ap_end_col else None
                if end_date_str:
                    end_date_obj = datetime.strptime(end_date_str, '%d-%m-%Y').date()
                    if start_date <= end_date_obj <= end_date:
                        is_upcoming_checkout = True
            except (ValueError, IndexError): pass
            # --- END OF NEW LOGIC (Part 1) ---

            if last_trans_type and 'CHECK-OUT' in last_trans_type.upper():
                continue 

            # A. Check for Rent Tasks
            try:
                contract_start_date = datetime.strptime(row[ap_start_col], '%d-%m-%Y').date()
                payment_day = contract_start_date.day
                
                all_due_dates_in_range = []
                current_date = start_date
                while current_date <= end_date:
                    if current_date.day == payment_day:
                        all_due_dates_in_range.append(current_date)
                    current_date += timedelta(days=1)

                for i, due_date in enumerate(all_due_dates_in_range):
                    payment_found_date = None
                    if ap_code in transactions:
                        for trans in transactions[ap_code]:
                            is_rent_payment = 'CHECK-IN' in trans['type'].upper() or 'RENT COLLECT' in trans['specific'].upper()
                            # Payment must be within 5 days BEFORE the due date OR up to 3 days AFTER
                            # This prevents old payments from being matched to future rent periods
                            if is_rent_payment and (due_date - timedelta(days=9) <= trans['date'] <= due_date + timedelta(days=5)):
                                payment_found_date = trans['date']
                                break
                    
                    # --- THIS IS THE NEW LOGIC (Part 2) ---
                    # Modify the reason string if it's an upcoming checkout
                    if payment_found_date:
                        reason = f"Rent due {due_date.strftime('%d-%b')}, Paid on {payment_found_date.strftime('%d-%b')}"
                        item = self._create_todo_item(ap_code, reason, due_date=due_date.isoformat(), realtor=realtor_name, status="Paid")
                    else:
                        reason = f"Rent due on {due_date.strftime('%d-%b')} (UNPAID)"
                        if is_upcoming_checkout:
                            reason += " - 🚪 CHECK-OUT" # Add the indicator
                        item = self._create_todo_item(ap_code, reason, due_date=due_date.isoformat(), realtor=realtor_name, status="Unpaid")
                    # --- END OF NEW LOGIC (Part 2) ---
                    
                    item['last_transaction_date'] = last_trans_date.isoformat() if last_trans_date else None
                    item['last_transaction_type'] = last_trans_type
                    item['last_ut_reading_date'] = last_ut_date.isoformat() if last_ut_date else None
                    new_list.append(item)

            except (ValueError, IndexError): pass

            # B. Check for upcoming Check-outs (This logic remains to populate the Check-Outs tab)
            checkout_dates_to_check = []
            try:
                checkout_dates_to_check.append(datetime.strptime(row[ap_end_col], '%d-%m-%Y').date())
            except (ValueError, IndexError): pass
            try:
                checkout_dates_to_check.append(datetime.strptime(row[ap_checkout_col], '%d.%m.%Y').date())
            except (ValueError, IndexError): pass
            
            for checkout_date in set(checkout_dates_to_check):
                if start_date <= checkout_date <= end_date:
                    if not any(item['ap_code'] == ap_code and item['status'] == 'Check-out' for item in new_list):
                        reason = f"Check-out on {checkout_date.strftime('%d-%b')}"
                        item = self._create_todo_item(ap_code, reason, due_date=checkout_date.isoformat(), realtor=realtor_name, status="Check-out")
                        item['last_transaction_date'] = last_trans_date.isoformat() if last_trans_date else None
                        item['last_transaction_type'] = last_trans_type
                        item['last_ut_reading_date'] = last_ut_date.isoformat() if last_ut_date else None
                        new_list.append(item)

        # --- (Merging with old state remains the same) ---
        final_list = []
        for new_item in new_list:
            item_key = new_item['ap_code'] + (new_item.get('due_date') or '')
            if item_key in old_state:
                old_item = old_state[item_key]
                new_item['checked'] = old_item.get('checked', new_item['checked'])
                new_item['note'] = old_item.get('note', new_item['note'])
                new_item['check_time'] = old_item.get('check_time', new_item['check_time'])
            final_list.append(new_item)

        for ap_code, old_item in old_state.items():
            if old_item.get('manual') and not any(x['ap_code'] == old_item['ap_code'] for x in final_list):
                final_list.append(old_item)

        self.todo_list = final_list
        self._sort_list()
        
        self.save_list_to_drive()

    def reload_from_drive(self):
        """Forces a reload of the todo list from Google Drive, bypassing cache."""
        print("Reloading todo list from Google Drive...")
        self.todo_list = self._load_list_from_drive()
        self._sort_list()
        return True, "✅ Todo list reloaded from Google Drive."

    def _save_to_local_cache(self):
        """Saves the current todo list to local cache."""
        try:
            cache_data = {
                'timestamp': self.last_sync_time,
                'todo_list': self.todo_list,
                'trash_bin': self.trash_bin
            }
            with open(self.local_cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, indent=4, ensure_ascii=False)
        except Exception as e:
            print(f"Error saving to local cache: {e}")

    def refresh_context_data_for_list(self):
        """
        Efficiently refreshes only the context data (last transaction, last UT reading)
        for the existing to-do list items without regenerating the entire list.
        """
        print("--- [Context Refresh] Starting fast refresh of task data... ---")
        
        # 1. Force a quick reload of only the necessary sheets
        self.sheet_manager.reload_specific_sheet("MO_DATA")
        self.sheet_manager.reload_specific_sheet("UT_DATA")
        self.sheet_manager.reload_specific_sheet("EMAIL_LOG")

        # 2. Re-process the data from these sheets into dictionaries (similar to generate_list)
        transactions = {}
        mo_header_upper = [str(h).strip().upper() for h in self.sheet_manager.all_data['MO_DATA']['header']]
        try:
            mo_code_col = mo_header_upper.index("APARTMENT CODE")
            mo_date_col = mo_header_upper.index("SUBMISSION DATE")
            mo_type_col = mo_header_upper.index("TYPE OF MONEY TASK")
            mo_specific_task_col = mo_header_upper.index("SPECIFIC MONEY TASK")
            for row in self.sheet_manager.all_data['MO_DATA']['data']:
                if len(row) > max(mo_code_col, mo_date_col, mo_type_col, mo_specific_task_col):
                    ap_code = row[mo_code_col].strip()
                    if not ap_code: continue
                    date_str = row[mo_date_col].strip()
                    trans_type = row[mo_type_col].strip()
                    specific_trans_type = row[mo_specific_task_col].strip()
                    parsed_date = None
                    for fmt in ('%Y-%m-%d %H:%M', '%d/%m/%Y %H:%M:%S', '%Y-%m-%d', '%d/%m/%Y'):
                        try:
                            parsed_date = datetime.strptime(date_str, fmt).date()
                            break
                        except ValueError: continue
                    if parsed_date:
                        if ap_code not in transactions: transactions[ap_code] = []
                        transactions[ap_code].append({'date': parsed_date, 'type': trans_type, 'specific': specific_trans_type})
            for ap_code in transactions:
                transactions[ap_code].sort(key=lambda x: x['date'], reverse=True)
        except ValueError:
            print("WARNING: Could not process MO_DATA during context refresh.")
            pass

        last_ut_dates = {}
        ut_header = self.sheet_manager.all_data['UT_DATA']['header']
        try:
            ut_code_col = ut_header.index("Apartment Code")
            ut_date_col = ut_header.index("Date of Reading")
            for row in self.sheet_manager.all_data['UT_DATA']['data']:
                # THIS IS THE FIX: Check if the row is valid before processing
                if len(row) > ut_code_col and row[ut_code_col]:
                    ap_code = row[ut_code_col].strip()
                    date_str = row[ut_date_col] if len(row) > ut_date_col else ""
                    reading_date = None
                    for fmt in ('%d/%m/%Y %H:%M:%S', '%d-%m-%Y', '%d/%m/%Y'):
                        try:
                            reading_date = datetime.strptime(date_str, fmt).date()
                            break
                        except (ValueError, TypeError): continue
                    if reading_date and (ap_code not in last_ut_dates or reading_date > last_ut_dates[ap_code]):
                        last_ut_dates[ap_code] = reading_date
        except ValueError:
            print("WARNING: Could not process UT_DATA during context refresh.")
            pass

        # 3. Loop through the EXISTING to-do list and update the context fields
        for item in self.todo_list:
            ap_code = item['ap_code']
            
            latest_transaction = transactions.get(ap_code, [None])[0]
            item['last_transaction_date'] = latest_transaction['date'].isoformat() if latest_transaction else None
            item['last_transaction_type'] = latest_transaction['type'] if latest_transaction else None
            
            last_ut_date = last_ut_dates.get(ap_code)
            item['last_ut_reading_date'] = last_ut_date.isoformat() if last_ut_date else None
        
        print("--- [Context Refresh] Fast refresh complete. ---")
        return True, "✅ Task data has been refreshed."

         
    def add_manual_item(self, ap_code, due_date=None):
        """
        Adds a new item to the list manually, fetching its realtor and latest
        transaction/utility data for context.
        """
        due_date_iso = due_date.isoformat() if due_date else None
        item_key = ap_code + (due_date_iso or '')

        if any(item['ap_code'] + (item.get('due_date') or '') == item_key for item in self.todo_list):
            return False, f"A task for `{ap_code}` with that due date already exists."
        
        apartment_data = self.sheet_manager.get_apartment_data(ap_code)
        realtor_name = "Unknown"
        if apartment_data:
            realtor_name = apartment_data.get("REALTOR", "Unknown")

        reason = "Manually added"
        if due_date:
            reason += f" - Due on {due_date.strftime('%d-%b')}"

        item = self._create_todo_item(ap_code, reason, due_date=due_date_iso, realtor=realtor_name)
        item['manual'] = True

        # --- NEW: Fetch context data for the new manual item ---
        # 1. Fetch Last Transaction
        mo_data = self.sheet_manager.all_data.get("MO_DATA", {}).get('data', [])
        mo_header_upper = [str(h).strip().upper() for h in self.sheet_manager.all_data.get("MO_DATA", {}).get('header', [])]
        try:
            mo_code_col = mo_header_upper.index("APARTMENT CODE")
            mo_date_col = mo_header_upper.index("SUBMISSION DATE")
            mo_type_col = mo_header_upper.index("TYPE OF MONEY TASK")
            
            all_trans = []
            for row in mo_data:
                if len(row) > mo_code_col and row[mo_code_col].strip() == ap_code:
                    date_str = row[mo_date_col].strip()
                    trans_type = row[mo_type_col].strip()
                    parsed_date = None
                    for fmt in ('%Y-%m-%d %H:%M', '%d/%m/%Y %H:%M:%S', '%Y-%m-%d', '%d/%m/%Y'):
                        try:
                            parsed_date = datetime.strptime(date_str, fmt).date()
                            break
                        except ValueError:
                            continue
                    if parsed_date:
                        all_trans.append({'date': parsed_date, 'type': trans_type})
            
            if all_trans:
                all_trans.sort(key=lambda x: x['date'], reverse=True)
                latest_transaction = all_trans[0]
                item['last_transaction_date'] = latest_transaction['date'].isoformat()
                item['last_transaction_type'] = latest_transaction['type']
        except ValueError:
            pass # A column was not found, so we can't get transaction data

        # 2. Fetch Last UT Reading
        ut_data = self.sheet_manager.all_data.get("UT_DATA", {}).get('data', [])
        ut_header = self.sheet_manager.all_data.get("UT_DATA", {}).get('header', [])
        try:
            ut_code_col = ut_header.index("Apartment Code")
            ut_date_col = ut_header.index("Date of Reading")
            
            last_ut_date = None
            for row in ut_data:
                if len(row) > ut_code_col and row[ut_code_col].strip() == ap_code:
                    date_str = row[ut_date_col]
                    reading_date = None
                    for fmt in ('%d/%m/%Y %H:%M:%S', '%d-%m-%Y', '%d/%m/%Y'):
                        try:
                            reading_date = datetime.strptime(date_str, fmt).date()
                            break
                        except (ValueError, TypeError):
                            continue
                    if reading_date and (last_ut_date is None or reading_date > last_ut_date):
                        last_ut_date = reading_date
            
            if last_ut_date:
                item['last_ut_reading_date'] = last_ut_date.isoformat()
        except ValueError:
            pass # A column was not found, so we can't get UT data
        # --- END OF NEW LOGIC ---

        self.todo_list.append(item)
        self._sort_list()
        
        self.save_list_to_drive()
        return True, f"✅ `{ap_code}` added to the list."


    def remove_item(self, ap_code, due_date=None):
        """Moves a specific task instance from the todo list to the trash bin."""
        for item in self.todo_list:
            if item['ap_code'] == ap_code and item.get('due_date') == due_date:
                self.todo_list.remove(item)
                self.trash_bin.append(item)
                self.save_list_to_drive()
                return True, f"✅ `{ap_code}` moved to trash bin."
        return False, f"❌ `{ap_code}` was not found on the list."

    def update_checkbox(self, ap_code, checkbox_name, is_checked, due_date=None):
        """Updates a checkbox for a specific task instance IN MEMORY."""
        for item in self.todo_list:
            if item['ap_code'] == ap_code and item.get('due_date') == due_date:
                item['checked'][checkbox_name] = is_checked
                
                return
    
    def update_note(self, ap_code, new_note_text, due_date=None):
        """Updates a note for a specific task instance IN MEMORY."""
        for item in self.todo_list:
            if item['ap_code'] == ap_code and item.get('due_date') == due_date:
                item['note'] = new_note_text
                
                return

    def update_check_time(self, ap_code, due_date=None):
        """Updates the 'check_time' for a specific task instance."""
        for item in self.todo_list:
            if item['ap_code'] == ap_code and item.get('due_date') == due_date:
                item['check_time'] = datetime.now(BUCHAREST_TZ).isoformat()
                self.is_dirty = True
                return

    def restore_item(self, ap_code, due_date=None):
        """Restores a specific task instance from the trash bin back to the todo list."""
        for item in self.trash_bin:
            if item['ap_code'] == ap_code and item.get('due_date') == due_date:
                self.trash_bin.remove(item)
                self.todo_list.append(item)
                self._sort_list()
                self.save_list_to_drive()
                return True, f"✅ `{ap_code}` restored from trash bin."
        return False, f"❌ `{ap_code}` was not found in the trash bin."


    def _create_todo_item(self, ap_code, reason, due_date=None, realtor="Unknown", status="Pending"):
        """Helper to create a new, default to-do item dictionary."""
        return {
            "ap_code": ap_code,
            "reason": reason,
            "due_date": due_date,
            "realtor": realtor,
            "status": status,
            "manual": False,
            "checked": {"TELEGRAM": False, "EMAIL": False, "UT_DATA": False, "WRITE": False},
            "check_time": None,
            "note": "",
            "last_transaction_date": None,
            "last_transaction_type": None, # ADDED: New field for context
            "last_ut_reading_date": None
        }

    def _sort_list(self):
        """
        Sorts the list to show incomplete items first, then by due date.
        Manual items without a due date are prioritized to appear at the top of their sections.
        """
        self.todo_list.sort(key=lambda x: (
            all(x['checked'].values()),  # Completed items (True) go to the bottom
            x.get('due_date') is None,    # Items without a date (manual) come first (False=0)
            x.get('due_date') or ''      # Then sort by the actual due date
        ))
    
    def get_action_items_for_realtor(self, realtor_name: str, limit: int = 5):
        """
        Gets the most urgent, actionable tasks (Unpaid and Check-out) for a specific realtor.
        """
        if not self.todo_list:
            return []

        # Filter for tasks that are assigned to the realtor and are actionable
        actionable_tasks = [
            item for item in self.todo_list
            if item.get('realtor', '').lower() == realtor_name.lower()
            and item.get('status') in ["Unpaid", "Check-out"]
            and not all(item['checked'].values()) # Exclude completed items
        ]

        # Sort the tasks by due date (oldest first)
        actionable_tasks.sort(key=lambda x: x.get('due_date') or '9999-12-31')

        # Return the top N most urgent tasks
        return actionable_tasks[:limit]