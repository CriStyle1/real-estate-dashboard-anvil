import anvil.server
import anvil.secrets
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
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

# --- Anvil Uplink Initialization ---
# Make sure to set your Anvil Uplink key in the Anvil app settings
# anvil.server.connect("your_anvil_uplink_key")

# --- Constants ---
BUCHAREST_TZ = pytz.timezone("Europe/Bucharest")

# --- Google API Authentication ---
def get_google_creds():
    creds_json_str = anvil.secrets.get_secret("google_credentials_json")
    creds_dict = json.loads(creds_json_str)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ])
    return creds

# --- GoogleSheet Class ---
class GoogleSheet:
    def __init__(self, spreadsheet_name: str, sheet_names: list):
        self.spreadsheet_name = spreadsheet_name
        self.sheet_names = sheet_names
        self.creds = get_google_creds()
        self.client = gspread.authorize(self.creds)
        self.spreadsheet = self.client.open(self.spreadsheet_name)
        self.drive_service = build('drive', 'v3', credentials=self.creds)
        self.all_data = {}
        self.sheet_data_timestamp = None
        self.settings_file = "drive_module_settings.json"
        self.settings = self._load_settings()

    def _load_settings(self) -> dict:
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
                    defaults.update(settings)
                    return defaults
            except (json.JSONDecodeError, TypeError):
                return defaults
        return defaults

    def save_settings(self):
        with open(self.settings_file, 'w') as f:
            json.dump(self.settings, f, indent=4)

    def sync_exchange_rate(self, silent: bool = False):
        try:
            url = "https://www.cursbnr.ro/"
            response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            eur_row = soup.find('td', string='EUR').find_parent('tr')
            rate_cell = eur_row.find_all('td')[2]
            base_rate = float(rate_cell.text.strip())
            rate_with_margin = round(base_rate * 1.003, 4)
            current_rate = self.settings.get("eur_to_ron_rate")
            if current_rate != rate_with_margin:
                self.settings["eur_to_ron_rate"] = rate_with_margin
                self.save_settings()
                return True, f"Exchange rate updated to {self.settings['eur_to_ron_rate']}."
            else:
                return True, f"Exchange rate is already up-to-date ({self.settings['eur_to_ron_rate']})."
        except Exception as e:
            return False, f"Failed to sync exchange rate: {e}"

    def load_data(self):
        if self.sheet_data_timestamp and (datetime.now(pytz.utc) - self.sheet_data_timestamp).total_seconds() < 600:
            return
        for sheet_name in self.sheet_names:
            worksheet = self.spreadsheet.worksheet(sheet_name)
            header = worksheet.row_values(1)
            data = worksheet.get_all_records(expected_headers=header)
            self.all_data[sheet_name] = {
                'header': header,
                'data': data
            }
        self.sheet_data_timestamp = datetime.now(pytz.utc)

    def get_row_by_code(self, ap_code: str, sheet_name: str, code_column_name: str) -> dict | None:
        if sheet_name not in self.all_data:
            return None
        for row in self.all_data[sheet_name]['data']:
            if row.get(code_column_name) == ap_code:
                return row
        return None

    def get_apartment_data(self, ap_code: str, sheet_name: str = "APARTMENTS") -> dict | None:
        return self.get_row_by_code(ap_code, sheet_name, "AP CODE")

    def get_apartments_by_realtor(self, realtor_name: str) -> list:
        sheet_name = "APARTMENTS"
        if sheet_name not in self.all_data: return []
        return [row['AP CODE'] for row in self.all_data[sheet_name]['data'] if row.get('REALTOR') == realtor_name]

    # ... other GoogleSheet methods ...

# --- TodoManager Class ---
class TodoManager:
    def __init__(self, sheet_manager: GoogleSheet):
        self.sheet_manager = sheet_manager
        self.todo_file_name = "todo_list.json"
        self.file_id = None
        self.todo_list = self._load_list_from_drive()
        self.trash_bin = []

    def _find_file_id(self):
        if self.file_id:
            return self.file_id
        try:
            query = f"name = '{self.todo_file_name}' and 'root' in parents and trashed = false"
            results = self.sheet_manager.drive_service.files().list(q=query, fields="files(id)").execute()
            items = results.get("files", [])
            if items:
                self.file_id = items[0]["id"]
                return self.file_id
        except HttpError as error:
            print(f"An error occurred: {error}")
            self.file_id = None
        return None

    def _load_list_from_drive(self) -> list:
        file_id = self._find_file_id()
        if not file_id:
            return []
        try:
            request = self.sheet_manager.drive_service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
            fh.seek(0)
            data = json.load(fh)
            self.todo_list = data.get('todo_list', [])
            self.trash_bin = data.get('trash_bin', [])
            return self.todo_list
        except HttpError as error:
            print(f"An error occurred: {error}")
            return []

    def save_list_to_drive(self):
        file_id = self._find_file_id()
        data_to_save = {
            'todo_list': self.todo_list,
            'trash_bin': self.trash_bin
        }
        with open(self.todo_file_name, 'w', encoding='utf-8') as f:
            json.dump(data_to_save, f, indent=4, ensure_ascii=False)
        
        media = MediaFileUpload(self.todo_file_name, mimetype='application/json')
        
        if file_id:
            self.sheet_manager.drive_service.files().update(fileId=file_id, media_body=media).execute()
        else:
            file_metadata = {'name': self.todo_file_name}
            file = self.sheet_manager.drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
            self.file_id = file.get('id')

    # ... other TodoManager methods ...

# --- Global Instances ---
sheet_manager = GoogleSheet(spreadsheet_name="APARTMENTS", sheet_names=["APARTMENTS", "UT_DATA", "EMAIL_LOG", "ZET", "STR", "MO_DATA", "CL_DATA"])
sheet_manager.load_data()
todo_manager = TodoManager(sheet_manager)

# --- Anvil Server Functions ---
@anvil.server.callable
def get_all_ap_codes():
    return ALL_AP_CODES

@anvil.server.callable
def load_translations(language):
    try:
        df = pd.read_csv("translations.csv", encoding="utf-8")
        if language in df.columns:
            return dict(zip(df['key'], df[language]))
        else:
            raise ValueError(f"Language '{language}' not supported")
    except FileNotFoundError:
        raise FileNotFoundError("translations.csv is missing")
    except pd.errors.ParserError as e:
        raise pd.errors.ParserError(f"Failed to parse translations.csv: {str(e)}")
    except Exception as e:
        raise Exception(f"Unexpected error: {str(e)}")

@anvil.server.callable
def get_apartment_data(ap_code):
    return sheet_manager.get_apartment_data(ap_code)

@anvil.server.callable
def get_apartments_by_realtor(realtor_name):
    return sheet_manager.get_apartments_by_realtor(realtor_name)

@anvil.server.callable
def calculate_and_format_utilities(ap_code, include_rent):
    return sheet_manager.calculate_and_format_utilities(ap_code, include_rent)

@anvil.server.callable
def generate_master_report(ap_code):
    return sheet_manager.generate_master_report(ap_code)

@anvil.server.callable
def format_email_log(ap_code):
    return sheet_manager.format_email_log(ap_code)

@anvil.server.callable
def generate_batch_report(codes_to_process):
    return sheet_manager.generate_batch_report(codes_to_process)

@anvil.server.callable
def find_ap_code_by_email(email):
    return sheet_manager.find_ap_code_by_email(email)

@anvil.server.callable
def find_email_by_ap_code(ap_code):
    return sheet_manager.find_email_by_ap_code(ap_code)

@anvil.server.callable
def get_todo_list():
    return todo_manager.todo_list

@anvil.server.callable
def get_trash_bin():
    return todo_manager.trash_bin

@anvil.server.callable
def generate_todo_list(start_date, end_date):
    todo_manager.generate_list(start_date, end_date)

@anvil.server.callable
def add_manual_todo_item(ap_code, due_date):
    return todo_manager.add_manual_item(ap_code, due_date)

@anvil.server.callable
def remove_todo_item(ap_code, due_date):
    return todo_manager.remove_item(ap_code, due_date)

@anvil.server.callable
def restore_todo_item(ap_code, due_date):
    return todo_manager.restore_item(ap_code, due_date)

@anvil.server.callable
def update_todo_checkbox(ap_code, checkbox_name, is_checked, due_date):
    todo_manager.update_checkbox(ap_code, checkbox_name, is_checked, due_date)

@anvil.server.callable
def update_todo_note(ap_code, new_note_text, due_date):
    todo_manager.update_note(ap_code, new_note_text, due_date)

@anvil.server.callable
def save_todo_list():
    todo_manager.save_list_to_drive()

@anvil.server.callable
def reload_todo_list():
    return todo_manager.reload_from_drive()

@anvil.server.callable
def get_settings():
    return sheet_manager.settings

@anvil.server.callable
def save_settings(settings):
    sheet_manager.settings = settings
    sheet_manager.save_settings()

@anvil.server.callable
def sync_exchange_rate():
    return sheet_manager.sync_exchange_rate()

@anvil.server.callable
def reload_all_data():
    return sheet_manager.reload_all_data()

if __name__ == "__main__":
    anvil.server.wait_forever()
