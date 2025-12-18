# data_registry/utils/google_sheets_writer.py
import os
import json
import time
import jwt
import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from django.conf import settings
import logging

logger = logging.getLogger(__name__)

class GoogleSheetsWriter:
    """
    Запись данных в Google Таблицу через JWT (без официального SDK)
    """

    def __init__(self):
        self.service_account_file = settings.SERVICE_ACCOUNT_FILE

    def _get_http_session(self):
        session = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        session.mount('https://', HTTPAdapter(max_retries=retries))
        return session

    def get_access_token(self) -> str:
        with open(self.service_account_file, 'r') as f:
            sa_data = json.load(f)

        iat = int(time.time())
        exp = iat + 3600
        payload = {
            'iss': sa_data['client_email'],
            'scope': 'https://www.googleapis.com/auth/spreadsheets',
            'aud': 'https://oauth2.googleapis.com/token',
            'exp': exp,
            'iat': iat
        }
        signed_jwt = jwt.encode(payload, sa_data['private_key'], algorithm='RS256')

        session = self._get_http_session()
        resp = session.post(
            'https://oauth2.googleapis.com/token',
            data={
                'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
                'assertion': signed_jwt
            }
        )
        resp.raise_for_status()
        return resp.json()['access_token']

    def write_registry(
        self,
        spreadsheet_id: str,
        sheet_name: str,
        headers: list,
        rows: list
    ):
        """
        Записывает данные в Google Таблицу:
        - Строка 1: пустая (как в вашем Реестре)
        - Строка 2: заголовки
        - Строка 3+: данные
        """
        token = self.get_access_token()
        session = self._get_http_session()
        auth_headers = {'Authorization': f'Bearer {token}'}

        # 1. Очищаем весь лист (A1:Z10000)
        clear_range = f"{sheet_name}!A1:Z10000"
        clear_url = f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values/{clear_range}:clear"
        session.post(clear_url, headers=auth_headers, json={})

        # 2. Формируем тело: строка 1 — пустая, строка 2 — заголовки, далее — данные
        values = [
            [],            # строка 1 — пустая
            headers,       # строка 2 — заголовки
        ] + rows           # строки 3+

        # 3. Записываем
        update_range = f"'{sheet_name}'!A1"
        update_url = (
            f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values/{update_range}"
            f"?valueInputOption=USER_ENTERED"
        )
        body = {"values": values}
        resp = session.put(update_url, headers=auth_headers, json=body)
        resp.raise_for_status()

        logger.info(f"✅ Успешно записано {len(rows)} строк в Google Таблицу")


    def read_full_registry_structure(self, spreadsheet_id: str, sheet_name: str) -> dict:
        """
        Читает весь лист, пропуская первые 2 строки (заголовки).
        Структура:
          - Строка 1: пустая
          - Строка 2: заголовки
          - Строка 3+: данные
        """
        token = self.get_access_token()
        session = self._get_http_session()
        auth_headers = {'Authorization': f'Bearer {token}'}

        url = f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values/{sheet_name}"
        resp = session.get(url, headers=auth_headers)
        resp.raise_for_status()

        data = resp.json()
        values = data.get('values', [])
        returned_range = data.get('range', '')
        print(f"ℹ️ Прочитано {len(values)} строк из диапазона: {returned_range}")

        if len(values) < 3:
            print("⚠️ Недостаточно строк (менее 3) — нет данных")
            return {}

        structure = {}
        current_table = None
        start_row = None

        # Начинаем с 3-й строки (индекс 2 в values, но номер строки = 3)
        for i in range(2, len(values)):
            row = values[i]
            row_number = i + 1  # потому что i=0 → строка 1

            col_a = row[0].strip() if len(row) > 0 and row[0] else ""
            col_b = row[1].strip() if len(row) > 1 and row[1] else ""

            is_empty_separator = (not col_a and not col_b)

            if is_empty_separator:
                if current_table is not None and start_row is not None:
                    structure[current_table] = {"start": start_row, "end": row_number - 1}
                    current_table = None
                    start_row = None
                continue

            # Начало нового блока: B не пустая, и мы вне блока
            if col_b and current_table is None:
                current_table = col_b
                start_row = row_number

        # Завершаем последний блок
        if current_table is not None and start_row is not None:
            structure[current_table] = {"start": start_row, "end": len(values)}

        return structure

    def write_registry_rows(self, spreadsheet_id: str, sheet_name: str, start_row: int, rows: list):
        token = self.get_access_token()
        session = self._get_http_session()
        auth_headers = {'Authorization': f'Bearer {token}'}

        end_row = start_row + len(rows) - 1
        range_str = f"{sheet_name}!A{start_row}:J{end_row}"  # ← до J
        url = f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values/{range_str}?valueInputOption=USER_ENTERED"

        # Убеждаемся, что каждая строка имеет 10 элементов
        normalized_rows = []
        for row in rows:
            row = (row + [""] * 10)[:10]
            normalized_rows.append(row)

        body = {"values": normalized_rows}
        resp = session.put(url, headers=auth_headers, json=body)
        resp.raise_for_status()
        print(f"✅ Записано {len(rows)} строк")

    def read_rows(self, spreadsheet_id: str, sheet_name: str, start_row: int, end_row: int) -> list:
        """Читает строки из Google Таблицы (все колонки)"""
        token = self.get_access_token()
        session = self._get_http_session()
        auth_headers = {'Authorization': f'Bearer {token}'}

        range_str = f"{sheet_name}!A{start_row}:J{end_row}"
        url = f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values/{range_str}"
        resp = session.get(url, headers=auth_headers)
        resp.raise_for_status()

        values = resp.json().get('values', [])
        # Приводим к 10 колонкам (A–J)
        normalized = []
        for row in values:
            row = (row + [""] * 10)[:10]
            normalized.append(row)
        return normalized