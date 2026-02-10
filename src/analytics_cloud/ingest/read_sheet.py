import os
from typing import List

import gspread
from google.oauth2.service_account import Credentials


def read_sheet_preview() -> List[List[str]]:
    # Where the service account key lives (local dev)
    key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "secrets/google_service_account.json")

    spreadsheet_id = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID")
    worksheet_name = os.getenv("GOOGLE_SHEETS_WORKSHEET_NAME", "Tickets")
    cell_range = os.getenv("GOOGLE_SHEETS_RANGE", "A:Z")

    if not spreadsheet_id:
        raise RuntimeError(
            "Missing env var GOOGLE_SHEETS_SPREADSHEET_ID. "
            "Get it from the Google Sheets URL (the long id between /d/ and /edit) "
            "and export it before running."
        )

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    creds = Credentials.from_service_account_file(key_path, scopes=scopes)
    client = gspread.authorize(creds)

    sh = client.open_by_key(spreadsheet_id)
    ws = sh.worksheet(worksheet_name)

    # Pull data
    values = ws.get(cell_range)

    return values


def main() -> None:
    values = read_sheet_preview()

    # Print a preview (first 5 rows)
    print(f"[sheets] rows fetched: {len(values)}")
    for i, row in enumerate(values[:5], start=1):
        print(f"[sheets] row {i}: {row}")


if __name__ == "__main__":
    main()
