from collections.abc import Sequence

import gspread


class GoogleTable:
    def __init__(self, spread_url: str) -> None:
        self.gs = gspread.service_account("google_auth/credentials.json")
        self._spread_url = spread_url

    def get_sheet(self) -> gspread.Worksheet:
        spread = self.gs.open_by_url(self._spread_url)
        return spread.sheet1

    def append_row(self, row: Sequence[str | int | float]) -> None:
        sheet = self.get_sheet()
        sheet.append_row(row)
