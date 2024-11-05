import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime


def fetch_data(code, year):
    url = f"https://www.mse.mk/mk/stats/symbolhistory/{code}"
    params = {
        'FromDate': f'01.01.{year}',
        'ToDate': f'31.12.{year}',
    }
    response = requests.get(url, params=params)

    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')

        table = soup.find('table')
        if table:
            rows = table.find_all('tr')
            data = []
            for row in rows[1:]:
                cols = row.find_all('td')
                cols = [col.text.strip() for col in cols]
                if cols:
                    data.append(cols)

            return data
    else:
        print(f"Failed to fetch data for {code} in {year} - Status Code: {response.status_code}")
        return None


def check_latest_data(csv_folder, codes):
    outdated_data = []
    today = datetime.today()

    for code in codes:
        csv_file_path = os.path.join(csv_folder, f"{code}.csv")
        if os.path.exists(csv_file_path):
            df = pd.read_csv(csv_file_path).tail(31)

            df['Date'] = pd.to_datetime(df['Date'], format='%d.%m.%Y', errors='coerce')
            latest_data = df['Date'].max()

            if latest_data < today:
                outdated_data.append((code, latest_data))

    return outdated_data


def fill_csv_data(company_codes):
    codes = company_codes
    csv_folder = './csv_data'
    os.makedirs(csv_folder, exist_ok=True)

    for code in codes:
        all_data = []

        for year in range(2014, 2024):
            data = fetch_data(code, year)
            if data:
                all_data.extend(data)

        if all_data:
            column_names = ["Date", "PriceOfLastTransaction", "Max", "Min", "AveragePrice", "PercentAvg", "Quantity",
                            "TurnoverBEST", "TotalTurnover"]
            df = pd.DataFrame(all_data, columns=column_names)

            csv_file_path = os.path.join(csv_folder, f'{code}.csv')
            df.to_csv(csv_file_path, index=False)
            # print(f"Data for {code} saved to {csv_file_path}")