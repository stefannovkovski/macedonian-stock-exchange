import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, timedelta


def fetch_data(code, from_date, to_date):
    url = f"https://www.mse.mk/mk/stats/symbolhistory/{code}"
    params = {
        'FromDate': from_date.strftime('%d.%m.%Y'),
        'ToDate': to_date.strftime('%d.%m.%Y'),
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
            print(f"No table found for {code}")
    else:
        print(f"Failed to fetch data for {code} - Status Code: {response.status_code}")
    return None


def update_data_for_codes(code_date_tuples, csv_folder='./csv_data'):
    yesterday = datetime.today() - timedelta(days=1)

    for code, latest_date in code_date_tuples:
        if isinstance(latest_date, str):
            latest_date = pd.to_datetime(latest_date, errors='coerce')

        if pd.isna(latest_date):
            print(f"Invalid date for code {code}: {latest_date}")
            continue

        from_date = latest_date + timedelta(days=1)

        if from_date <= yesterday:
            # print(f"Fetching missing data for {code} from {from_date.strftime('%d.%m.%Y')} to "
            #       f"{yesterday.strftime('%d.%m.%Y')}")

            new_data = fetch_data(code, from_date, yesterday)

            if new_data:
                column_names = ["Date", "PriceOfLastTransaction", "Max", "Min", "AveragePrice", "PercentAvg",
                                "Quantity", "TurnoverBEST", "TotalTurnover"]
                new_df = pd.DataFrame(new_data, columns=column_names)
                csv_file_path = os.path.join(csv_folder, f"{code}.csv")
                new_df.to_csv(csv_file_path, mode='a', header=False, index=False)
                # print(f"Updated data for {code} saved to {csv_file_path}")
            else:
                print(f"No new data found for {code}.")
        else:
            print(f"Data for {code} is already up-to-date.")


