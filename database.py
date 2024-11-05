import sqlite3
import os
import csv

database_path = 'main_database.db'
csv_folder_path = './csv_data'


def create_currency_table(conn, table_name):
    cursor = conn.cursor()
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            Date TEXT PRIMARY KEY,
            PriceOfLastTransaction TEXT,
            Max TEXT,
            Min TEXT,
            AvgPrice TEXT,
            PercentChange TEXT,
            Quantity INTEGER,
            TurnoverBEST TEXT,
            TotalTurnover TEXT
        );
    ''')
    conn.commit()


def convert_value(value):
    if value.strip() == '':
        return None
    return value.strip()


def parse_and_insert_data(conn, table_name, csv_file):
    cursor = conn.cursor()
    with open(csv_file, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            date = row['Date']
            last_transaction_price = convert_value(row['PriceOfLastTransaction'])
            max_price = convert_value(row['Max'])
            min_price = convert_value(row['Min'])
            avg_price = convert_value(row['AveragePrice'])
            percent_change = convert_value(row['PercentAvg'])
            quantity_str = row['Quantity']
            quantity = int(float(quantity_str.replace('.', '').replace(',', '.'))) if quantity_str.strip() else 0
            turnover_best = convert_value(row['TurnoverBEST'])
            total_turnover = convert_value(row['TotalTurnover'])

            cursor.execute(f'''
                INSERT OR REPLACE INTO {table_name} (
                    Date, PriceOfLastTransaction, Max, Min, AvgPrice, PercentChange, Quantity, TurnoverBEST, TotalTurnover
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (date, last_transaction_price, max_price, min_price, avg_price, percent_change, quantity, turnover_best, total_turnover))
    conn.commit()


def create_database():
    conn = sqlite3.connect(database_path)
    for filename in os.listdir(csv_folder_path):
        if filename.endswith('.csv'):
            currency_code = os.path.splitext(filename)[0]
            csv_file_path = os.path.join(csv_folder_path, filename)
            create_currency_table(conn, currency_code)
            parse_and_insert_data(conn, currency_code, csv_file_path)
    conn.close()
    print("Database created and populated successfully!")


if __name__ == "__main__":
    create_database()