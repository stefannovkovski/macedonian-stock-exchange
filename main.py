from data_pipeline.filter1_fetch_codes import fetch_company_codes
from data_pipeline.filter2_check_data import check_latest_data, fill_csv_data
from data_pipeline.filter3_fill_missing import update_data_for_codes
from database import create_database
import time


def main():
    start_time = time.time()

    company_codes = fetch_company_codes(url="https://www.mse.mk/mk/stats/symbolhistory/REPL")

    fill_csv_data(company_codes)

    outdated_companies = check_latest_data('./csv_data', company_codes)

    update_data_for_codes(outdated_companies, './csv_data')

    create_database()

    print(f"Pipeline execution time: {time.time() - start_time:.2f} seconds")


if __name__ == "__main__":
    main()