import requests
from bs4 import BeautifulSoup


def fetch_company_codes(url):
    """
        Fetches a list of company codes from the Macedonian Stock Exchange website.

        Args:
            url (str): The URL of the webpage to scrape. Defaults to the historical data page.

        Returns:
            List[str]: A list of unique company codes.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")

        dropdown = soup.select("#Code")
        if not dropdown:
            raise ValueError("Could not find company codes dropdown on the page.")

        codes = [option.text.strip() for option in dropdown[0]
                 if option.text.strip() and not any(char.isdigit() for char in option.text.strip())]

        return codes
    except requests.RequestException as e:
        print(f"Error fetching company codes: {e}")
        return []
    except ValueError as e:
        print(f"Error parsing company codes: {e}")
        return []