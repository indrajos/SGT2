# Distributed Web Scraping:

# Select a website. Distribute web scraping tasks across multiple processes or threads using libraries
# like concurrent.futures or Scrapy to gather data from various websites simultaneously.

import requests
import concurrent.futures


def fetch_apod(url):
    response = requests.get(url)
    data = response.json()
    return data


if __name__ == "__main__":
    api_key = "DEMO_KEY"  # Replace with your actual API key
    base_url = f"https://api.nasa.gov/planetary/apod?api_key={api_key}"

    dates = ["2023-08-25", "2023-08-24", "2023-08-23"]

    urls = [f"{base_url}&date={date}" for date in dates]

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(urls)) as executor:
        results = executor.map(fetch_apod, urls)

    for result in results:
        print(result)
