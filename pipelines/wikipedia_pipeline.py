import json
import pandas as pd
from geopy import Nominatim
from geopy.extra.rate_limiter import RateLimiter


NO_IMAGE = 'https://upload.wikimedia.org/wikipedia/commons/thumb/0/0a/No-image-available.png/480px-No-image-available.png'


# ===============================
# 1️⃣ Extract: Get Wikipedia Page
# ===============================
def get_wikipedia_page(url):
    import requests

    print("🌐 Getting Wikipedia page:", url)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/122.0.0.0 Safari/537.36"
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()  # check if successful
        print("✅ Page fetched successfully.")
        return response.text
    except requests.RequestException as e:
        print(f"❌ Error fetching Wikipedia page: {e}")
        return None


# =====================================
# 2️⃣ Parse: Extract Table from Wikipedia
# =====================================
def get_wikipedia_data(html):
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, 'html.parser')
    tables = soup.find_all("table", class_="wikitable")

    if not tables:
        raise ValueError("❌ No 'wikitable' found in the Wikipedia page!")

    # ✅ Pick the largest table (most rows)
    largest_table = max(tables, key=lambda t: len(t.find_all('tr')))
    table_rows = largest_table.find_all('tr')

    print(f"✅ Found {len(table_rows)} rows in the largest table.")
    return table_rows


# =======================
# 3️⃣ Data Cleaning Helper
# =======================
def clean_text(text):
    text = str(text).strip()
    text = text.replace('&nbsp', '')
    if ' ♦' in text:
        text = text.split(' ♦')[0]
    if '[' in text:
        text = text.split('[')[0]
    if ' (formerly)' in text:
        text = text.split(' (formerly)')[0]
    return text.replace('\n', '')


# ==============================================
# 4️⃣ Extract Task: Convert HTML → Cleaned Records
# ==============================================
def extract_wikipedia_data(**kwargs):
    url = kwargs['url']
    html = get_wikipedia_page(url)
    if not html:
        raise ValueError("❌ Failed to fetch Wikipedia page content.")

    rows = get_wikipedia_data(html)
    data = []

    for i, row in enumerate(rows[1:], start=1):  # skip header
        tds = row.find_all('td')

        # Skip rows that don’t have enough columns
        if len(tds) < 6:
            print(f"⚠️ Skipping row {i} — only {len(tds)} columns found")
            continue

        try:
            values = {
                'rank': i,
                'stadium': clean_text(tds[0].text if len(tds) > 0 else ""),
                'capacity': clean_text(tds[1].text if len(tds) > 1 else "").replace(',', '').replace('.', ''),
                'region': clean_text(tds[2].text if len(tds) > 2 else ""),
                'country': clean_text(tds[3].text if len(tds) > 3 else ""),
                'city': clean_text(tds[4].text if len(tds) > 4 else ""),
                'images': (
                    'https://' + tds[5].find('img').get('src').split("//")[1]
                    if len(tds) > 5 and tds[5].find('img')
                    else NO_IMAGE
                ),
                'home_team': clean_text(tds[6].text if len(tds) > 6 else ""),
            }
            data.append(values)
        except Exception as e:
            print(f"⚠️ Error parsing row {i}: {e}")
            continue

    # Push to XCom
    json_rows = json.dumps(data)
    kwargs['ti'].xcom_push(key='rows', value=json_rows)
    #df = pd.DataFrame(data)
    #df.to_csv('data/debug_extracted_data.csv', index=False)  # Debug output

    print(f"✅ Extracted {len(data)} valid stadium rows from Wikipedia.")
    print(data)
    return "OK"


# ===================================
# 5️⃣ Transform: Geocode & Clean Data
# ===================================
def get_lat_long(country, city):
    geolocator = Nominatim(user_agent="my_stadium_locator_v1")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1, max_retries=2)

    try:
        location = geocode(f"{city}, {country}", timeout=10)
        if location:
            return location.latitude, location.longitude
        else:
            print(f"⚠️ No location found for {city}, {country}")
            return None
    except Exception as e:
        print(f"⚠️ Geocoding failed for {city}, {country}: {e}")
        return None


def transform_wikipedia_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='extract_data_from_wikipedia')
    data = json.loads(data)

    stadiums_df = pd.DataFrame(data)
    if stadiums_df.empty:
        print("⚠️ No data found to transform.")
        return "NO_DATA"

    stadiums_df['location'] = stadiums_df.apply(
        lambda x: get_lat_long(x['country'], x['stadium']), axis=1
    )
    stadiums_df['images'] = stadiums_df['images'].apply(
        lambda x: x if x not in ['NO_IMAGE', '', None] else NO_IMAGE
    )

    # Clean numeric capacity
    stadiums_df['capacity'] = pd.to_numeric(stadiums_df['capacity'], errors='coerce').fillna(0).astype(int)

    # Handle duplicates (same coordinates)
    duplicates = stadiums_df[stadiums_df.duplicated(['location'])]
    if not duplicates.empty:
        duplicates['location'] = duplicates.apply(
            lambda x: get_lat_long(x['country'], x['city']), axis=1
        )
        stadiums_df.update(duplicates)

    kwargs['ti'].xcom_push(key='rows', value=stadiums_df.to_json())
    print(f"✅ Transformed {len(stadiums_df)} records successfully.")
    return "OK"


# ======================
# 6️⃣ Load: Save as CSV
# ======================
def write_wikipedia_data(**kwargs):
    from datetime import datetime

    data = kwargs['ti'].xcom_pull(key='rows', task_ids='transform_wikipedia_data')
    data = json.loads(data)
    df = pd.DataFrame(data)

    if df.empty:
        print("⚠️ No data to save.")
        return "NO_DATA"

    file_name = (
        'stadium_cleaned_' + str(datetime.now().date())
        + "_" + str(datetime.now().time()).replace(":", "_") + '.csv'
    )

    #output_path = 'data/' + file_name
    #df.to_csv(output_path, index=False)

    df.to_csv('abfs://footballdata@dataengfootball.dfs.core.windows.net/data/' + file_name,
                storage_options={
                    'account_key': 'your_key_here'
                }, index=False)
    #print(f"✅ Saved cleaned data to: {output_path}")

    return "OK"
