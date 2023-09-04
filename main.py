import os
import requests
import gspread
import yaml
import pandas as pd


# Load the configuration from the YAML file
with open(os.path.join("config", "items.yaml"), 'r') as config_file:
    config = yaml.safe_load(config_file)

"""
Exclude number city name or Caerleon-related
"""
def exclude_cites(cityName: str):
    return any(city in cityName for city in config["allowed_cities"])


def genSearchStr(resource: str, tier=5):
    rescName = config[resource]["target"]
    rescName = list(filter(lambda x: x.startswith(f'T{tier}'), rescName))
    return ','.join(rescName)


def handleResource(data: dict) -> pd.DataFrame:
    market_df = pd.DataFrame.from_dict(data)

    # Clean up unused markets
    market_df = market_df.drop(columns=["quality"])
    market_df = market_df[market_df["location"].apply(lambda x: exclude_cites(x))]
    market_df.reset_index(drop=True, inplace=True)

    # From 'data' 1st line -> create Result Dataframe
    city_json = market_df.at[0, "data"]
    city_df = pd.DataFrame(city_json)
    city_df['timestamp'] = pd.to_datetime(city_df["timestamp"])
    city_df = city_df.rename(
        columns={"avg_price": market_df.at[0, "location"]})
    merged_df = city_df.drop(columns=["item_count"])

    # Merge 'data' in each line 
    for idx in range(1, len(market_df)):
        city_json = market_df.at[idx, "data"]
        city_df = pd.DataFrame(city_json)
        city_df['timestamp'] = pd.to_datetime(city_df["timestamp"])
        city_df = city_df.rename(
            columns={"avg_price": market_df.at[idx, "location"]})
        city_df = city_df.drop(columns=["item_count"])

        # Progressively merge
        merged_df = pd.merge(merged_df, city_df,
                                on='timestamp', how='outer')

    # Merge city & portal market into 1
    columns_delete = []
    for column in merged_df.columns:
        parts = column.split(' Portal')
        if len(parts) == 2:
            city, portal = parts[0], column
            merged_df[city] = merged_df.apply(
                lambda row: row[column] if row[city] == '' else row[city], axis=1)
            columns_delete.append(portal)
    merged_df.drop(columns=columns_delete, inplace=True)

    # Sort by timestamp
    merged_df = merged_df.sort_values(by='timestamp')
    # Move 'Timestamp' column to the first position
    timestamp_col = merged_df.pop('timestamp')  # Remove the column
    # Reinsert it at the 1st col
    merged_df.insert(0, 'timestamp', timestamp_col)
    merged_df['timestamp'] = merged_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
    merged_df.fillna('', inplace=True)

    return merged_df


if __name__ == '__main__':
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

    # authorize the clientsheet
    serviceAccount = gspread.service_account(filename="credentials.json")
    workbook = serviceAccount.open("GSW Macroeconomics")
    sheet1 = workbook.worksheet(config["ore"]["sheetName"])

    # Get API
    api_url = "https://east.albion-online-data.com/api/v2/stats/history/T5_ORE?time-scale=1"
    response = requests.get(api_url)

    if response.status_code == 200:
        json_data = response.json()
        df = handleResource(json_data)
        
        sheet1.update("A1", [df.columns.tolist()])
        sheet1.update("A2", df.values.tolist())
