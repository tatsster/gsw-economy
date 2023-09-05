import os
from src import constant
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


def extractResourcePrice(data: dict, tier: str) -> pd.DataFrame:
    market_df = pd.DataFrame.from_dict(data)

    # Clean up unused markets
    market_df = market_df.drop(columns=["quality"])
    market_df = market_df[market_df["location"].apply(
        lambda x: exclude_cites(x))]
    market_df.reset_index(drop=True, inplace=True)

    merged_df = pd.DataFrame({'timestamp': []})
    # Merge 'data' in each line
    for idx in range(0, len(market_df)):
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
    # Delete all portal columns
    merged_df.drop(columns=columns_delete, inplace=True)

    # Adding tier to col cityName
    renameCols = []
    for i, col in enumerate(merged_df.columns):
        if str(col) != 'timestamp':
            renameCols.append(f'{col} {tier}')
        else:
            renameCols.append(col)
    merged_df.columns = renameCols
    return merged_df


def prettifyDataFrame(df: pd.DataFrame) -> pd.DataFrame:
    # Sort by timestamp
    df = df.sort_values(by='timestamp')
    # Move 'Timestamp' column to the first position
    timestamp_col = df.pop('timestamp')  # Remove the column
    # Reinsert it at the 1st col
    df.insert(0, 'timestamp', timestamp_col)
    df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df.fillna('', inplace=True)
    return df


def fetchResourceByTier(resourceName: str, tier: int) -> pd.DataFrame:
    targetList = config[resourceName]["target"]
    df_tier = pd.DataFrame({'timestamp': []})

    for ench in range(constant.START_ENCHANT, constant.END_ENCHANT + 1):
        for target in targetList:
            isInSearch = False
            if target.startswith(f'T{tier}'):
                if ench == 0 and target.endswith(resourceName.upper()):
                    isInSearch = True
                elif ench != 0 and target.endswith(f'{ench}'):
                    isInSearch = True
            # Only process with correct resource
            if isInSearch:
                api_url = constant.MARKET_URL.format(target=target)
                response = requests.get(api_url)
                if response.status_code == 200:
                    json_data = response.json()
                    df_enchant = extractResourcePrice(
                        json_data, f'T{tier}.{ench}')
                    df_tier = pd.merge(df_tier, df_enchant,
                                       on='timestamp', how='outer')

    return prettifyDataFrame(df_tier)


if __name__ == '__main__':
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

    # authorize the clientsheet
    serviceAccount = gspread.service_account(filename="credentials.json")
    workbook = serviceAccount.open("GSW Macroeconomics")
    sheet1 = workbook.worksheet(config["ore"]["sheetName"])

    for tier in range(constant.START_TIER, constant.END_TIER + 1):
        ore = fetchResourceByTier("ore", tier)
        sheetLine = config["ore"][f't{tier}_line']
        sheet1.update(f"A{sheetLine}", [ore.columns.tolist()])
        sheet1.update(f"A{sheetLine + 1}", ore.values.tolist())
