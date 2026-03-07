import requests
import pandas as pd
from datetime import date, timedelta
import sqlalchemy
import yaml

# Extraction
def API_REQUEST(target_date) -> dict:
    headers = {
    'Accept': 'application/json'
    } # state to the api with the api request that the response should be in JSON.
    url = f"https://api.carbonintensity.org.uk/regional/intensity/{target_date}/pt24h"

    response = requests.get(url, headers=headers)
    print(f" request for {target_date} sent and received via {url}")

    return response.json()['data']

# Transformation
# 1. Flattening the data
def DATA_TRANSFORMATION_FLAT(data):
    records = []
    
    for interval in data:
        for region in interval['regions']:
            # create flat dictionary for each region at the 30-mins mark
            row = {
                'regionid': region['regionid'],
                'shortname': region['shortname'],
                'dno': region['dnoregion'],
                'intensity': region['intensity']['forecast'],
                'index': region['intensity']['index']
            }
            # further flatten the generation mix
            for fuel in region['generationmix']:
                row[fuel['fuel']] = fuel['perc']

            records.append(row)
    print(f"Flatten {len(data)} data points into {len(records)} records")
    return records

# 2. Convert flattened data to data frame
def DATA_TRANSFORMATION_DF(flat_data, target_date):
    # Transform to dataframe
    df = pd.DataFrame(flat_data)
    # Aggregate and round to 2 decimal places
    agg_df = df.groupby('regionid').agg({
        'shortname': 'first', # Keeps the name
        'dno': 'first', # keeps the dno
        'intensity': 'mean',
        'index': lambda x: x.mode()[0],
        'biomass': 'mean', 'coal': 'mean', 'imports': 'mean',
        'gas': 'mean', 'nuclear': 'mean', 'other': 'mean',
        'hydro': 'mean', 'solar': 'mean', 'wind': 'mean'
    }).reset_index()

    agg_df['date_recorded'] = target_date - timedelta(days=1)
    
    print(f"Converted and aggregated the flat into dataframe with {len(agg_df)} records for date {target_date - timedelta(days=1)}")
    return agg_df.round(2)

# 3. loading
# Create database connection engine
def CREATE_ENGINE():
    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)
    
    db_url = sqlalchemy.URL.create(
                drivername="postgresql+psycopg2",  # driver
                username=config['user'],
                password=config['password'],
                host=config.get('host', 'localhost'),
                port=config.get('port', 5432),
                database=config['database']
            )
    engine = sqlalchemy.create_engine(db_url)
    print(f"Database engine created for {config['database']} at {config.get('host', 'localhost')}:{config.get('port', 5432)}")
    return engine

def LOAD_TO_DB(df, engine, schema='carbon'):
    # Dim Region: One Time Load
    dim_region = df[['regionid', 'shortname', 'dno']].drop_duplicates()

    # Push to table 'dim_region'
    dim_region.to_sql('dim_region', engine, schema='carbon', if_exists='append', index=False)
    print('Dim Region Updated')

    # Load the Intensity Fact Table
    fact_intensity = df[['regionid', 'date_recorded', 'intensity', 'index']]

    fact_intensity.to_sql('fact_carbon_intensity', engine, schema=schema, if_exists='append', index=False)
    print("fact_carbon_intensity loaded.")

    # Load the Generation Mix Fact Table
    fact_gen_mix = df[['regionid', 'date_recorded', 'biomass', 'coal',
        'imports', 'gas', 'nuclear', 'other', 'hydro', 'solar', 'wind']]

    fact_gen_mix.to_sql('fact_generation_mix', engine, schema=schema, if_exists='append', index=False)
    print("fact_generation_mix loaded.")

# Finally
def main():
    engine = CREATE_ENGINE()
    target_date = date.today()
    print(f"Starting ETL process for target date: {target_date}")
    data = API_REQUEST(target_date)
    flat_data = DATA_TRANSFORMATION_FLAT(data)
    df = DATA_TRANSFORMATION_DF(flat_data, target_date)
    LOAD_TO_DB(df, engine)

if __name__ == "__main__":

    main()
