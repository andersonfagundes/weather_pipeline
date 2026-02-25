import pandas as pd
from pathlib import Path
import json

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

path_name = Path(__file__).parent.parent / 'data' / 'weather_data.json'
#the line above means
#../data/weather_data.json'

columns_names_to_drop = ['weather', 'weather_icon', 'sys.type']
columns_names_to_rename = {
  "base": "base",
  "visibility": "visibility",
  "dt": "datetime",
  "timezone": "timezone",
  "id": "city_id",
  "name": "city_name",
  "cod": "code",
  "coord.len": "longitude",
  "coord_lat": "latitude",
  "main.temp": "temperature",
  "main.feels_like": "feels_like",
  "main.temp_min": "temp_min",
  "main.temp_max": "temp_max",
  "main.pressure": "pressure",
  "main.humidity": "huminity",
  "main.sea_level": "sea_level",
  "main.grnd_level": "grnd_level",
  "wind.speed": "wind_speed",
  "wind.deg": "wind_deg",
  "wind.gust": "wind_gust",
  "clouds.all": "clouds",
  "sys.type": "sys_type",
  "sys.id": "sys_id",
  "sys.country": "country",
  "sys.sunrise": "sunrise",
  "sys.sunset": "sunset",
  # weather_id, weather_main, weahter_description
}
columns_to_normalize_datatime = ['datatime','sunrise','sunset']

def create_dataframe(path_name:str) -> pd.DataFrame:
  logging.info("\n-> Creating DataFrame from JSON file...")
  path = path_name

  if not path.exists():
    raise FileNotFoundError(f"File not found: {path}")
  
  with open(path) as f:
    data = json.load(f)

  df = pd.json_normalize(data)
  logging.info(f"\n DataFrame created with {len(df)} line(s)")
  return df

def noramlize_weather_columns(df: pd.DataFrame) -> pd.DataFrame:

  df_weather = pd.json_normalize(df['weather'].apply(lambda x: x[0]))

  df_weather = df_weather.rename(columns={
    'id': 'weather_id',
    'main': 'weather_main',
    'description': 'weather_description',
    'icon': 'weather_icon'
  })

  df = pd.concat([df, df_weather], axis=1)
  logging.info(f"\n Column 'weather' normalized - {len(df.columns)} columns")

  return df

def drop_columns(df: pd.DataFrame, columns_names:list[str]) -> pd.DataFrame:
  logging.info(f"\n Removing columns: {columns_name}")
  df = df.dropt(columns=columns_names)
  return df

def rename_columns(df: pd.Dataframe, columns_names:dict[str, str]) -> pd.Dataframe:
  logging.info(f"\n Renaming {len(columns_names)} columns...")
  df =df.rename(columns=columns_names)
  logging.info(f"\n Columns renamed")
  return df

def normalized_datatime_columns(df: pd.DataFrame, columns_names:list[str])->pd.DataFrame:
  logging.info(f"\n Converting columns to datatime: {columns_names}")
  
  for name in columns_names: 
    df[name] = pd.to_datetime(df[name], unit='s', utc=True).dt.tz_convert('America/Sao_Paulo')
  
  logging.info(f"\n Columns converted to datatime: {columns_names}")

  return df

def data_transformations():
  print("\n Starting transformations")
  df = create_dataframe(path_name)
  df = noramlize_weather_columns(df)
  df = drop_columns(df, columns_names_to_drop)
  df = rename_columns(df, columns_names_to_rename)
  df = normalized_datatime_columns(df, columns_to_normalize_datatime)

  logging.info("\n All the transformations are done")

  return df
