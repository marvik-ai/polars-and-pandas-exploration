import pandas as pd
import time
from typing import List

from modules.utils import extract_id_from_listing_url

def pd_read_csv(file_pathname: str):
  start = time.time()
  loaded_dataframe = pd.read_csv(file_pathname, low_memory=False)
  end = time.time()
  print(f"[Pandas] Elapsed time to read csv: {end-start}s")
  return loaded_dataframe, end-start

def pd_filter_dataset(listings_dataset_pd, desirable_neighboors: List[str]):
  start = time.time()
  filtered = listings_dataset_pd[
    (listings_dataset_pd['neighbourhood_cleansed'].isin(desirable_neighboors))
    &
    (listings_dataset_pd['host_is_superhost'] == 't')
    &
    (listings_dataset_pd['room_type'] == 'Entire home/apt')
  ]
  end = time.time()
  print(f'[Pandas] Elapsed time to filter csv: {end-start}s')
  return filtered, end-start

def sort_by_column(dataset, column_name: str):
  start = time.time()
  sorted = dataset.sort_values(by=[column_name])
  end = time.time()
  print(f'[Pandas] Elapsed time to sort dataframe: {end-start}s')
  return sorted, end-start

def remove_unwanted_columns(dataset, columns_to_remove: list):
  start = time.time()
  dataset.drop(columns = columns_to_remove, inplace=True)
  end = time.time()
  print(f'[Pandas] Elapsed time to remove columns: {end-start}s')
  return end-start

def join_datasets(first_dataset, second_dataset, joining_column_name: str):
  start = time.time()
  first_dataset['listing_id'] = first_dataset['listing_url'].map(extract_id_from_listing_url)

  first_dataset.set_index(joining_column_name, inplace = True)
  second_dataset.set_index(joining_column_name, inplace = True)

  joined_dataset = first_dataset.join(second_dataset, on = joining_column_name, lsuffix='_left', rsuffix='_right')
  end = time.time()
  print(f'[Pandas] Elapsed time to join datasets: {end-start}s')
  return joined_dataset, end-start
