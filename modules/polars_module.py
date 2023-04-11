from typing import List
import polars
import time

from modules.utils import extract_id_from_listing_url

# Lazy section

def scan_csv(dataframe_filename: str):
  start = time.time()
  polars_df = polars.scan_csv(dataframe_filename, ignore_errors=True)
  end = time.time()
  print(f'[Polars] Elapsed time to scan dataframe: {end-start}s')
  return polars_df, end-start

def filter_dataset(
    lazy_loaded_dataframe: polars.LazyFrame,
    desirable_neighboors: List[str]):

  start = time.time()
  lazy_loaded_dataframe = (
      lazy_loaded_dataframe
      .filter(polars.col('neighbourhood_cleansed').is_in(desirable_neighboors))
      .filter(polars.col('host_is_superhost') == 't')
      .filter(polars.col('room_type') == 'Entire home/apt')
      )
  end = time.time()

  print(f'[Polars] Elapsed time to filter dataset (lazy execution): {end-start}s')
  return lazy_loaded_dataframe, end-start

def get_overall_score(
    lazy_loaded_dataframe: polars.LazyFrame,
    columns_to_add: List[str]):
  return lazy_loaded_dataframe.with_columns(polars.sum(columns_to_add))

def sort_by_column_value_lazy(
    lazy_loaded_dataframe: polars.LazyFrame,
    column_name: str):
  start = time.time()
  sorted_lazy = lazy_loaded_dataframe.sort(column_name)
  end = time.time()
  print(f"[Polars] Elapsed time to perform sorting (lazy execution): {end-start}s")
  return sorted_lazy, end-start

def drop_unwanted_columns_lazy(lazy_loaded_dataframe, columns: List[str]):
  start = time.time()
  lazy_loaded_dataframe = lazy_loaded_dataframe.drop(columns)
  end = time.time()
  print(f"[Polars] Elapsed time to drop {len(columns)} columns (lazy execution): {end-start}s")
  return lazy_loaded_dataframe, end-start

def collect_transformation(
    lazy_loaded_dataframe: polars.LazyFrame):
  start = time.time()
  loaded_dataframe = lazy_loaded_dataframe.collect()
  end = time.time()
  print(f'[Polars] Elapsed time to collect lazy frame: {end-start}s')
  return loaded_dataframe, end-start


# Eager section

def read_csv(dataframe_filename: str):
  start = time.time()
  polars_df = polars.read_csv(dataframe_filename)
  end = time.time()
  print(f"[Polars] Elapsed time to read csv (eager execution) {end-start}s")
  return polars_df, end-start

def remove_unused_columns_eager(loaded_dataframe, not_wanted_columns):
  start = time.time()
  for not_wanted_column in not_wanted_columns:
    loaded_dataframe.drop_in_place(not_wanted_column)
  end = time.time()
  print(f"[Polars] Elapsed time to drop {len(not_wanted_columns)} (eager execution) {end-start}s")
  return end-start

def filter_dataset_eager(loaded_dataframe, desirable_neighboors):
  start = time.time()
  filtered =  loaded_dataframe.filter(
    (polars.col('neighbourhood_cleansed').is_in(desirable_neighboors))
    &
    (polars.col('host_is_superhost') == 't')
    &
    (polars.col('room_type') == 'Entire home/apt')
  )
  end = time.time()
  print(f'[Polars] Elapsed time to perform filters (eager execution): {end-start}')
  return filtered, end-start

def add_column_eager(loaded_dataframe):
  start = time.time()
  df_with_new_column = loaded_dataframe.with_columns(polars.col("listing_url").apply(extract_id_from_listing_url).alias('listing_id'))
  end = time.time()
  print(f'[Polars] Elapsed time to add column (eager execution): {end-start}s')
  return df_with_new_column, end-start

def join_datasets_eager(first_dataset, second_dataset, matching_column_name: str):
  start = time.time()
  joined_df = first_dataset.join(second_dataset, on=matching_column_name, how='inner')
  end = time.time()
  print(f"[Polars] Elapsed time to perform join (eager execution): {end-start}s")
  return joined_df, end-start

def migrate_to_pandas(loaded_dataframe):
  start = time.time()
  migrated_df = loaded_dataframe.to_pandas()
  end = time.time()
  print(f"[Polars] Elapsed time to perform migration (eager execution): {end-start}s")
  return migrated_df, end-start

def sort_by_column_value_eager(loaded_dataframe, column_name: str):
  start = time.time()
  sorted_df = loaded_dataframe.sort(column_name)
  end = time.time()
  print(f"[Polars] Elapsed time to perform sorting (eager execution): {end-start}s")
  return sorted_df, end-start

