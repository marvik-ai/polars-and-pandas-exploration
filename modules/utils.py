from sys import getsizeof
import re

def get_size_of_dataframe(current_object) -> None:
  size = round(getsizeof(current_object) / 1024 / 1024,2)
  print(f"Current dataframe's size is {size} MB")

def extract_id_from_listing_url(x) -> int:
    return int(re.findall(r'rooms\/(.*)', x)[0])