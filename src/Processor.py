from project_config import (
    SPLIT_DATA_FOLDER,
    TEMP_FOLDER,
    ARCHIVE_FOLDER
)
# from functools import reduce
from datetime import datetime
from typing import List
from os import listdir, rename


class Processor:
    def __init__(self,
                 required_years: str,
                 location: str):
        self.required_years = required_years
        self.location = location

    def process_month_and_year(self) -> None:
        """Filters through timestamps and stores the indexes"""
        if self.required_years == '1':
            dt_to_check = datetime(2011, 1, 1, 0, 0)
        else:
            dt_to_check = datetime(2000 + int(self.required_years), 1, 1, 0, 0)
        with open(f'{SPLIT_DATA_FOLDER}/Timestamp.txt', 'r') as f:
            lines = f.read().splitlines()
        lowest_idx = binary_search(
            dt_to_check=dt_to_check,
            lines=lines
        )
        # splitting into different years and months
        years = [year for year in range(2002, 2022)
                 if year % 10 == int(self.required_years)]
        opened_files = [open(f'{TEMP_FOLDER}/Timestamp_{year}_{i}.txt', 'w')
                        for year in years
                        for i in range(1, 13)]
        for i in range(lowest_idx, len(lines)):
            line = lines[i]
            if line[3] != self.required_years:
                continue
            year, month = int(line[:4]), int(line[5:7])
            year_idx = years.index(year)
            file_idx = 12 * year_idx + (month - 1)
            opened_files[file_idx].write(f'{i}\n')
        for file in opened_files:
            file.close()
        return

    def process_location(self) -> None:
        """Iterates through files in temp folder and filters and stores indexes"""
        current_files = ['/'.join([TEMP_FOLDER, f]) for f in listdir(TEMP_FOLDER)]
        with open(f'{SPLIT_DATA_FOLDER}/Station.txt', 'r') as f:
            station_data = f.read().splitlines()
        for file in current_files:
            filename = file.split('/')[-1].split('.txt')[0]
            col, year, month = filename.split('_')
            if col != 'Timestamp':
                continue
            # will read in a list of indexes
            with open(file, 'r') as f:
                timestamp_data = list(map(int, f.read().splitlines()))
            station_ok = [idx for idx in timestamp_data
                          if station_data[idx] == self.location]
            with open(f'{TEMP_FOLDER}/Station_{year}_{month}.txt', 'w') as f:
                for i in station_ok:
                    f.write(f'{i}\n')
            # move timestamp temp file to archive
            new_file_path = f'{ARCHIVE_FOLDER}/{filename}'
            rename(file, new_file_path)
        return


# def get_max_index(file_name: str) -> int:
#     f = open(file_name, 'r')
#     return reduce(lambda a, b: min(a, b), f.read().splitlines())

def binary_search(dt_to_check: datetime, lines: List) -> int:
    left, right = 0, len(lines) - 1
    while left <= right:
        mid = (left + right) // 2
        curr = datetime.strptime(lines[mid], '%Y-%m-%d %H:%M')
        if curr == dt_to_check:
            return mid
        if curr < dt_to_check:
            left = mid + 1
        else:
            right = mid - 1
    return 0
