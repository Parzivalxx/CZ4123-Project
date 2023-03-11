from project_config import (
    SPLIT_DATA_FOLDER,
    TEMP_RESULTS_FOLDER,
    FINAL_RESULTS_FOLDER
)
# from functools import reduce
from datetime import datetime
from typing import List

class Processor:
    def __init__(self,
                 required_years: str,
                 location: str):
        self.required_years = required_years
        self.location = location
    
    def process_month_and_year(self) -> None:
        """Filters through timestamps and stores the indexes with their respective months and years"""
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
        years = [year for year in range(2002, 2022) if year % 10 == int(self.required_years)]
        opened_files = [open(f'{TEMP_RESULTS_FOLDER}/Timestamp_{year}_{i}.txt', 'w')
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


# def get_max_index(file_name: str) -> int:
#     f = open(file_name, 'r')
#     return reduce(lambda a, b: min(a, b), f.read().splitlines())

def binary_search(dt_to_check: datetime, lines: List) -> int:
    l, r = 0, len(lines) - 1
    while l <= r:
        m = (l + r) // 2
        curr = datetime.strptime(lines[m], '%Y-%m-%d %H:%M')
        if curr == dt_to_check:
            return m
        if curr < dt_to_check:
            l = m + 1
        else:
            r = m - 1
    return 0