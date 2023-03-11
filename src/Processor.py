from project_config import (
    SPLIT_DATA_FOLDER,
    TEMP_FOLDER,
    ARCHIVE_FOLDER,
    MAX_FILE_LINE
)
# from functools import reduce
from datetime import datetime
from typing import List, Dict
import os


class Processor:
    def __init__(
        self,
        required_years: str,
        location: str,
        zone_maps: Dict[str, Dict[int, Dict]]
    ) -> None:
        self.required_years = required_years
        self.location = location
        self.zone_maps = zone_maps

    def process_month_and_year(self) -> None:
        """Filters through timestamps and stores the indexes"""
        years = [year for year in range(2002, 2022)
                 if year % 10 == int(self.required_years)]
        for year in years:
            dt_to_check = f'{year}-01-01 00:00'
            zone = self.find_zone(dt_to_check=dt_to_check)
            if zone == -1:
                print('Error, could not find date')
                return
            opened_files = [open(f'{TEMP_FOLDER}/Timestamp_{year}_{i}.txt', 'w')
                            for i in range(1, 13)]
            self.write_to_file_from_zone(
                zone=zone,
                year=year,
                opened_files=opened_files,
                dt_to_check=dt_to_check
            )
        return

    def write_to_file_from_zone(
        self,
        zone: int,
        year: int,
        opened_files: List,
        dt_to_check: str = None,
    ) -> None:
        with open(f'{SPLIT_DATA_FOLDER}/Timestamp_{zone}.txt', 'r') as f:
            lines = f.read().splitlines()
        lowest_idx = 0
        if dt_to_check:
            lowest_idx = binary_search(
                dt_to_check=dt_to_check,
                lines=lines
            )
        jump = MAX_FILE_LINE * zone
        for idx in range(lowest_idx, lowest_idx + len(lines)):
            line = lines[idx]
            if int(line[:4]) != year:
                break
            month = int(line[5:7])
            opened_files[month - 1].write(f'{jump + idx}\n')
        else:
            self.write_to_file_from_zone(
                zone=zone + 1,
                year=year,
                opened_files=opened_files
            )
        for file in opened_files:
            file.close()
        return

    def find_zone(self, dt_to_check: datetime) -> int:
        for zone, min_and_max in self.zone_maps['Timestamp'].items():
            min_value, max_value = min_and_max
            if min_value <= dt_to_check <= max_value:
                return zone
        return -1

    def process_location(self) -> None:
        """Iterates through files in temp folder and filters and stores indexes"""
        current_files = ['/'.join([TEMP_FOLDER, f])
                         for f in os.listdir(TEMP_FOLDER)]
        for zone in self.zone_maps['Timestamp'].keys():
            with open(f'{SPLIT_DATA_FOLDER}/Station_{zone}.txt', 'r') as f:
                station_data = f.read().splitlines()
                jump = MAX_FILE_LINE * zone
            for file in current_files:
                filename = file.split('/')[-1]
                underscore_idx = filename.find('_')
                col = filename[:underscore_idx]
                remainder = filename[underscore_idx + 1:]
                if col != 'Timestamp':
                    continue
                # will read in a list of indexes
                with open(file, 'r') as f:
                    timestamp_data = list(map(int, f.read().splitlines()))
                station_ok = [idx for idx in timestamp_data
                              if idx in range(jump, jump + len(station_data))
                              if station_data[idx - jump] == self.location]
                with open(f'{TEMP_FOLDER}/Station_{remainder}', 'a') as f:
                    for idx in station_ok:
                        f.write(f'{idx}\n')
        # move timestamp temp file to archive
        folder = f'{ARCHIVE_FOLDER}/Timestamp'
        if not os.path.exists(folder):
            os.makedirs(folder)
        for file in current_files:
            filename = file.split('/')[-1]
            new_file_path = f'{folder}/{filename}'
            os.rename(file, new_file_path)
        return


# def get_max_index(file_name: str) -> int:
#     f = open(file_name, 'r')
#     return reduce(lambda a, b: min(a, b), f.read().splitlines())

def binary_search(dt_to_check: str, lines: List) -> int:
    left, right = 0, len(lines) - 1
    while left <= right:
        mid = (left + right) // 2
        mid_value = lines[mid]
        if mid_value == dt_to_check:
            return mid
        if mid_value < dt_to_check:
            left = mid + 1
        else:
            right = mid - 1
    return 0
