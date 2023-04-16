from project_config import (
    SPLIT_DATA_FOLDER,
    TEMP_FOLDER,
    ARCHIVE_FOLDER,
    MAX_FILE_LINE,
    RESULTS_FOLDER
)
from typing import List, Dict, Tuple, Set, Union
import os
import csv


class Processor:
    def __init__(
        self,
        required_years: int,
        location: str,
        zone_maps: Dict[str, List[Tuple[int, int]]]
    ) -> None:
        self.required_years = required_years
        self.location = location
        self.zone_maps = zone_maps

    def process_month_and_year(self) -> None:
        """Filters through timestamps and stores the indexes"""
        years = [year for year in range(2002, 2022)
                 if year % 10 == self.required_years]
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
        """After finding the correct zone, get correct timestamps and write to file"""
        if not self.zone_maps:
            return
        if zone >= len(list(self.zone_maps.values())[0]):
            return
        with open(f'{SPLIT_DATA_FOLDER}/Timestamp_{zone}.txt', 'r') as f:
            lines = f.read().splitlines()
        lowest_idx = 0
        if dt_to_check:
            lowest_idx = self.binary_search(
                dt_to_check=dt_to_check,
                lines=lines
            )
        jump = MAX_FILE_LINE * zone
        for idx in range(lowest_idx, len(lines)):
            line = lines[idx]
            if int(line[:4]) != year:
                break
            month = int(line[5:7])
            opened_files[month - 1].write(f'{line.split()[0]} {jump + idx}\n')
        else:
            self.write_to_file_from_zone(
                zone=zone + 1,
                year=year,
                opened_files=opened_files
            )
        for file in opened_files:
            file.close()
        return

    def find_zone(self, dt_to_check: str) -> int:
        """Compares the range of each zone map and finds the zone the date is in"""
        for zone, min_max_dict in enumerate(self.zone_maps['Timestamp']):
            min_date = min_max_dict['min_date']
            max_date = min_max_dict['max_date']
            if min_date <= dt_to_check <= max_date:
                return zone
        return -1

    def process_location(self) -> None:
        """Stores positions with correct location"""
        current_files = ['/'.join([TEMP_FOLDER, f])
                         for f in os.listdir(TEMP_FOLDER)]
        for file in current_files:
            filename = file.split('/')[-1]
            underscore_idx = filename.find('_')
            col = filename[:underscore_idx]
            remainder = filename[underscore_idx + 1:]
            if col != 'Timestamp':
                continue
            # will read in a list of indexes
            station_file = f'{TEMP_FOLDER}/Station_{remainder}'
            with open(file, 'r') as f1, open(station_file, 'a') as f2:
                timestamp_data = f1.read().splitlines()
                timestamp_data = list(map(self.split_timestamp, timestamp_data))
                starting_idx = timestamp_data[0][1]
                ending_idx = timestamp_data[-1][1]
                for zone, min_max_dict in enumerate(self.zone_maps['Station']):
                    min_idx = min_max_dict['min_idx']
                    max_idx = min_max_dict['max_idx']
                    # if no overlap between zone and indexes, skip to next file
                    if starting_idx > max_idx:
                        continue
                    # if exceeded, break
                    if ending_idx < min_idx:
                        break
                    with open(f'{SPLIT_DATA_FOLDER}/Station_{zone}.txt', 'r') as f:
                        station_data = f.read().splitlines()
                    for timestamp_date, timestamp_idx in timestamp_data:
                        if timestamp_idx not in range(min_idx, max_idx + 1):
                            continue
                        if station_data[timestamp_idx - min_idx] == self.location:
                            f2.write(f'{timestamp_date} {timestamp_idx}\n')
        # move timestamp temp file to archive
        folder = f'{ARCHIVE_FOLDER}/Timestamp'
        if not os.path.exists(folder):
            os.makedirs(folder)
        for file in current_files:
            filename = file.split('/')[-1]
            new_file_path = f'{folder}/{filename}'
            os.rename(file, new_file_path)
        return

    def process_temperature_and_humidity(self, matric_num) -> None:
        file_name = f'ScanResult_{matric_num}.csv'
        current_files = ['/'.join([TEMP_FOLDER, f]) for f in os.listdir(TEMP_FOLDER)]
        for file in current_files:
            min_temp, min_temp_dates = float('inf'), set()
            max_temp, max_temp_dates = float('-inf'), set()
            min_humidity, min_humidity_dates = float('inf'), set()
            max_humidity, max_humidity_dates = float('-inf'), set()
            with open(file, 'r') as f1:
                station_data = f1.read().splitlines()
                # Skip to next intermediate file if current file is empty
                if not station_data:
                    continue
                station_data = list(map(self.split_timestamp, station_data))
                starting_idx = station_data[0][1]
                ending_idx = station_data[-1][1]
                for zone, min_max_dict in enumerate(self.zone_maps['Temperature']):
                    min_idx = min_max_dict['min_idx']
                    max_idx = min_max_dict['max_idx']
                    if starting_idx > max_idx:
                        continue
                    if ending_idx < min_idx:
                        break
                    with open(f'{SPLIT_DATA_FOLDER}/Temperature_{zone}.txt', 'r') as f:
                        temperature_data = list(
                            map(self.convert_to_float, f.read().splitlines())
                        )
                    with open(f'{SPLIT_DATA_FOLDER}/Humidity_{zone}.txt', 'r') as f:
                        humidity_data = list(
                            map(self.convert_to_float, f.read().splitlines())
                        )

                    for station_date, station_idx in station_data:
                        if station_idx not in range(min_idx, max_idx + 1):
                            continue
                        current_temp = temperature_data[station_idx - min_idx]
                        current_humidity = humidity_data[station_idx - min_idx]
                        min_temp, min_temp_dates = self.compare_stats(
                            current_date=station_date,
                            current_stat=current_temp,
                            stat_to_change=min_temp,
                            date_set=min_temp_dates,
                            is_min=True
                        )
                        max_temp, max_temp_dates = self.compare_stats(
                            current_date=station_date,
                            current_stat=current_temp,
                            stat_to_change=max_temp,
                            date_set=max_temp_dates,
                            is_min=False
                        )
                        min_humidity, min_humidity_dates = self.compare_stats(
                            current_date=station_date,
                            current_stat=current_humidity,
                            stat_to_change=min_humidity,
                            date_set=min_humidity_dates,
                            is_min=True
                        )
                        max_humidity, max_humidity_dates = self.compare_stats(
                            current_date=station_date,
                            current_stat=current_humidity,
                            stat_to_change=max_humidity,
                            date_set=max_humidity_dates,
                            is_min=False
                        )
            self.write_results(
                file_name=file_name,
                min_temp_stats=[min_temp, min_temp_dates],
                max_temp_stats=[max_temp, max_temp_dates],
                min_humidity_stats=[min_humidity, min_humidity_dates],
                max_humidity_stats=[max_humidity, max_humidity_dates]
            )
        return

    def write_results(
        self,
        file_name: str,
        min_temp_stats: List[Union[float, Set]],
        max_temp_stats: List[Union[float, Set]],
        min_humidity_stats: List[Union[float, Set]],
        max_humidity_stats: List[Union[float, Set]],
    ) -> None:
        if not os.path.exists(RESULTS_FOLDER):
            os.makedirs(RESULTS_FOLDER)
        station = 'Paya Lebar' if self.location == '1' else 'Changi'
        stats_and_categories = [
            (min_temp_stats, 'Min Temperature'),
            (max_temp_stats, 'Max Temperature'),
            (min_humidity_stats, 'Min Humidity'),
            (max_humidity_stats, 'Max Humidity')
        ]
        file_name = f'{RESULTS_FOLDER}/{file_name}'
        file_exists = os.path.isfile(file_name)
        with open(file_name, 'a', newline='') as csv_file:
            csv_writer = csv.writer(csv_file, delimiter=',')
            if not file_exists:
                col_name = ['Date', 'Station', 'Category', 'Value']
                csv_writer.writerow(col_name)  # write col name
            for (stat, dates), category in stats_and_categories:
                for date in dates:
                    line = [date, station, category, str(stat)]
                    csv_writer.writerow(line)
        return

    def compare_stats(
        self,
        current_date: str,
        current_stat: float,
        stat_to_change: float,
        date_set: Set,
        is_min: bool
    ) -> Tuple[float, Set]:
        if is_min:
            if current_stat != 'M':
                if current_stat < stat_to_change:
                    stat_to_change = current_stat
                    date_set = set([current_date])
                elif current_stat == stat_to_change:
                    date_set.add(current_date)
        else:
            if current_stat != 'M':
                if current_stat > stat_to_change:
                    stat_to_change = current_stat
                    date_set = set([current_date])
                elif current_stat == stat_to_change:
                    date_set.add(current_date)
        return stat_to_change, date_set

    def binary_search(self, dt_to_check: str, lines: List[str]) -> int:
        """Binary search to find smallest position of record in current month"""
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
        return left

    def convert_to_float(self, s: str) -> Union[str, float]:
        try:
            return float(s)
        except ValueError:
            return s

    def split_timestamp(self, s: str) -> List[Union[str, int]]:
        date_and_idx = s.rstrip().split()
        date_and_idx[1] = int(date_and_idx[1])
        return date_and_idx
