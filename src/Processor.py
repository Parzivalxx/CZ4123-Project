from project_config import (
    SPLIT_DATA_FOLDER,
    TEMP_FOLDER,
    ARCHIVE_FOLDER,
    MAX_FILE_LINE,
    RESULTS_FOLDER
)
from datetime import datetime
from typing import List, Dict, Tuple
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
            lowest_idx = binary_search(
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

    def find_zone(self, dt_to_check: datetime) -> int:
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
                timestamp_data = list(map(split_timestamp, timestamp_data))
                for zone, min_max_dict in enumerate(self.zone_maps['Station']):
                    min_idx = min_max_dict['min_idx']
                    max_idx = min_max_dict['max_idx']
                    starting_idx = timestamp_data[0][1]
                    ending_idx = timestamp_data[-1][1]
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
        file_name = "ScanResult_" + matric_num
        current_files = ['/'.join([TEMP_FOLDER, f]) for f in os.listdir(TEMP_FOLDER)]
        station = "Paya Lebar" if self.location == "1" else "Changi"
        maxTempRes, minTempRes, maxHumidRes, minHumidRes = [], [], [], []
        for file in current_files:
            minTemp, minTempList, minTempDate = float("inf"), [], set()
            maxTemp, maxTempList, maxTempDate = float("-inf"), [], set()
            minHumid, minHumidList, minHumidDate = float("inf"), [], set()
            maxHumid, maxHumidList, maxHumidDate = float("-inf"), [], set()
            with open(file, 'r') as f1:
                station_data = f1.read().splitlines()
                # Skip to next intermediate file if current file is empty
                if not station_data:
                    continue
                station_data = list(map(split_timestamp, station_data))
                for zone, min_max_dict in enumerate(self.zone_maps['Temperature']):
                    min_idx = min_max_dict['min_idx']
                    max_idx = min_max_dict['max_idx']
                    starting_idx = station_data[0][1]
                    ending_idx = station_data[-1][1]

                    if starting_idx > max_idx:
                        continue
                    if ending_idx < min_idx:
                        break

                    with open(f'{SPLIT_DATA_FOLDER}/Temperature_{zone}.txt', 'r') as f:
                        temperature_data = f.read().splitlines()
                    with open(f'{SPLIT_DATA_FOLDER}/Humidity_{zone}.txt', 'r') as f:
                        humidity_data = f.read().splitlines()

                    for station_date, station_idx in station_data:
                        if station_idx not in range(min_idx, max_idx + 1):
                            continue
                        currentTemp = temperature_data[station_idx - min_idx]
                        currentHumidity = humidity_data[station_idx - min_idx]

                        if (currentTemp != 'M' and float(currentTemp) < minTemp):
                            minTemp = float(currentTemp)
                            minTempList = [[station_date, station,
                                            "Min Temperature", minTemp]]
                            minTempDate = set()
                            minTempDate.add(station_date)
                        elif (currentTemp != 'M' and float(currentTemp) == minTemp and
                                station_date not in minTempDate):
                            minTempList.append([station_date, station,
                                                "Min Temperature", minTemp])
                            minTempDate.add(station_date)
                        if (currentTemp != 'M' and float(currentTemp) > maxTemp):
                            maxTemp = float(currentTemp)
                            maxTempList = [[station_date, station,
                                            "Max Temperature", maxTemp]]
                            maxTempDate = set()
                            maxTempDate.add(station_date)
                        elif (currentTemp != 'M' and float(currentTemp) == maxTemp and
                                station_date not in maxTempDate):
                            maxTempList.append([station_date, station,
                                                "Max Temperature", maxTemp])
                            maxTempDate.add(station_date)
                        if (currentHumidity != 'M' and
                                float(currentHumidity) < minHumid):
                            minHumid = float(currentHumidity)
                            minHumidList = [[station_date, station,
                                             "Min Humidity", minHumid]]
                            minHumidDate = set()
                            minHumidDate.add(station_date)
                        elif (currentHumidity != 'M' and
                              float(currentHumidity) == minHumid and
                              station_date not in minHumidDate):
                            minHumidList.append([station_date, station,
                                                 "Min Humidity", minHumid])
                            minHumidDate.add(station_date)
                        if (currentHumidity != 'M' and
                                float(currentHumidity) > maxHumid):
                            maxHumid = float(currentHumidity)
                            maxHumidList = [[station_date, station,
                                             "Max Humidity", maxHumid]]
                            maxHumidDate = set()
                            maxHumidDate.add(station_date)
                        elif (currentHumidity != 'M' and
                              float(currentHumidity) == maxHumid and
                              station_date not in maxHumidDate):
                            maxHumidList.append([station_date, station,
                                                 "Max Humidity", maxHumid])
                            maxHumidDate.add(station_date)
            minTempRes.append(minTempList)
            maxTempRes.append(maxTempList)
            minHumidRes.append(minHumidList)
            maxHumidRes.append(maxHumidList)

        write_results(
                fileName=file_name,
                minTempRes=minTempRes,
                maxTempRes=maxTempRes,
                minHumidRes=minHumidRes,
                maxHumidRes=maxHumidRes
        )
        return


def write_results(
    fileName: str,
    minTempRes: List[list],
    maxTempRes: List[list],
    minHumidRes: List[list],
    maxHumidRes: List[list]
) -> None:
    with open(f'{RESULTS_FOLDER}/{fileName}.csv', 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        col_name = [['Date', 'Station', 'Category', 'Value']]
        csvwriter.writerows(col_name)  # write col name
        for i in minTempRes:
            csvwriter.writerows(i)
        for i in maxTempRes:
            csvwriter.writerows(i)
        for i in minHumidRes:
            csvwriter.writerows(i)
        for i in maxHumidRes:
            csvwriter.writerows(i)
    return


def binary_search(dt_to_check: str, lines: List[str]) -> int:
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


def split_timestamp(s: str) -> Tuple[str, int]:
    date_and_idx = s.rstrip().split()
    date_and_idx[1] = int(date_and_idx[1])
    return date_and_idx
