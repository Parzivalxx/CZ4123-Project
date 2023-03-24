import os
import shutil
from project_config import (
    SPLIT_DATA_FOLDER,
    DATA_FILE,
    TEMP_FOLDER,
    ARCHIVE_FOLDER,
    RESULTS_FOLDER,
    MAX_FILE_LINE,
    MAPPER,
    ZONE_MAP_COLS
)
from typing import List, Dict, Tuple
from Processor import Processor


def get_columns(data_file: str) -> List:
    """Gets header columns in file"""
    return open(data_file, 'r').readline().rstrip().split(',')


def split_columns(data_file: str, zone_maps: Dict) -> None:
    """Splits the large csv into individual columns in their own files"""
    columns = get_columns(data_file=data_file)
    recreate_folders(folders=[SPLIT_DATA_FOLDER])
    opened_files = []
    with open(data_file, 'r') as f:
        next(f)
        min_max_dict = initialize_min_max_dict(zone_maps=zone_maps)
        curr_zone = 0
        for i, line in enumerate(f):
            if i % MAX_FILE_LINE == 0:
                if opened_files:
                    # store in map and reset min_max_dict
                    zone_maps, min_max_dict = store_in_zone_map(
                        min_max_dict=min_max_dict,
                        zone_maps=zone_maps
                    )
                    close_files(opened_files=opened_files)
                opened_files = [
                    open(f'{SPLIT_DATA_FOLDER}/{col}_{curr_zone}.txt', 'w')
                    for col in columns
                ]
                curr_zone += 1
            content = line.rstrip().split(',')
            for file, c, col in zip(opened_files, content, columns):
                if col in MAPPER:
                    c = MAPPER[col][c]
                if col in zone_maps:
                    min_val, max_val = min_max_dict[col]
                    min_val = min(min_val, c)
                    max_val = max(max_val, c)
                    min_max_dict[col] = [min_val, max_val]
                file.write(f'{c}\n')
    zone_maps, min_max_dict = store_in_zone_map(
        min_max_dict=min_max_dict,
        zone_maps=zone_maps
    )
    close_files(opened_files=opened_files)
    return zone_maps


def recreate_folders(folders: List[str]) -> None:
    for folder in folders:
        if os.path.exists(folder) and os.path.isdir(folder):
            shutil.rmtree(folder)
        os.makedirs(folder)
    return


def initialize_min_max_dict(zone_maps: Dict) -> Dict:
    """Initialize the dictionary that stores the min and max for each col zone"""
    min_date_str = '9999-01-01 00:00'
    max_date_str = '0001-01-01 00:00'
    min_max_dict = {
        col: [min_date_str, max_date_str]
        for col in zone_maps
    }
    return min_max_dict


def store_in_zone_map(
    min_max_dict: Dict,
    zone_maps: Dict
) -> Tuple[Dict, Dict]:
    """Store the current min and max in the zone map and reinitialize the min and max"""
    for col in zone_maps:
        zone_maps[col].append(tuple(min_max_dict[col]))
        min_date_str = '9999-01-01 00:00'
        max_date_str = '0001-01-01 00:00'
        min_max_dict[col] = [min_date_str, max_date_str]
    return zone_maps, min_max_dict


def close_files(opened_files: List) -> None:
    for file in opened_files:
        file.close()
    return


def process_data(
    required_years: str,
    location: str,
    zone_maps: Dict
) -> None:
    """Uses required years and location to churn out a resulting csv"""
    folders = [TEMP_FOLDER, ARCHIVE_FOLDER, RESULTS_FOLDER]
    recreate_folders(folders=folders)
    print('Processing data...')
    processor = Processor(
        required_years=required_years,
        location=location,
        zone_maps=zone_maps
    )
    processor.process_month_and_year()
    processor.process_location()
    processor.process_temperature_and_humidity() # getting results here


def main() -> None:
    """Main interface with user"""
    print(f'Data file used: {DATA_FILE}')
    print(f'File Size is {os.stat(DATA_FILE).st_size / (1024 * 1024)} MB')

    line_count = sum(1 for _ in open(DATA_FILE, 'r'))
    print(f'Number of Lines in the file is {line_count}')

    zone_maps = {
        col: []
        for col in ZONE_MAP_COLS
    }

    zone_maps = split_columns(data_file=DATA_FILE, zone_maps=zone_maps)

    while True:
        print()
        text = 'Enter your matriculation number for processing, c to cancel: '
        matric_num = input(text).strip()
        if matric_num == 'c':
            print('Have a good day, bye bye...')
            break
        try:
            if len(matric_num) != 9:
                print('Invalid input, matriculation number is of length 9...')
                continue
            required_years, location = int(matric_num[-2]), int(matric_num[-3])
        except ValueError:
            print('Invalid input, please try again...')
            continue
        if location % 2:
            process_data(
                required_years=required_years,
                location='1',
                zone_maps=zone_maps
            )
        else:
            process_data(
                required_years=required_years,
                location='0',
                zone_maps=zone_maps
            )


if __name__ == '__main__':
    main()
