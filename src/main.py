import os
import shutil
from project_config import (
    SPLIT_DATA_FOLDER,
    DATA_FILE,
    TEMP_FOLDER,
    ARCHIVE_FOLDER,
    RESULTS_FOLDER,
    MAX_FILE_LINE,
    mapper
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
        curr_map_idx = 0
        min_max_dict = initialize_min_max_dict(zone_maps=zone_maps)
        for i, line in enumerate(f):
            if i % MAX_FILE_LINE == 0:
                if opened_files:
                    # store in map and reset min_max_dict
                    zone_maps, min_max_dict, curr_map_idx = store_in_zone_map(
                        curr_map_idx=curr_map_idx,
                        min_max_dict=min_max_dict,
                        zone_maps=zone_maps
                    )
                    close_files(opened_files=opened_files)
                opened_files = [
                    open(f'{SPLIT_DATA_FOLDER}/{col}_{curr_map_idx}.txt', 'w')
                    for col in columns
                ]
            content = line.rstrip().split(',')
            for file, c, col in zip(opened_files, content, columns):
                if col in mapper:
                    c = mapper[col][c]
                if col in zone_maps:
                    min_val, max_val = min_max_dict[col]
                    min_val = min(min_val, c)
                    max_val = max(max_val, c)
                    min_max_dict[col] = [min_val, max_val]
                file.write(f'{c}\n')
    zone_maps, min_max_dict, curr_map_idx = store_in_zone_map(
        curr_map_idx=curr_map_idx,
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
    min_date_str = '9999-01-01 00:00'
    max_date_str = '0001-01-01 00:00'
    min_max_dict = {
        col: [min_date_str, max_date_str]
        for col in zone_maps
    }
    return min_max_dict


def store_in_zone_map(
    curr_map_idx: int,
    min_max_dict: Dict,
    zone_maps: Dict
) -> Tuple[Dict, Dict, int]:
    for col in zone_maps:
        zone_maps[col][curr_map_idx] = min_max_dict[col]
        min_date_str = '9999-01-01 00:00'
        max_date_str = '0001-01-01 00:00'
        min_max_dict[col] = [min_date_str, max_date_str]
    curr_map_idx += 1
    return zone_maps, min_max_dict, curr_map_idx


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
    processor = Processor(
        required_years=required_years,
        location=location,
        zone_maps=zone_maps
    )
    processor.process_month_and_year()
    processor.process_location()


def main() -> None:
    """Main interface with user"""
    print(f'Data file used: {DATA_FILE}')
    print(f'File Size is {os.stat(DATA_FILE).st_size / (1024 * 1024)} MB')

    line_count = sum(1 for _ in open(DATA_FILE, 'r'))
    print(f'Number of Lines in the file is {line_count}')

    zone_maps = {
        'Timestamp': {}
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
            required_years, location = matric_num[-2], int(matric_num[-3])
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
