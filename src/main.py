import os
import shutil
from project_config import (
    SPLIT_DATA_FOLDER,
    DATA_FILE,
    TEMP_FOLDER,
    ARCHIVE_FOLDER,
    RESULTS_FOLDER,
    mapper
)
from typing import List
from Processor import Processor


def get_columns(data_file: str) -> List:
    """Gets header columns in file"""
    return open(data_file, 'r').readline().rstrip().split(',')


def split_columns(data_file: str) -> None:
    """Splits the large csv into individual columns in their own files"""
    columns = get_columns(data_file=data_file)
    if not os.path.exists(SPLIT_DATA_FOLDER):
        os.makedirs(SPLIT_DATA_FOLDER)
    opened_files = [open(f'{SPLIT_DATA_FOLDER}/{col}.txt', 'w') for col in columns]
    with open(data_file, 'r') as f:
        next(f)
        for line in f:
            content = line.rstrip().split(',')
            for file, c, col in zip(opened_files, content, columns):
                if col in mapper:
                    c = mapper[col][c]
                file.write(c + '\n')
    for file in opened_files:
        file.close()
    return


def process_data(required_years: str, location: str) -> None:
    """Uses required years and location to churn out a resulting csv"""
    folders = [TEMP_FOLDER, ARCHIVE_FOLDER, RESULTS_FOLDER]
    for folder in folders:
        if os.path.exists(folder) and os.path.isdir(folder):
            shutil.rmtree(folder)
        os.makedirs(folder)
    processor = Processor(
        required_years=required_years,
        location=location
    )
    processor.process_month_and_year()
    processor.process_location()


def main() -> None:
    """Main interface with user"""
    print(f'Data file used: {DATA_FILE}')
    print(f'File Size is {os.stat(DATA_FILE).st_size / (1024 * 1024)} MB')

    line_count = sum(1 for _ in open(DATA_FILE, 'r'))
    print(f'Number of Lines in the file is {line_count}')

    split_columns(data_file=DATA_FILE)

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
                location='1'
            )
        else:
            process_data(
                required_years=required_years,
                location='0'
            )


if __name__ == '__main__':
    main()
