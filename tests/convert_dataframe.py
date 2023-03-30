import pandas as pd
from typing import Dict, Tuple

df_res_columns = ['Date', 'Station', 'Category', 'Value']


def read_output(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)

    # filters
    df[['Date', 'Time']] = df['Timestamp'].str.split(' ', expand=True)
    df[['Year', 'Month', 'Day']] = df['Date'].str.split('-', expand=True)
    df = df[df['Station'] == 'Paya Lebar']
    df = df[df['Year'].isin(['2003', '2013'])]
    df[['Temperature', 'Humidity']] = df[['Temperature', 'Humidity']].astype(float)
    df.drop(['Timestamp', 'Time', 'id'], axis=1, inplace=True)

    groupbys = create_groupby(df)

    df_res = pd.DataFrame(columns=df_res_columns)

    df_res = iterate_df(df, df_res, groupbys)

    return df_res.sort_values(['Date', 'Category'], ignore_index=True)


def create_groupby(df: pd.DataFrame) -> Tuple[pd.DataFrame]:
    max_temp_df = df.groupby(['Year', 'Month'])['Temperature'].max()
    min_temp_df = df.groupby(['Year', 'Month'])['Temperature'].min()
    max_humidity_df = df.groupby(['Year', 'Month'])['Humidity'].max()
    min_humidity_df = df.groupby(['Year', 'Month'])['Humidity'].min()
    return (max_temp_df, min_temp_df, max_humidity_df, min_humidity_df)


def store_in_df_res(row: Dict, category: str, df_res: pd.DataFrame) -> None:
    check1 = (df_res['Date'] == row['Date'])
    check2 = (df_res['Category'] == category)
    if len(df_res[check1 & check2]) > 0:
        return df_res
    new_row = {
        col: row[col] for col in df_res_columns
        if col in row
    }
    if 'Temperature' in category:
        new_row['Value'] = row['Temperature']
    else:
        new_row['Value'] = row['Humidity']
    new_row['Category'] = category
    df_res = df_res.append(new_row, ignore_index=True)
    return df_res


def iterate_df(
    df: pd.DataFrame,
    df_res: pd.DataFrame,
    groupbys: Tuple[pd.DataFrame]
) -> pd.DataFrame:
    max_temp_df, min_temp_df, max_humidity_df, min_humidity_df = groupbys
    for _, row in df.iterrows():
        temperature, humidity = row['Temperature'], row['Humidity']
        year, month = row['Year'], row['Month']
        max_temp = max_temp_df[(year, month)]
        min_temp = min_temp_df[(year, month)]
        max_humidity = max_humidity_df[(year, month)]
        min_humidity = min_humidity_df[(year, month)]
        if temperature == max_temp:
            df_res = store_in_df_res(row, 'Max Temperature', df_res)
        if temperature == min_temp:
            df_res = store_in_df_res(row, 'Min Temperature', df_res)
        if humidity == max_humidity:
            df_res = store_in_df_res(row, 'Max Humidity', df_res)
        if humidity == min_humidity:
            df_res = store_in_df_res(row, 'Min Humidity', df_res)
    return df_res
