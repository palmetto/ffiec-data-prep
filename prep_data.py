from typing import Dict
import argparse
import pandas as pd


FLAT_FILE_2022_URL = 'http://www.ffiec.gov/Census/Census_Flat_Files/CensusFlatFile2022.zip'
DATA_DICT_2022_URL = 'https://www.ffiec.gov/Census/Census_Flat_Files/FFIEC_Census_File_Definitions_26AUG22.xlsx'
STORAGE_OPTIONS = {'User-Agent': 'Mozilla/5.0'}
FIELDS_RENAME = {
    'Key field. MSA/MD Code': 'msa_geoid',
    'Key field. FIPS state code': 'fips_state_code',
    'Key field. FIPS county code': 'fips_county_code',
    'Key field. Census tract. Implied decimal point.': 'census_tract_code',
    'FFIEC Estimated MSA/MD median family income': 'ffiec_msamd_mfi',
    'Tract median family income as a percentage of the MSA/MD median family income. 2 decimal places, truncated.': 'mfi_as_percent_of_msamd_mfi',
    'Income indicator, which identifies low, moderate, middle, and upper income areas': 'income_indicator',
    'Poverty level percent (2 decimal places with decimal point), rounded': 'poverty_level_percent',
}


def get_ffiec_data_dict():
    """ Get the data dictionary from the FFIEC website and return
    a dictionary of column index (in the flat file) to column name,
    just for the subset of columns we want.
    """
    data_dict = pd.read_excel(
        DATA_DICT_2022_URL,
        sheet_name='Data Dictionary',
        usecols=['Index', 'Description'],
        storage_options=STORAGE_OPTIONS
    ).dropna().set_index('Index')['Description']
    data_dict.index = data_dict.index.astype(int) - 1
    data_dict = data_dict[data_dict.isin(FIELDS_RENAME.keys())].replace(FIELDS_RENAME)
    return data_dict.to_dict()


def get_ffiec_income_data(data_dict: Dict[str, str]):
    """ Get the FFIEC income data from the FFIEC website
    using the data dictionary to choose a subset of columns,
    and set their names and types.
    """
    return pd.read_csv(
        FLAT_FILE_2022_URL,
        compression='zip',
        header=None,
        storage_options=STORAGE_OPTIONS,
        usecols=data_dict.keys(),
        dtype={
            k: 'str' for k, v in data_dict.items()
            if v.endswith(('_code', '_geoid'))
        },
    ).rename(columns=data_dict)


def cast_income_indicator(df):
    """ cast income indicator to int
    """
    df.income_indicator = df.income_indicator.astype(int)
    return df


def make_tract_geoid(df):
    """ contruct unique tract geoid
    NOTE: assumes column order in the flat file is:
    state, county, tract
    """
    fields_list = [c for c in df.columns if c.endswith('_code')]
    df['census_tract_geoid'] = df[fields_list].apply(lambda x: ''.join(x), axis=1)
    df.drop(columns=fields_list, inplace=True)
    return df


def drop_nulls(df):
    """ drop rows with any NaN or '999999' tract codes
    """
    df = df.dropna(subset=['income_indicator', 'poverty_level_percent'])
    df = df[df['census_tract_code'] != '999999']
    return df


def make_low_income_community(df):
    """ create low income community column,
    based on income indicator and proverty level columns
    """
    df['low_income_community'] = (
        (df['income_indicator'].isin([1, 2])) |
        (df['poverty_level_percent'] >= 20)
    )
    return df


def main():

    ap = argparse.ArgumentParser()
    ap.add_argument(
        'mode',
        choices=['dev', 'prod'],
        default='prod',
        help='Run in dev mode to use small test subset',
    )
    is_dev = ap.parse_args().mode == 'dev'

    # get data dictionary
    data_dict = get_ffiec_data_dict()

    # get ffiec data
    df_ffiec = get_ffiec_income_data(data_dict)

    # process ffiec data
    df_ffiec = drop_nulls(df_ffiec)
    df_ffiec = cast_income_indicator(df_ffiec)
    df_ffiec = make_tract_geoid(df_ffiec)
    df_ffiec = make_low_income_community(df_ffiec)

    # subset if dev mode
    if is_dev:
        df_ffiec = df_ffiec[df_ffiec.census_tract_geoid.str.startswith('25')]

    # save to ndjson
    (
        df_ffiec
        .apply(dict, axis=1)
        .to_frame(name='properties')
        .to_json(
            f"{'ma_' if is_dev else ''}ffiec_income_data.ndjson.gz",
            compression='gzip',
            orient='records',
            lines=True,
        )
    )


if __name__ == '__main__':
    main()
