import pandas as pd
from inflection import underscore, transliterate


FLAT_FILE_2022_URL = 'http://www.ffiec.gov/Census/Census_Flat_Files/CensusFlatFile2022.zip'
DATA_DICT_2022_URL = 'https://www.ffiec.gov/Census/Census_Flat_Files/FFIEC_Census_File_Definitions_26AUG22.xlsx'
STORAGE_OPTIONS = {'User-Agent': 'Mozilla/5.0'}
FIELDS_RENAME = {
    'Key field. MSA/MD Code': 'msa_md_code',
    'Key field. FIPS state code': 'fips_state_code',
    'Key field. FIPS county code': 'fips_county_code',
    'Key field. Census tract. Implied decimal point.': 'census_tract',
    'FFIEC Estimated MSA/MD median family income': 'ffiec_estimated_msa_md_median_family_income',
    'Income indicator, which identifies low, moderate, middle, and upper income areas': 'income_category',
    'Poverty level percent (2 decimal places with decimal point), rounded': 'poverty_rate',
}
FIELDS_STRING = [
    'msa_md_code',
    'fips_state_code',
    'fips_county_code',
    'census_tract',
]
FIELDS_INTEGER = [
    'income_indicator',
]

def set_field_types(data_dict):
    type_dict = {}
    for key, value in data_dict.items():
        for item in FIELDS_STRING:
            if item == value:
                type_dict[key] = 'str'
        for item in FIELDS_INTEGER:
            if item == value:
                type_dict[key] == 'Int64'
    return type_dict


def make_data_dict():
    data_dict = pd.read_excel(
        DATA_DICT_2022_URL,
        sheet_name='Data Dictionary',
        usecols=['Index', 'Description'],
        storage_options=STORAGE_OPTIONS
    ).dropna().set_index('Index')['Description']#.apply(transliterate)
    data_dict.index = data_dict.index.astype(int) - 1
    data_dict = data_dict[data_dict.isin(FIELDS_RENAME.keys())].replace(FIELDS_RENAME)
    return data_dict.to_dict()


def main():

    data_dict = make_data_dict()

    df = pd.read_csv(
        FLAT_FILE_2022_URL,
        compression='zip',
        header=None,
        nrows=100, # REMOVE ME AFTER TESTING!
        storage_options=STORAGE_OPTIONS,
        dtype=set_field_types(data_dict),
    )[data_dict.keys()].rename(columns=make_data_dict())

    print(df)

    print("Saving to ndjson")
    df.to_json(
        "ffiec_income_data.ndjson",
        orient="records",
        lines=True
    )

    # data_dict = pd.read_excel(
    #     DATA_DICT_2022_URL,
    #     sheet_name='Data Dictionary',
    #     storage_options=STORAGE_OPTIONS
    # ).set_index('Index')['Description']#.apply(underscore).to_dict()
    # print(data_dict)



if __name__ == '__main__':
    main()
