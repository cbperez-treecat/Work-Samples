import pandas as pd
import numpy as np

from pyep import qualify_file_name
from pyep.transform import derive_from_date_exited_4y
from pyep import normalize_file_extension
from pycron import fetch_from_sftp, copy_to_processor_directory


def main():
    # DISTRICT VARIABLES
    district = 'ca-schooldistrict'
    file_name = 'SKR*.csv'
    remote_directory = '/data/'
    data_type = 'student'
    file_watcher_type = 'SIS'
    # COPY AND READ FILE(S)
    file_path = fetch_from_sftp(district_name=district,
                                file_name=file_name,
                                match='latest',
                                remote_directory=remote_directory,
                                data_type=data_type,
                                add_datestamp=True)

    df = pd.read_csv(file_path, sep=',', dtype=str, na_values=['NA', '', ' ', 'nan'], keep_default_na=False)

    #fill row with previous file value to avoid nulls
    df['Race'] = df['Race'].fillna(method="ffill")

    #new df for non-ebs to combine later
    nonebs_df = df[df.loc[:, 'Start Date'].isnull()]
    nonebs_df.drop_duplicates(subset=['Local Student ID'], inplace=True)

    #remove blank start dates
    df = df[~df.loc[:, 'Start Date'].isnull()]

    #   ~~ PRE-PROCESSING ~~
    #sort for earliest start date
    entry_date = df[['Local Student ID', 'Start Date']]
    entry_date = entry_date.sort_values(by='Start Date', ascending=False)
    entry_date.drop_duplicates(subset=['Local Student ID'], keep='first')
    entry_date = entry_date.rename(columns={'Start Date': 'ELV_LEP_Start_Date'})
    entry_date_df = pd.merge(df, entry_date, how='left', on='Local Student ID')
    entry_date_df.drop_duplicates(subset=['Local Student ID'], keep='first', inplace=True)

    # sort for latest end date
    exit_date = df[['Local Student ID', 'End Date']]
    exit_date = exit_date.sort_values(by=['End Date'])
    exit_date = exit_date.drop_duplicates(subset=['Local Student ID'], keep='last')
    exit_date = exit_date.rename(columns={'End Date': 'ELV_END_DATE'})
    temp_df = pd.merge(entry_date_df, exit_date, how='left', on='Local Student ID')
    temp_df.drop_duplicates(subset=['Local Student ID'], inplace=True)

    # extract LEP data
    lep_data = df[['Local Student ID', 'End Date', 'Years in U.S. Schools', 'ELP Designation', 'Parent Permission Code']]
    lep_data = lep_data.sort_values(by=['End Date'])
    lep_data = lep_data.drop_duplicates(subset=['Local Student ID'], keep='last')
    lep_data = lep_data.drop(columns=['End Date'])
    lep_data = lep_data.rename(columns={'Years in U.S. Schools': 'ELV_YUSS'})
    lep_data = lep_data.rename(columns={'ELP Designation': 'ELV_Designation'})
    lep_data = lep_data.rename(columns={'Parent Permission Code': 'ELV_PPC'})
    eb_df = pd.merge(temp_df, lep_data, how='left', on='Local Student ID')
    eb_df.drop_duplicates(subset=['Local Student ID'], inplace=True)

    #concatenate two df together
    final_df = pd.concat([eb_df, nonebs_df], axis=0, ignore_index=True, sort=False)

    final_df.drop_duplicates(subset=['Local Student ID'], inplace=True)

    final_df['Elv_LEPStatus'] = final_df.apply(derive_lep_status, axis=1)

    final_df['ELV_Parent_permission_code_final'] = final_df.apply(derive_ppc, axis=1)
    # ~~ END PRE-PROCESSING ~~

    # WRITE AND COPY FILE(S)
    output_path = normalize_file_extension(qualify_file_name(file_path, 'processed'), ',')

    final_df.to_csv(output_path, index=False)
    copy_to_processor_directory(district_name=district,
                                file_name=output_path,
                                file_watcher_type=file_watcher_type)

def derive_lep_status(row):
    if row['ELV_Designation'] == 'Y' and pd.isnull(row['ELV_END_DATE']) and row['ELV_PPC'] not in 'C':
        return 'EB'
    elif row['ELV_PPC'] == 'C':
        return 'Parent Denial'
    elif row['ELV_Designation'] == 'Y' and pd.notnull(row['ELV_END_DATE']):
        return derive_from_date_exited_4y(row['ELV_END_DATE'])
    else:
        return 'Non-EB'

def derive_ppc(row):
    monitoring = ['Monitored_Year_1', 'Monitored_Year_2', 'Monitored_Year_3', 'Monitored_Year_4', 'Fully_Exited']
    if row['Elv_LEPStatus'] in monitoring:
        return ' '
    else:
        return row['ELV_PPC']

if __name__ == '__main__':
    main()