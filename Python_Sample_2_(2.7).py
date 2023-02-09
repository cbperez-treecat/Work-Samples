from pyep import qualify_file_name, normalize_file_extension
from pycron import fetch_from_sftp, copy_to_processor_directory
from pyep.transform import derive_from_date_exited_4y
import pandas as pd
import numpy as np
from datetime import timedelta


def main():
    # Edit these variables
    district = 'tx-district'
    demo_filename = 'district-ellevation.student.csv'
    ell_filename = 'district-ellevation.studentLEPData.csv'
    remote_directory = '/data/'
    data_type = 'student'
    file_watcher_type = 'SIS'

    demo_file_path = fetch_from_sftp(district_name=district,
                                     file_name=demo_filename,
                                     remote_directory=remote_directory,
                                     data_type=data_type,
                                     add_datestamp=True)
    ell_file_path = fetch_from_sftp(district_name=district,
                                    file_name=ell_filename,
                                    remote_directory=remote_directory,
                                    data_type=data_type,
                                    add_datestamp=True)

    demo_df = pd.read_csv(demo_file_path, dtype=str)
    ell_df = pd.read_csv(ell_file_path, dtype=str, parse_dates=['LEPStartDate', 'LEPEndDate', 'First Entry Dt'])

    # Keep BIL Credit Date if students have one
    bil_df = ell_df[['LocalStudentID', 'IsBilLoteCredit Assessment Met Date']]
    bil_df = bil_df.drop_duplicates(subset='LocalStudentID')
    bil_df['ELV_BILOTECredit'] = bil_df.apply(
        lambda x: 'Y' if pd.notnull(x['IsBilLoteCredit Assessment Met Date']) else 'N', axis=1)

    # Keep the yuss date for students before dropping dupes
    yuss_df = ell_df[['LocalStudentID', 'YearsInUSSchools']]
    yuss_df = yuss_df.sort_values('YearsInUSSchools', ascending=False)
    yuss_df = yuss_df[yuss_df['YearsInUSSchools'].notna()]
    values = ['Blank - Not Applicable']
    yuss_df = yuss_df[yuss_df['YearsInUSSchools'].isin(values) == False]
    yuss_df = yuss_df.drop_duplicates(subset='LocalStudentID')
    yuss_df['ELV_YearsInUSSchools'] = yuss_df['YearsInUSSchools']
    yuss_df = yuss_df.drop(columns=['YearsInUSSchools'])
    ell_df = pd.merge(ell_df, yuss_df, how='left', on='LocalStudentID')

    # keep the oldest entry date
    entry_date = ell_df[['LocalStudentID', 'First Entry Dt']]
    entry_date['First Entry Dt'] = pd.to_datetime(entry_date['First Entry Dt'])
    entry_date = entry_date.dropna(subset=['First Entry Dt'])
    entry_date = entry_date.sort_values('First Entry Dt', ascending=False)
    entry_date.drop_duplicates(subset=['LocalStudentID'], keep='first')
    entry_date['ELV_Entry_Date'] = entry_date['First Entry Dt']
    entry_date = entry_date.drop(columns=['First Entry Dt'])
    ell_df = pd.merge(ell_df, entry_date, how='left', on='LocalStudentID')

    #group dates by ID; keep first date
    ell_df['ELV_LEPStartDate_min'] = ell_df.groupby(['LocalStudentID'])['LEPStartDate'].transform(min)

    # Keep the exit date for a student if there is no start date for that student after the exit date
    ell_df['ELV_LEPStartDate_max'] = ell_df.groupby(['LocalStudentID'])['LEPStartDate'].transform(max)
    ell_df['ELV_LEPEndDate_max'] = ell_df.groupby(['LocalStudentID'])['LEPEndDate'].transform(max)
    ell_df['ELV_EndDate_after_StartDate'] = ell_df['ELV_LEPEndDate_max'] > ell_df['ELV_LEPStartDate_max']

    # Keep most recent record (last record sent in the file)
    ell_df = ell_df.drop_duplicates(subset='LocalStudentID', keep='last')

    # Use the end date for the student if they do not have a start date after the end date
    ell_df['ELV_LEPEndDate'] = ell_df['ELV_LEPEndDate_max'].where(ell_df['ELV_EndDate_after_StartDate'] == True)
    ell_df['ELV_LEPEndDate'] = ell_df['ELV_LEPEndDate'].astype(str).replace('NaT', np.nan)

    # Drop columns that exist in both files (use the columns from the ell file)
    demo_df = demo_df.drop(
        columns=['HomeLang', 'YearsInUSSchools', 'DateEnteredInUS', 'EnrolledDate', 'GraduationDate', 'LEPStartDate',
                 'LEPEndDate', 'LEPMonitoringStartDate', 'IsBilLoteCredit', 'First Entry Dt'])

    # merge the files together
    df = pd.merge(demo_df, ell_df, how='left', on='LocalStudentID')

    # Keep only Active students; keep for future requests
    # df = df[df.loc[:, 'StudentStatus'].isin(['A'])]

    df['ELV_LEPStatus'] = df.apply(derive_lep_status, axis=1)

    df['ELV_Dyslexic_Status'] = df.apply(derive_dyslexic_status, axis=1)

    # Only import years in US schools for non exited/monitored students; keep for future requests
    # exited = df['ELV_LEPStatus'].str.contains('Monitored|Exited').fillna(False)
    # df['ELV_YearsInUSSchools'] = df['YearsInUSSchools']

    df['ELV_YearsInUSSchools'] = df.apply(derive_yuss, axis=1)

    df['ELV_Receiving_services'] = df.apply(derive_receiving_services, axis=1)

    # merge the last files together
    final_df = pd.merge(df, bil_df, how='left', on='LocalStudentID')

    #Keep students with certain statuses
    statuses = ['ELL', 'Exited-Monitored', 'Fully_Exited', 'Lang Other than Eng or Blank', 'Monitored_Year_1',
                'Monitored_Year_2', 'Monitored_Year_3', 'Monitored_Year_4', 'TDNQ', 'Parent Denial', 'Action Required']
    final_df = final_df[final_df['ELV_LEPStatus'].isin(statuses)]

    final_df = final_df.drop_duplicates()

    processed_file_name = normalize_file_extension(qualify_file_name(demo_file_path, 'processed'), ',')

    final_df.to_csv(processed_file_name, index=False)

    copy_to_processor_directory(district, processed_file_name, file_watcher_type)


def derive_lep_status(row):
    today = pd.to_datetime('today')
    cutoff_date = today - timedelta(days=60)
    nat_lang_eng = row['NativeLanguage'] == '98'
    nat_lang_blank = pd.isnull(row['NativeLanguage'])
    home_lang_eng = row['HomeLang'] == '98'
    home_lang_blank = pd.isnull(row['HomeLang'])

    if row['ELV_LEPEndDate'] != '' and pd.notnull(row['ELV_LEPEndDate']):
        return derive_from_date_exited_4y(row['ELV_LEPEndDate'])
    elif row['RefusedServices'] == 'TRUE':
        return 'Parent Denial'
    elif row['ParentPermissionCodes'] == '3':
        return 'TDNQ'
    elif row['ParentPermissionCodes'] == 'E':
        return 'ELL'
    elif row['ParentPermissionCodes'] == 'G':
        return 'Exited-Monitored'
    elif row['LEPStatus'] == '1' or row[
        'Program Participation'] == 'Student Participates in an ESL Program' or pd.notnull(row['ELV_LEPStartDate_min']):
        return 'ELL'
    elif (nat_lang_eng or nat_lang_blank) and (home_lang_eng or home_lang_blank):  # If native language is English
        return 'Never Identified as EL'
    elif pd.to_datetime(row['ELV_Entry_Date']) < cutoff_date:
        # If enrolled date is before 30 days ago, student should be TNDQ
        return 'TDNQ'
    elif pd.to_datetime(row['ELV_Entry_Date']) > cutoff_date and row['LEPStatus'] == '0':
        return 'Action Required'
    else:
        return 'Lang Other than Eng or Blank'


def derive_dyslexic_status(row):
    if row['Dyslexia_Identified'] == 'No':
        return 'Not Dyslexic'
    if row['Dyslexia_Identified'] == 'Yes' and pd.isnull(row['Dyslexia_Dismissal_Date']):
        return 'Dyslexic'
    if row['Dyslexia_Identified'] == 'Yes' and pd.notnull(row['Dyslexia_Dismissal_Date']):
        return 'Not Dyslexic'
    else:
        return row['Dyslexia_Identified']

# set year is us schools based on status
def derive_yuss(row):
    if 'Monitored' in row['ELV_LEPStatus']:
        return ''
    elif row['ELV_LEPStatus'] == 'Fully_Exited':
        return ''
    elif row['ELV_LEPStatus'] == 'Exited-Monitored':
        return ''
    elif row['ELV_LEPStatus'] == 'Never Identified as EL':
        return ''
    elif row['ELV_LEPStatus'] == 'TDNQ':
        return ''
    elif row['ELV_LEPStatus'] == 'Lang Other than Eng or Blank':
        return ''
    elif row['Grade'] == 'K':
        return 'N/A'
    elif row['Grade'] == 'EE':
        return 'N/A'
    elif row['Grade'] == 'PK':
        return 'N/A'
    else:
        return row['ELV_YearsInUSSchools']

#derive receiving services based ppc and not status
def derive_receiving_services(row):
    if pd.isnull(row['ParentPermissionCodes']) and row['ELV_LEPStatus'] == 'ELL':
        return 'No'
    elif row['ParentPermissionCodes'] == 'E':
        return 'Yes'
    elif row['ParentPermissionCodes'] == 'G':
        return 'No'
    elif row['ParentPermissionCodes'] == '3':
        return 'Yes'
    elif row['ELV_LEPStatus'] == 'ELL':
        return 'Yes'
    else:
        return 'No'


if __name__ == '__main__':
    main()