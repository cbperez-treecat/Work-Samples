import os
import re
import zipfile
import datetime as dt

import pandas as pd
import numpy as np
from pycron import fetch_from_sftp, copy_to_processor_directory
from pyep.transform import derive_from_date_exited_4y


def main():
    # DISTRICT VARIABLES
    district = 'tx-district'
    zipfile_name = 'ELLevation_ClassLink_Export.zip'
    remote_directory = '/data/'
    demo_filename = 'demographics.csv'
    eb_supp_filename = 'Ellevation_supp.csv'
    user_filename = 'users.csv'
    data_type = 'student'
    file_watcher_type = 'SIS'

    zip_path = fetch_from_sftp(district_name=district,
                               file_name=zipfile_name,
                               remote_directory=remote_directory,
                               data_type='zipfile',
                               add_datestamp=True)

    file_path = fetch_from_sftp(district_name=district,
                                file_name=eb_supp_filename,
                                remote_directory=remote_directory,
                                data_type=data_type,
                                add_datestamp=True)

    #Bring in files rom zip and csv
    df_demo = pd.read_csv(file_path, sep=',', dtype=str)

    #rename key to match
    df_demo = df_demo.rename(columns={'PER_ID': 'sourcedId'})

    zip_file = zipfile.ZipFile(zip_path)

    student_demo = pd.read_csv(zip_file.open(demo_filename), dtype=str, sep=',')

    user = pd.read_csv(zip_file.open(user_filename), dtype=str).query('role == "student"')

    #   ~~ PRE-PROCESSING ~~
    #merge all 3 files into one
    df_temp = pd.merge(student_demo, df_demo, how='left', on='sourcedId')
    df = pd.merge(df_temp, user, how='left', on='sourcedId')
    df.drop_duplicates(subset='sourcedId', inplace=True)

    #derive status
    df['ELV_EB_status'] = df.apply(derive_eb_status, axis=1)

    #derive days in district for students who are Never Identified + home lang not English + native lang not English
    df['ELV_Days_In_District'] = df.apply(derive_days_in_district, axis=1)

    #strip leading zero from grades
    df['grades'] = df['grades'].str.lstrip('0')

    df['ELV_CustomListEE'] = df.apply(derive_ee, axis=1)
    # ~~ END PRE-PROCESSING ~~

    # WRITE AND COPY FILE(S)
    output_name = 'OneRoster_Students_processed_{:%Y%m%d}.csv'.format(
        pd.to_datetime('today'))
    output_dir = os.path.join(os.path.dirname(os.path.dirname(zip_path)), data_type)
    output_path = os.path.join(output_dir, output_name)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    df.to_csv(output_path, index=False)
    copy_to_processor_directory(district_name=district,
                                file_name=output_path,
                                file_watcher_type=file_watcher_type)
#derive status
def derive_eb_status(row):
    if row['LEP_STATUS'] == '1' and row['PARENT_PERMISSION_CODE'] not in 'C':
        return 'EB'
    elif row['PARENT_PERMISSION_CODE'] == 'C':
        return 'Parent Denial'
    elif pd.isnull(row['LEP_STATUS']):
        return 'Never Identified'
    elif row['LEP_STATUS'] == '0':
        return 'Did Not Qualify'
    elif row['LEP_STATUS'] == 'F':
        return 'Monitored_Year_1'
    elif row['LEP_STATUS'] == 'S':
        return 'Monitored_Year_2'
    elif row['LEP_STATUS'] == '3':
        return 'Monitored_Year_3'
    elif row['LEP_STATUS'] == '4':
        return 'Monitored_Year_4'
    elif row['LEP_STATUS'] == '5':
        return 'Exited_Monitored_Complete'

#derive early education students
def derive_ee(row):
    if row['ELV_EB_status'] == 'EB' and row['grades'] == 'PK' and row['IEP'] == 'Yes':
        return 'Yes'
    elif row['ELV_EB_status'] == 'EB' and row['grades'] == 'PK' and row['IEP'] == 'No':
        return 'No'
    else:
        return np.NaN

#derive time in district
def derive_days_in_district(row):
    today = dt.datetime.today()
    eng = ['English']
    if row['ELV_EB_status'] == 'Never Identified' and row['HOME_LANGUAGE'] not in eng \
            and row['STUDENT_LANGUAGE'] not in eng:
        if (today - pd.to_datetime(row['DATE_ENROLLED_DISTRICT'])).days <= 7:
            return 'Week 1'
        elif (today - pd.to_datetime(row['DATE_ENROLLED_DISTRICT'])).days <= 14:
            return 'Week 2'
        elif (today - pd.to_datetime(row['DATE_ENROLLED_DISTRICT'])).days <= 21:
            return 'Week 3'
        elif (today - pd.to_datetime(row['DATE_ENROLLED_DISTRICT'])).days <= 28:
            return 'Week 4'
        else:
            return 'Beyond 4 Weeks'



if __name__ == '__main__':
    main()