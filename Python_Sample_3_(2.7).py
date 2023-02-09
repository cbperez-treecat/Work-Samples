"""
Cristina Perez 2021
"""

import pandas as pd
import numpy as np
import os

from glob import glob
from pyep.assessmentqc import COLUMNS, AssessmentQC
from pyep.read_file import read_simple
from pyep.settings import WORKSPACE_ROOT

DISTRICT = 'tx-district'
FOLDER_NAME = 'ipt_oral_spanish'
FILE_NAME_OR_MASK = 'Oral IPT Fall 2020_2021 (1).csv'

TEST_NAME = 'IPT'
TEST_TYPE_NAME = 'IPT I-Oral Spanish'
TEST_SUBJECT_NAME = 'SLA'

STUDENT_LOCAL_ID_COL = 'Student_ID'
STUDENT_STATE_ID_COL = None
STUDENT_GRADE_LEVEL_COL = 'Grade Assessed'
TEST_DATE_COL = 'Oral_Test_Date'
DEFAULT_DATE = None  # if no default date, leave as None

SCORE_1_COL = 'Oral_Scaled_Score'
SCORE_2_COL = 'Oral_Prof_Level'
SCORE_3_COL = 'Oral_Percentile'
SCORE_4_COL = 'Oral_Raw_Score'
SCORE_5_COL = 'Oral_Score_Level'
SCORE_6_COL = 'Oral_Plac_NCE'
SCORE_7_COL = 'Oral_Levels_Taken'


# Add more constants here as needed


def main():
    file_path_mask = os.path.join(WORKSPACE_ROOT, DISTRICT, FOLDER_NAME, FILE_NAME_OR_MASK)
    all_file_path_list = glob(file_path_mask)
    unprocessed_file_path_list = [x for x in all_file_path_list if '-processed' not in x]

    if len(unprocessed_file_path_list) == 0:
        raise IOError('There are no files to be processed.')

    for file_path in unprocessed_file_path_list:
        file_name = os.path.basename(file_path)
        print '\n>>> Processing {}'.format(file_name)

        df = read_simple(file_path)
        df_out = process(df, file_name)

        assessment_qc = AssessmentQC(df=df_out,
                                     district=DISTRICT,
                                     folder_name=FOLDER_NAME,
                                     file_name=file_name,
                                     default_date=DEFAULT_DATE)
        assessment_qc.begin(check_database=True)


def process(df_src, src_file_name):
    """Processes data from test file.

    :param df_src: data frame of raw data
    :param district: district name
    :param src_file_name: source filename
    :return: DataFrame of processed data
    """
    level_map = {
        'Beginning': '1 - Beginning',
        'Intermediate': '2 - Intermediate',
        'Early Intermediate': '3 - Early Intermediate',
        'Early Advanced': '4 - Early Advanced',
        'Advanced': '5 - Advanced'
    }

    processed = pd.DataFrame(columns=COLUMNS)

    processed['StudentLocalID'] = df_src[STUDENT_LOCAL_ID_COL]  # delete if not provided
    #processed['StudentStateID'] = df_src[STUDENT_STATE_ID_COL]  # delete if not provided

    processed['StudentGradeLevel'] = df_src[STUDENT_GRADE_LEVEL_COL]
    processed['TestGradeLevel'] = processed['StudentGradeLevel']
    processed['TestDate'] = df_src[TEST_DATE_COL] if TEST_DATE_COL else DEFAULT_DATE

    processed['TestName'] = TEST_NAME
    processed['TestTypeName'] = TEST_TYPE_NAME
    processed['TestSubjectName'] = TEST_SUBJECT_NAME

    processed['Score1Label'] = 'Scale Score'
    processed['Score1Type'] = 'Scale'
    processed['Score1Value'] = df_src[SCORE_1_COL]

    processed['Score2Label'] = 'Performance Level'
    processed['Score2Type'] = 'Level'
    processed['Score2Value'] = df_src[SCORE_2_COL].map(level_map).fillna(df_src[SCORE_2_COL])

    processed['Score3Label'] = 'Percentile Score'
    processed['Score3Type'] = 'Scale'
    processed['Score3Value'] = df_src[SCORE_3_COL]
    # Add further Score# blocks here as needed

    processed['Score4Label'] = 'Oral Raw Score'
    processed['Score4Type'] = 'Scale'
    processed['Score4Value'] = df_src[SCORE_4_COL]

    processed['Score5Label'] = 'Score Level'
    processed['Score5Type'] = 'Level'
    processed['Score5Value'] = df_src[SCORE_5_COL]

    processed['Score6Label'] = 'NCE Score'
    processed['Score6Type'] = 'Scale'
    processed['Score6Value'] = df_src[SCORE_6_COL]

    processed['Score7Label'] = 'Oral Levels Taken'
    processed['Score7Type'] = 'Level'
    processed['Score7Value'] = df_src[SCORE_7_COL]

    processed['TestID'] = '0'
    processed['TestTypeID'] = '0'
    processed['TestSubjectID'] = '0'

    processed['DistrictDBName'] = 'ESLREPS-{}'.format(DISTRICT.upper())
    processed['SourceFile'] = src_file_name

    return processed


if __name__ == '__main__':
    main()