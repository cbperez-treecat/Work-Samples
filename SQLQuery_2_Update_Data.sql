--Query to move data from one student to another to dedupe and combine data

--Backup
BEGIN TRANSACTION 
SELECT * INTO [EllevationUtility].[dbo].[NM-SCHOOLDISTRICT-tblStudentStandardizedTest-DI-34343]
FROM [ESLREPS-NM-SCHOOLDISTRICT].[dbo].[tblStudentStandardizedTest]
WHERE StudentID = 15812
AND AssessmentDate = '5/1/2021' 
AND AssessmentTypeID in (77, 79, 84)
COMMIT TRANSACTION

BEGIN TRAN
SELECT * FROM tblStudentStandardizedTest -- These four lines are to check the number of rows we will change
WHERE StudentID = 15812
AND AssessmentDate = '5/1/2021' 
AND AssessmentTypeID in (77, 79, 84)

UPDATE tblStudentStandardizedTest
SET StudentID = 16096,
LastChangedBy = '247847'
WHERE StudentID = 15812
AND AssessmentDate = '5/1/2021' 
AND AssessmentTypeID in (77, 79, 84)

SELECT * FROM tblStudentStandardizedTest -- These four lines are to check the number of rows changed
WHERE StudentID = 16096
AND AssessmentDate = '5/1/2021' 
AND AssessmentTypeID in (77, 79, 84)

COMMIT TRAN