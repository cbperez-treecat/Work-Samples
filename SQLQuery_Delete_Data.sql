--SQL Query to remove designated supports assigned to inactive students

--backup data into table; join tables on student key with aliases
--District requested dates based on user error 
SELECT A.*
FROM [ESLREPS-MA-SCHOOLDISTRICT].[dbo].[tblStudentStandardizedTestAccommodation] A
JOIN [ESLREPS-MA-SCHOOLDISTRICT].[dbo].[tblStudent] S 
    ON A.StudentID = S.StudentID
WHERE 
    S.StudentActive = 0 
    AND A.DistrictAccommodationID = 1 
    AND year(A.LastChanged) = 2022 
    AND month(A.LastChanged) = 02 
    AND day(A.LastChanged) = 15

    --Delete assignments from students 
BEGIN TRAN
DELETE A
FROM [ESLREPS-MA-SCHOOLDISTRICT].[dbo].[tblStudentStandardizedTestAccommodation] A
JOIN [ESLREPS-MA-SCHOOLDISTRICT].[dbo].[tblStudent] S 
    ON A.StudentID = S.StudentID
WHERE 
    S.StudentActive = 0 
    AND A.DistrictAccommodationID = 1 
    AND year(A.LastChanged) = 2022 
    AND month(A.LastChanged) = 02 
    AND day(A.LastChanged) = 15
COMMIT TRAN