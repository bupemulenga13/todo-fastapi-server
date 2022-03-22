from typing import List, Optional

from sqlalchemy import text
from sqlalchemy.engine.base import Engine

def get_facility_details(engine: Engine) -> Optional[List[dict]]:
	"""
    Returns a list of facility details data
    :param engine: SQLAlchemy database engine object
    :return: List of dictonaries
    """        
	sql = text(
	f"""
        IF (OBJECT_ID('tempdb..#TempFacilityImplementingPartner')) IS NOT NULL
        DROP TABLE #TempFacilityImplementingPartner
        --GO


        IF (OBJECT_ID('tempdb..#TempFacilityDetail')) IS NOT NULL
        DROP TABLE #TempFacilityDetail
        --GO

        SELECT 
            f.[DistrictId]
            ,f.[FacilityName]
            ,dst.Name as District
            ,prv.Name as Province
            ,MONTH(GETDATE()) as [Month]
            ,f.FacilityGuid
            INTO #TempFacilityDetail
        FROM [cdc_fdb_db].[dbo].[Facility] f
        INNER JOIN District dst on dst.DistrictSeq = f.DistrictId
        inner join Province prv on prv.ProvinceSeq = dst.ProvinceSeq
        WHERE f.FacilityGuid IN (SELECT [Value] as 'FacilityGuid'
        FROM [cdc_fdb_db].[dbo].[Setting]
        WHERE Name = 'FacilityGuid')
        --GO

        ALTER TABLE #TempFacilityDetail ADD FacilityImplementing VARCHAR(MAX);
        --GO

        UPDATE #TempFacilityDetail
        SET 
        FacilityImplementing = (
        SELECT 
        st.Value as FacilityImplementing
        FROM Setting st
        WHERE Name = 'FacilityImplementingPartner'
        )

        SELECT
        FacilityName
        ,Province
        ,District
        ,CASE
        WHEN [Month] = 1 THEN 'January'
        WHEN [Month] = 2 THEN 'February'
        WHEN [Month] = 3 THEN 'March'
        WHEN [Month] = 4 THEN 'April'
        WHEN [Month] = 5 THEN 'May'
        WHEN [Month] = 6 THEN 'June'
        WHEN [Month] = 7 THEN 'July'
        WHEN [Month] = 8 THEN 'August'
        WHEN [Month] = 9 THEN 'September'
        WHEN [Month] = 10 THEN 'October'
        WHEN [Month] = 11 THEN 'November'
        WHEN [Month] = 12 THEN 'December'
        END AS [Month]
        ,FacilityImplementing as IPName
        ,FacilityGuid
        FROM #TempFacilityDetail

	 """)

	result = engine.execute(sql)
	rows = [dict(row) for row in result.fetchall()]
	return rows 