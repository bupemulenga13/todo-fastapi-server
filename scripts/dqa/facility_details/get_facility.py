from typing import List, Optional

from sqlalchemy import text
from sqlalchemy.engine.base import Engine

def get_facility(engine: Engine) -> Optional[List[dict]]:
	"""
    Returns a list of facilities data
    :param engine: SQLAlchemy database engine object
    :param start_date: Start date of the query
    :param end_date: End date of the query
    :return: List of dictonaries
    """        
	sql = text(
	f"""
	SELECT 
	f.FacilityGuid as FacilityId
	,f.FacilityName
	,f.DistrictId
	,dst.Name as DistrictName
	,prv.ProvinceSeq as ProvinceId
	,prv.name as ProvinceName
	FROM Facility f
	LEFT JOIN District dst on f.DistrictId = dst.DistrictSeq
	LEFT JOIN Province prv on dst.ProvinceSeq = prv.ProvinceSeq
	WHERE f.FacilityGuid IN (SELECT [Value] as 'FacilityGuid'
	FROM [cdc_fdb_db].[dbo].[Setting]
	WHERE Name = 'FacilityGuid')

	 """)

	result = engine.execute(sql)
	rows = [dict(row) for row in result.fetchall()]
	return rows 