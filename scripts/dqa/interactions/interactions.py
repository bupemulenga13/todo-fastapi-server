from typing import List, Optional

from sqlalchemy import text
from sqlalchemy.engine.base import Engine

def get_interactions(engine: Engine, start_date: str, end_date: str) -> Optional[List[dict]]:
	"""
    Returns a list of interactions data
    :param engine: SQLAlchemy database engine object
    :param start_date: Start date of the query
    :param end_date: End date of the query
    :return: List of dictonaries
	SET NOCOUNT ON
	SET @StartDate = '{start_date}'
	SET @EndDate = '{end_date}'

    """        
	sql = text(
	f"""
    SELECT
        inter.ServiceCode as serviceCode
        ,srv.ServiceName as serviceName
        ,COUNT(*) AS visits 
    FROM InteractionOverviewNg inter
    LEFT JOIN ServiceCodes srv on inter.ServiceCode = srv.ServiceCode
    WHERE inter.InteractionTime BETWEEN '{start_date}' and '{end_date}' AND inter.Deprecated = 0 and srv.Deprecated = 0
    GROUP BY inter.ServiceCode, srv.ServiceName
    ORDER BY inter.ServiceCode


	 """)

	result = engine.execute(sql)
	rows = [dict(row) for row in result.fetchall()]
	return rows 