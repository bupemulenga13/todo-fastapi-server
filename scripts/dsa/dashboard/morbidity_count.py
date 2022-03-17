from typing import List, Optional

from sqlalchemy import text
from sqlalchemy.engine.base import Engine

def get_morbidity_count(engine: Engine, start_date: str, end_date: str) -> Optional[List[dict]]:
    """
    Returns a count of morbidity data for the given date range.
    
    :param engine: SQLAlchemy database engine object
    :param start_date: Start date of the query
    :param end_date: End date of the query
    :return: List of dictonaries
    """
    sql = text(
        f"""
        DECLARE @StartDate DATETIME
DECLARE @EndDate DATETIME
DECLARE @ProvinceId VARCHAR(2)
DECLARE @DistrictId varchar(3)
DECLARE @FacilityId VARCHAR(9)
DECLARE @LoginName VARCHAR(50)

SET @StartDate = '{start_date}'
SET @EndDate = '{end_date}'
SET @ProvinceId = (select Value from Setting where Name = 'ProvinceId')
SET @DistrictId = (select Value from Setting where Name = 'DistrictId')
SET @FacilityId = (select Value from Setting where Name = 'FacilityHmisCode')


IF object_id('tempdb..#HmisCodes') is not null
      DROP TABLE #HmisCodes
    SELECT SubSiteId
	into #HmisCodes 
                           from fn_GetFacilitySubSiteIds(@ProvinceId, @DistrictId, @FacilityId)
						   

    declare @facility_name varchar(100)

    if (@FacilityId IS NULL OR @FacilityId = '')
      Set @facility_name = 'All'
    else set @facility_name = (select dbo.fn_GetFacilityNames(@ProvinceId, @DistrictId, @FacilityId, ', '))

    declare @ProvinceNames varchar(50)
    if (@ProvinceId IS NULL OR @ProvinceId = '')
      set @ProvinceNames = 'All'
    else set @ProvinceNames = (select dbo.fn_GetProvinceNames(@ProvinceId, @DistrictId, @FacilityId, ', '))

    declare @DistrictNames varchar(50)
    if (@DistrictId IS NULL OR @DistrictId = '')
      set @DistrictNames = 'All'
    else set @DistrictNames = (select dbo.fn_GetDistrictNames(@ProvinceId, @DistrictId, @FacilityId, ', '))


    select
      r.FirstName,
      r.Surname,
      ISNULL(d.DeathRecordNumber, 'Unknown') as DeathRecordNumber,
      d.VisitDate                               as 'DateOfRegistration',
      d.DateOfDeath,
      d.AgeOfDeceased
      ,
      ISNULL(d.UnderlyingCauseOfDeath, 'Unknown')                  as 'CauseOfDeath',
      (select top 1 t.Name
       from District as t
       where t.DistrictSeq = d.DistrictOfDeath) as 'PlaceOfDeath',
      (select @facility_name)                   as facilityName,
      (select @LoginName)                       as LoginName,
      (select @ProvinceNames)                   as ProvinceNames,
      (select @DistrictNames)                   as DistrictNames

    from crtRegistrationInteraction as r
      join crtReportOfDeath as d on d.PatientId = r.patientId_int
    where
      d.VisitDate between @StartDate and @EndDate
      and d.EditLocation in (SELECT SubSiteId
                             FROM #HmisCodes)
      and r.Deprecated = 0
      and d.Deprecated = 0
      AND (D.DateOfDeath BETWEEN @StartDate AND @EndDate)       
""")
    result = engine.execute(sql)
    rows = [dict(row) for row in result.fetchall()]
    return rows