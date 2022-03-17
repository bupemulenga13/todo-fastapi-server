from typing import List, Optional

from sqlalchemy import text
from sqlalchemy.engine.base import Engine

def get_clinical_visits_list(engine: Engine, start_date: str, end_date: str) -> Optional[List[dict]]:
    """
    Returns a list of all clinical visits.

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
        Declare @QuarterYear int
        DECLARE @YearToday INT = YEAR(GETDATE())
        
        
        SET @StartDate = '{start_date}'
        SET @EndDate = '{end_date}'
        SET @ProvinceId = (select Value from Setting where Name = 'ProvinceId')
        SET @DistrictId = (select Value from Setting where Name = 'DistrictId')
        SET @FacilityId = (select Value from Setting where Name = 'FacilityHmisCode')
        
        IF (object_id('tempdb..#currentAddresses')) IS NOT NULL
            DROP TABLE #currentAddresses;
        select distinct PatientGuid, PhoneNumber, MobilePhoneNumber 
        into #currentAddresses
        from Address 
        where AddressType = 'Current'
        
        
        
        IF object_id('tempdb..#HmisCodes') is not null
                DROP TABLE #HmisCodes
        
        SELECT SubSiteId
        into #HmisCodes from fn_GetFacilitySubSiteIds(@ProvinceId, @DistrictId, @FacilityId)
        
        SELECT DISTINCT
        c.PatientId,
        c.FirstName + ' ' + c.SurName as ClientName,
        c.Sex,
        c.DateOfBirth,
        c.Age,
        c.UserName as ServiceProvider,
        c.ServiceName,
        c.PhoneNumber,
        --CONVERT(varchar, sm.PriorClinicalVisitDate, 103) AS PriorClinicalVisitDate,
        c.VisitDate,
        c.NextVisitDate,
        c.HmisCode,
        vl.LabTestValue
        
        FROM   
        (select  reg.PatientId,
            reg.SurName,
            reg.FirstName,
            reg.Sex,
            CONVERT(varchar, reg.DateOfBirth, 103) as DateOfBirth,
            Age = @YearToday - reg.BirthYear,
            servicex.ServiceName,
            case 
            when (ad.PhoneNumber is not null  and ad.PhoneNumber <> '') then ad.PhoneNumber
            when (ad.PhoneNumber is not null and ad.PhoneNumber = '' and ad.MobilePhoneNumber is not null and ad.MobilePhoneNumber <> '') then ad.MobilePhoneNumber 
            else 'Unknown' end as PhoneNumber,
            CONVERT(varchar, visit.InteractionDate, 103) as VisitDate,
            CONVERT(varchar, visit.NextVisitDate, 103) as NextVisitDate,
            visit.Location as HmisCode,
            reg.patientId_int,
            l.UserName
        from ClientClinicalCareDates visit
        join GuidMap mp on visit.PatientId = mp.NaturalNumber
        left join #currentAddresses ad on mp.MappedGuid = ad.PatientGUID
        Join InteractionOverviewNg b on visit.InteractionID = b.InteractionIdInt 
        Join Login l on CAST(l.LoginID AS VARCHAR(10)) =  cast((b.InteractionUser) as VARCHAR(10))
        join ServiceCodes servicex on visit.VisitType = servicex.ServiceCode
        join crtRegistrationInteraction reg on reg.patientId_int = visit.PatientId
        where visit.InteractionDate BETWEEN @StartDate and @EndDate
        and visit.EditLocation in (select SubSiteId from #HmisCodes
        )) c
        join [ClientViralLoadTestResult] vl on c.patientId_int = vl.PatientId
        join ( SELECT [PatientId]
            ,max(InteractionDate) latestVlDate
            FROM [ClientViralLoadTestResult]
            Group by PatientId
        ) vl1
        ON vl.[PatientId] = vl1.[PatientId] and vl.InteractionDate = vl1.latestVlDate
        join (SELECT Distinct w.PatientId,t.LatestDate,w.WhoStageToday
        FROM [cdc_fdb_db].[dbo].[ClientHivWhoStage] w
        join (
        SELECT  PatientId,
            Max([VisitDate]) as LatestDate
        FROM [cdc_fdb_db].[dbo].[ClientHivWhoStage] 
        GROUP BY  PatientId)
        t on  t.LatestDate = w.VisitDate and t.PatientId = w.PatientId
        where w.WhoStageToday = 1 OR w.WhoStageToday = 2
        group by w.PatientId,t.LatestDate,w.WhoStageToday
        ) f
        on f.PatientId = patientId_int
        JOIN ClientClinicalCareDates d on c.patientId_int = d.PatientId
        JOIN (
        SELECT DISTINCT PatientId, 
        MAX(NextVisitDate) as NextVisitDate 
        from ClientClinicalCareDates 
        GROUP BY PatientId) d1
        on d1.PatientId = d.PatientId 
        AND d1.NextVisitDate = d.NextVisitDate
    """)
    result = engine.execute(sql)
    rows = [dict(row) for row in result.fetchall()]
    return rows