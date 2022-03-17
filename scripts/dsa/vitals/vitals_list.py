from typing import List, Optional

from sqlalchemy import text
from sqlalchemy.engine.base import Engine

def get_vitals_list(engine: Engine, start_date: str, end_date: str) -> Optional[List[dict]]:
    """
    Returns a list of all vitals.

    :param engine: SQLAlchemy database engine object
    :param start_date: Start date of the query
    :param end_date: End date of the query
    :return: List of dictonaries
    """
        
    sql = text(
    f"""
        SET NOCOUNT ON
        IF (object_id('tempdb..#currentAddresses')) IS NOT NULL
        DROP TABLE #currentAddresses;
        select distinct PatientGuid, PhoneNumber, MobilePhoneNumber 
        into #currentAddresses
        from Address 
        where AddressType = 'Current'

        SELECT DISTINCT

        a.PatientId,
        rg.FirstName + ' ' + rg.SurName as ClientName,
        lg.UserName as ServiceProvider,
        lg.LoginID as ProviderId,
        srv.ServiceName,
        srv.ServiceCode,
        rg.PatientId as PatientNUPIN,
        rg.ArtNumber,
        rg.Sex,
        dbo.fn_Age(rg.DateOfBirth, GETDATE()) as Age,
        case 
            when (ad.PhoneNumber is not null  and ad.PhoneNumber <> '') then ad.PhoneNumber
            when (ad.PhoneNumber is not null and ad.PhoneNumber = '' and ad.MobilePhoneNumber is not null) then ad.MobilePhoneNumber 
        else '' end as PhoneNumber,
        CONVERT(VARCHAR, a.InteractionTime, 103) AS DateServiceProvided,
        -- CONVERT(VARCHAR,MAX(a.InteractionTime), 103) as  DateServiceProvided,
        a.[Temperature]
        ,a.[Weight]
        ,a.[Height]
        ,a.[Bmi]
        ,a.[PulseRate]
        ,a.[RespiratoryRate]
        ,a.[SystolicPressure]
        ,a.[DiastolicPressure]
        ,a.[O2Saturation]
        ,vl.LabTestValue
        ,vl.VLResultDate
        ,b.Deprecated as InteractionDepricated
        ,rg.Deprecated as RegDepricated
        ,srv.Deprecated as ServiceDepricated


        FROM crtVitals a
        LEFT JOIN crtRegistrationInteraction rg on a.PatientId = rg.patientId_int
        LEFT JOIN GuidMap mp on a.PatientId = mp.NaturalNumber
        LEFT JOIN #currentAddresses ad on mp.MappedGuid = ad.PatientGUID
        LEFT JOIN InteractionOverviewNg b on a.InteractionID = b.InteractionIdInt AND a.InteractionTime = b.InteractionTime
        LEFT JOIN Login lg on  CAST(b.InteractionUser as VARCHAR(10)) = CAST(lg.LoginID as Varchar(10))
        LEFT JOIN (SELECT PatientId, MAX(InteractionDate) as VLDate FROM ClientViralLoadTestResult GROUP BY PatientId) vl1 on a.PatientId = vl1.PatientId
        LEFT JOIN (SELECT PatientId, InteractionDate as VLResultDate,  LabTestValue FROM ClientViralLoadTestResult GROUP BY PatientId, LabTestValue, InteractionDate) vl on vl1.PatientId = vl.PatientId AND vl1.VLDate = vl.VLResultDate
        left JOIN ServiceCodes srv on b.ServiceCode = srv.ServiceCode
        WHERE a.InteractionTime BETWEEN '{start_date}' AND '{end_date}' 
        --AND b.Deprecated = 0 
        --AND rg.Deprecated = 0 
        --AND srv.Deprecated = 0
        ORDER BY PatientId
    """)
    result = engine.execute(sql)
    rows = [dict(row) for row in result.fetchall()]
    return rows