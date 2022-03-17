from datetime import date
from typing import List, Optional

from sqlalchemy import text
from sqlalchemy.engine.base import Engine

def get_attendance_list(engine: Engine, selected_date: str or date) -> Optional[List[dict]]:
    """
    Returns a list of attendances.

    :param engine: SQLAlchemy database engine object
    :param selected_date: Date to check for attendance list
    :return: List of dictonaries
    """        
    
    sql = text(
    f"""
       DECLARE @ReportingDate Datetime
    --SET @ReportingDate = '2018-02-01'
    
    SET @ReportingDate = {selected_date}
    SET NOCOUNT ON
    
    IF (object_id('tempdb..#currentAddresses')) IS NOT NULL
         DROP TABLE #currentAddresses;
     select distinct PatientGuid, PhoneNumber, MobilePhoneNumber 
     into #currentAddresses
     from Address 
     where AddressType = 'Current'
    
    select DISTINCT
    d.PatientId
    ,reg.FirstName + ' ' + reg.SurName as ClientName
    ,reg.Sex
    ,dbo.fn_Age(reg.DateOfBirth, @ReportingDate) as Age
    ,case 
     when (ad.PhoneNumber is not null  and ad.PhoneNumber <> '') then ad.PhoneNumber
     when (ad.PhoneNumber is not null and ad.PhoneNumber = '' and ad.MobilePhoneNumber is not null and ad.MobilePhoneNumber <> '') then ad.MobilePhoneNumber 
     else 'Unknown' end as PhoneNumber
    ,reg.ArtNumber
    ,CONVERT(VARCHAR, d.InteractionDate, 103) as PriorVisitDate
    ,CONVERT(VARCHAR, d.NextVisitDate, 103) as NextVisitDate
    ,d.VisitType
    ,sv.ServiceName
    ,CONVERT(VARCHAR,d1.InteractionDate,103) as LatestVisitDate
    ,case
        WHEN d1.InteractionDate is null then 'Missed'
        ELSE 'Attended'
    end as 'AppointmentStatus'
    from ClientClinicalCareDates d
    LEFT JOIN crtRegistrationInteraction reg on d.PatientId = reg.patientId_int
    LEFT JOIN GuidMap mp on d.PatientId = mp.NaturalNumber
    left join #currentAddresses ad on mp.MappedGuid = ad.PatientGUID
    join ServiceCodes sv on d.VisitType = sv.ServiceCode
    left join 
    (select patientid,max(interactiondate) as InteractionDate 
    from ClientClinicalCareDates d1  where d1.InteractionDate > @ReportingDate group by d1.PatientId)
    d1 on d.PatientId = d1.PatientId
    where d.NextVisitDate = @ReportingDate
    AND sv.Deprecated = 0
    order by PatientId, PriorVisitDate            
    -- )

    """)
    result = engine.execute(sql)
    rows = [dict(row) for row in result.fetchall()]
    return rows