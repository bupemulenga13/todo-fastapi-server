from typing import List, Optional

from sqlalchemy import text
from sqlalchemy.engine.base import Engine

def get_diagnostics_list(engine: Engine, start_date: str, end_date: str) -> Optional[List[dict]]:
    """
    Returns a list of all diagnostics.

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
        SET NOCOUNT ON


        IF object_id('tempdb..#HmisCodes') is not null
            DROP TABLE #HmisCodes
            SELECT SubSiteId
            into #HmisCodes 
                                from fn_GetFacilitySubSiteIds(@ProvinceId, @DistrictId, @FacilityId)


            select CONVERT(varchar, reg.VisitDate, 103)                                       as DateOfRegistration,
                Reg.PatientId                                                     as NUPN,
                CONVERT(varchar, Reg.DateOfBirth, 103)                                    as DateOfBirth,
                Reg.Sex,
                reg.FirstName + ' ' + reg.SurName as ClientName,
                dbo.fn_Age(Reg.DateOfBirth, getDate())                             as Age,
                ISNULL(ad.HousePlotNumber + ' ' + ad.StreetName + ' ' + ad.CommunityName, 'Unknown') as Location,
                case 
                    when (ad.PhoneNumber is not null  and ad.PhoneNumber <> '') then ad.PhoneNumber
                    when (ad.PhoneNumber is not null and ad.PhoneNumber = '' and ad.MobilePhoneNumber is not null and ad.MobilePhoneNumber <> '') then ad.MobilePhoneNumber 
                    else 'Unknown' end as PhoneNumber,
                --clinical interaction
                CONVERT(varchar, p.InteractionTime, 103)                                  as DateOfInteraction,
                cods.Title                                                        as Complaints,
                ISNULL(COALESCE(look.Title, cods2.Title), 'Unknown')                                    Diagnosis,
                ISNULL(look.ICD10ID, 'Unknown')                                                      AS Icd10,
                ISNULL(cods2.ICPC2eCode, 'Unknown')                                                  AS ICPC2e,
                Case Certainty
                    when 0 then 'Confirmed'
                    when 1 then 'Suspected'
                    when 2 then 'Rule-out'
                    when 3 then 'Ruled out'
                else null end as Certainty,
            CONVERT(varchar, OnsetDate, 103) as OnsetDate
            from (
                    select PatientId,
                            InteractionTime,
                            CCProblem,
                            ICPCProblem,
                            ICDProblem,
                            Certainty,
                            OnsetDate,
                            InteractionID,
                            EditLocation
                    from (
                            select PatientId,
                                    InteractionTime,
                                    CCProblem,
                                    ICPCProblem,
                                    ICDProblem,
                                    Certainty,
                                    OnsetDate,
                                    p.InteractionID,
                                    row_number()
                                            over ( partition by PatientId, problemepisodeid
                                                order by onsetdate desc ) seq ,
                                                p.EditLocation
                            from crtProblemEpisode p
                                    join crctProblemEpisode c
                                            on p.InteractionID = c.InteractionID and p.EditLocation = c.EditLocation
                                                and p.EditLocationSeqNumber = c.EditLocationSeqNumber
                            where CCProblem > 0
                                and InteractionTime between DATEADD(dd, DATEDIFF(dd, 0, @StartDate), 0) and DATEADD(dd,
                                                                                                                    DATEDIFF(dd,
                                                                                                                            0,
                                                                                                                            @EndDate),
                                                                                                                    0)
                        ) x
                    where seq = 1
                ) p
                    left join crtRegistrationInteraction reg on p.PatientId = reg.patientId_int
                    left join Registration r2 on reg.PatientID = r2.PatientId
                    left join Address ad on ad.PatientGUID = r2.PatientGUID and ad.AddressType = 'Current'
                    left join ICPC2eCodes cods on p.CCProblem = cods.ICPCConceptID
                    left join ICPC2eCodes cods2 on p.ICPCProblem = cods2.ICPCConceptID
                    left join ICD10Lookup look on p.ICDProblem = look.ICD10ConceptID
                    left join Interaction inter on p.InteractionID = inter.InteractionID
                    where p.EditLocation in (select SubSiteId from #HmisCodes)
                    AND InteractionTime BETWEEN @StartDate AND @EndDate
    """)
    result = engine.execute(sql)
    rows = [dict(row) for row in result.fetchall()]

    return rows