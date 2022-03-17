from typing import List, Optional

from sqlalchemy import text
from sqlalchemy.engine.base import Engine

def get_dispensations_list(engine: Engine, start_date: str, end_date: str) -> Optional[List[dict]]:
    """
    Returns a list of all dispensations.

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
        set @QuarterYear = 2020

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

            SELECT distinct   R.PatientId,
                    
            r.ArtNumber,
            r.FirstName + ' ' + r.SurName as ClientName,
            
            r.Sex,
            Age = @YearToday - r.BirthYear,
            case 
            when (ad.PhoneNumber is not null  and ad.PhoneNumber <> '') then ad.PhoneNumber
            when (ad.PhoneNumber is not null and ad.PhoneNumber = '' and ad.MobilePhoneNumber is not null and ad.MobilePhoneNumber <> '') then ad.MobilePhoneNumber 
            else 'Unknown' end as PhoneNumber,
        CONVERT(varchar, DATEADD(DD, DATEDIFF(DD, 0, p.VisitDate), 0), 103) AS VisitDate,
                substring(p.Location, 1, 6) as Hmiscode,
                GenericName as DrugDispensed,
                        GenericStrength as DispensedDrugStrength, 
                ComponentCount,
                CONVERT(varchar, p.NextAppointmentDate, 103) as NextAppointmentDate,
                --******
                --Uses MasterDefinitions.DurationsFromDispensations.
                --To Change, first apply change on MasterDefinitions. Durations from Dispensations.
                --TODO Duration = (Product Strength * Quantity Dispensed) / (Frequency * Dose)
                CASE
                    WHEN (
                            (
                                CASE
                                    WHEN
                                        m.UnitsDispensed > 100 AND CalcUom = 'mg/mL'
                                        THEN
                                        (
                                                m.UnitsDispensed /
                                                (CASE
                                                    (DosesPerDay * m.UnitQuantityPerDose *
                                                    (SUBSTRING(genericstrength, 1, CHARINDEX(' ', genericstrength, 1))))
                                                    WHEN 0
                                                        THEN NULL
                                                    ELSE (DosesPerDay * m.UnitQuantityPerDose *
                                                        (SUBSTRING(genericstrength, 1, CHARINDEX(' ', genericstrength, 1)))) END)
                                            )
                                    ELSE
                                        (m.UnitsDispensed /
                                        (CASE (DosesPerDay * m.UnitQuantityPerDose)
                                            WHEN 0
                                                THEN NULL
                                            ELSE (DosesPerDay * m.UnitQuantityPerDose) END
                                            )
                                            )
                                    END
                                )
                                ) > 180
                        THEN 180
                    ELSE (
                        CASE
                            WHEN
                                m.UnitsDispensed > 100 AND CalcUom = 'mg/mL'
                                THEN
                                (
                                        m.UnitsDispensed /
                                        (CASE (DosesPerDay * m.UnitQuantityPerDose *
                                                (SUBSTRING(genericstrength, 1, CHARINDEX(' ', genericstrength, 1))))
                                                WHEN 0
                                                    THEN NULL
                                                ELSE (DosesPerDay * m.UnitQuantityPerDose *
                                                    (SUBSTRING(genericstrength, 1, CHARINDEX(' ', genericstrength, 1)))) END)
                                    )
                            ELSE
                                (m.UnitsDispensed /
                                    (CASE (DosesPerDay * m.UnitQuantityPerDose)
                                        WHEN 0
                                            THEN NULL
                                        ELSE (DosesPerDay * m.UnitQuantityPerDose) END)
                                    )
                            END)
                    END
                                                            AS DurationInDays

                --******

            FROM crctMedicationDispensingDetails m
                    JOIN crtMedicationsDispensed p
                        ON m.InteractionID = p.InteractionID
                    JOIN DrugProductsCachedView d ON m.MedDrugId = d.MedDrugId
                    JOIN MedFrequency f ON m.Frequency = f.FrequencyID
                    JOIN crtRegistrationInteraction r on r.patientId_int = p.PatientId
                    JOIN GuidMap mp on r.patientId_int = mp.NaturalNumber
                    LEFT JOIN #currentAddresses ad on mp.MappedGuid = ad.PatientGUID
                    WHERE D.drugclass = 'Antiretrovirals' and m.EditLocation in (select SubSiteId from #HmisCodes) AND (p.VisitDate BETWEEN @StartDate AND @EndDate) AND p.ArvDrugDispensed = 1
    """)
    result = engine.execute(sql)
    rows = [dict(row) for row in result.fetchall()]
    print("Dispensations: ", len(rows))
    return rows