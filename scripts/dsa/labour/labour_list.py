from typing import List, Optional

from sqlalchemy import text
from sqlalchemy.engine.base import Engine

def get_labour_and_delivery_list(engine: Engine, start_date: str, end_date: str) -> Optional[List[dict]]:
    """
    Returns a list of all labour and delivery data.

    :param engine: SQLAlchemy database engine object
    :param start_date: Start date of the query
    :param end_date: End date of the query
    :return: List of dictonaries
    """        
    
    sql = text(
    f"""
        DECLARE @StartDate DateTime
        DECLARE @EndDate DateTime
        DECLARE @ProvinceId VARCHAR(2)
        DECLARE @DistrictId varchar(3)
        DECLARE @FacilityId VARCHAR(9)
        DECLARE @LoginName VARCHAR(50)
        DECLARE @YearToday INT = YEAR(GETDATE())

        SET @StartDate = '{start_date}'
        SET @EndDate = '{end_date}'
        SET @ProvinceId = (select Value from Setting where Name = 'ProvinceId')
        SET @DistrictId = (select Value from Setting where Name = 'DistrictId')
        SET @FacilityId = (select Value from Setting where Name = 'FacilityHmisCode')

        --select * from Setting
        --Helper Fields

        declare @ProvinceName varchar(50)
        set @ProvinceName = dbo.fn_GetProvinceNames(@ProvinceId, @DistrictId, @FacilityId, ',')
        if (@ProvinceId IS NULL OR @ProvinceId = '')
            Begin
            set @ProvinceName = 'ALL'
            End

        declare @DistrictName varchar(50)
        set @DistrictName = dbo.fn_GetDistrictNames(@ProvinceId, @DistrictId, @FacilityId, ',')
        if (@DistrictId IS NULL OR @DistrictId = '')
            Begin
            set @DistrictName = 'ALL'
            End

        declare @FacilityName nvarchar(255)
        set @FacilityName = dbo.fn_GetFacilityNames(@ProvinceId, @DistrictId, @FacilityId, ',')
        if (@FacilityId IS NULL OR @FacilityId = '')
            Begin
            set @FacilityName = 'ALL'
            End
        --

        select distinct
                    DAI.overview_interaction_location as FacilityID,
                    R.PatientID,
                    R.FirstName + ' ' + R.Surname  as ClientName,
            CONVERT(varchar, R.DateOfBirth, 103) as DateOfBirth,
            Age = @YearToday - r.BirthYear,
            case 
            when (A.PhoneNumber is not null  and A.PhoneNumber <> '') then A.PhoneNumber
            when (A.PhoneNumber is not null and A.PhoneNumber = '' and A.MobilePhoneNumber is not null and A.MobilePhoneNumber <> '') then A.MobilePhoneNumber 
            else 'Unknown' end as PhoneNumber,
                    DAI.mother_admission_in_patient_register_number as InPatientRegisterNumber,
                    ISNULL(DAI.mother_admission_safe_motherhood_number, 'Unknown') as SafeMotherhoodNumber,
                    CONVERT(varchar, DAI.overview_interaction_time, 8) as InteractionTime,
                    CONVERT(varchar, DAI.overview_interaction_time, 103) AS DateOfAdmission,
                    case (DAI.pmtctd_elivery_feeding_options)
                    when 0
                    then 'IFB'
                    when 1
                    then 'IFR'
                    when 2
                    then 'Mixed'
                    when 3
                    then 'N/A'
                    end
                    as FeedingMethod,

                    case(DI.mother_labour_delivery_delivery_location)
                    when 0 then 'Home'
                    when 1 then 'Clinic'
                    end
                    as Deliverylocation,

                    DI.mother_labour_delivery_number_of_births as NumberOfBirths,

                    case (DI.new_born_delivery_details_1_birth_outcome)
                    when 0
                    then 'Alive'
                    when 1
                    then 'Fresh Stillbirth'
                    when 2
                    then 'Macerated Stillbirth'
                    when 3
                    then 'Neonatal Death'
                    else null
                    end
                    as BirthType,
                    case (year(DDI.mother_treatment_postnatal_visit_date))
                    when 1900
                    then null
                    else DDI.mother_treatment_postnatal_visit_date
                    end
                    as PlannedInitialFollow_UpDate                

                from
                    GuidMap as GTNN
                    join (select *
                        from DeliveryAdmissionInteraction3
                        where overview_deprecated = 0
                                and (pmtctd_elivery_consent_to_arv is not null
                                    or pmtctd_elivery_feeding_options is not null
                                    or pmtctd_elivery_mother_arv_prohylaxis is not null
                                    or pmtctd_elivery_postcounseling is not null
                                    or pmtctd_elivery_precounseling is not null
                                    or pmtctd_elivery_refer_to_art is not null
                                    or pmtctd_elivery_result_given is not null
                                    or pmtctd_elivery_results is not null)
                                    ) as DAI on DAI.patient_guid = GTNN.naturalNumber
                    left join (select *
                            from DeliveryInteraction3
                            where overview_deprecated = 0
                            ) as DI
                    on DI.mother_labour_delivery_in_patient_register_number = DAI.mother_admission_in_patient_register_number
                    left join (select *
                            from DeliveryDischargeInteraction2
                            where overview_deprecated = 0
                                    and overview_interaction_time between @StartDate and @EndDate
                                    ) as DDI
                    on DDI.mother_discharge_in_patient_register_number = DI.mother_labour_delivery_in_patient_register_number
                    join Registration as R on R.PatientGUID = GTNN.MappedGuid
                    join Address as A on A.PatientGUID = R.PatientGUID
                    WHERE ( DAI.overview_interaction_time BETWEEN @StartDate AND @EndDate)
    """)
    result = engine.execute(sql)
    rows = [dict(row) for row in result.fetchall()]
    return rows