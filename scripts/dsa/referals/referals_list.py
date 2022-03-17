from typing import List, Optional

from sqlalchemy import text
from sqlalchemy.engine.base import Engine

def get_referals_list(engine: Engine, start_date: str, end_date: str) -> Optional[List[dict]]:
    """
    Returns a list of all referals.

    :param engine: SQLAlchemy database engine object
    :param start_date: Start date of the query
    :param end_date: End date of the query
    :return: List of dictonaries
    """        
    
    sql = text(
    f"""
        DECLARE @startdate as datetime
        DECLARE @enddate as datetime
        
        SET @startdate = '{start_date}'
        SET @enddate = '{end_date}'
        
        SELECT DISTINCT
        z.patient_art_number,
        case 
            when (z.PhoneNumber is not null  and z.PhoneNumber <> '') then z.PhoneNumber
            when (z.PhoneNumber is not null and z.PhoneNumber = '' and z.MobilePhoneNumber is not null) then z.MobilePhoneNumber 
            else '' end as PhoneNumber,
        z.FirstName + ' ' + z.SurName as ClientName,
        z.Sex, 
        z.Age,
        case
        when (z.Age < 1 ) then '<1'
        when (z.Age BETWEEN 1 AND 4 ) then '1 - 4'
        when (z.Age BETWEEN 5 AND 9 ) then '5 - 9'
        when (z.Age BETWEEN 10 AND 14 ) then '10 - 14'
        when (z.Age BETWEEN 15 AND 19 ) then '15 - 19'
        when (z.Age BETWEEN 20 AND 24 ) then '20 - 24'
        when (z.Age BETWEEN 25 AND 29 ) then '25 - 29'
        when (z.Age BETWEEN 30 AND 34 ) then '30 - 34'
        when (z.Age BETWEEN 35 AND 39 ) then '35 - 39'
        when (z.Age BETWEEN 40 AND 44 ) then '40 - 44'
        when (z.Age BETWEEN 45 AND 49 ) then '45 - 49'
        when (z.Age > 50 ) then '50+'
        end as AgeCategory,
        z.InteractionDate,
        z.other_referrals,
        z.consented_to_hbc_,
        z.adherence_counselling_,
        z.family_planning_,
        z.treatment_preparation,
        z.inpatient_care_this_facility,
        z.nutritional_support,
        z.tb_treatment_dot_program,
        z.ca_cx,
        z.referrals_inpatient_care,
        z.psychosocial_support,
        z.NO_Referal,
        vl.LabTestValue,
        f.WhoStageToday
        FROM
        (select 
        patient_art_number,
        PhoneNumber,
        MobilePhoneNumber,
        r.FirstName,
        r.SurName,
        r.patientId_int,
        r.Sex,
        Age = dbo.fn_Age(r.DateOfBirth, @enddate),
        r.DateOfBirth,
        CONVERT(varchar, x.InteractionDate, 103) as InteractionDate,
        other_referrals,
        case when consented_to_hbc is null then 0 else 1 end consented_to_hbc_,
        case when adherence_counselling is null then 0 else 1 end adherence_counselling_,
        case when family_planning is null then 0 else 1 end family_planning_,
        case when treatment_preparation is null then 0 else 1 end treatment_preparation,
        case when inpatient_care_this_facility is null then 0 else 1 end inpatient_care_this_facility,
        case when nutritional_support is null then 0 else 1 end nutritional_support,
        case when tb_treatment_dot_program is null then 0 else 1 end tb_treatment_dot_program,
        case when ca_cx is null then 0 else 1 end ca_cx,
        case when referrals_inpatient_care is null then 0 else 1 end referrals_inpatient_care,
        --case when other_referrals is null then 0 else 1 end other_referrals,
        case when psychosocial_support is null then 0 else 1 end psychosocial_support,
        case when none_ is null then 0 else 1 end NO_Referal
        --select  
        from
        
        (
        select 
        patient_art_number, 
        patient_guid,
        overview_interaction_time InteractionDate,
        Null consented_to_hbc,
        [investigation_and_referrals_referrals_adherence_counselling]   adherence_counselling,
        [investigation_and_referrals_referrals_community_health_worker]  community_health_worker,
        [investigation_and_referrals_referrals_family_planning] family_planning,
        [investigation_and_referrals_referrals_none] none_,
        [investigation_and_referrals_referrals_other] other,
        [investigation_and_referrals_referrals_treatment_preparation] treatment_preparation,
        [investigation_and_referrals_referrals_inpatient_care] inpatient_care,
        [investigation_and_referrals_referrals_inpatient_care_this_facility] inpatient_care_this_facility,
        [investigation_and_referrals_referrals_nutritional_support] nutritional_support,
        [investigation_and_referrals_referrals_tb_treatment_dot_program] tb_treatment_dot_program,
        [investigation_and_referrals_referrals_ca_cx] ca_cx,
        [investigation_and_referrals_inpatient_care] referrals_inpatient_care,
        [investigation_and_referrals_other_referrals] other_referrals,
        null psychosocial_support
        from [dbo].[Art45_ClinicalFollowUpInteraction1]
        union
        SELECT 
        patient_art_number
        ,patient_guid
        ,overview_interaction_time InteractionDate
        ,Null consented_to_hbc
        ,[investigations_and_referrals_referrals_adherence_counselling] adherence_counselling
        ,[investigations_and_referrals_referrals_community_health_worker] community_health_worker
        ,[investigations_and_referrals_referrals_family_planning] family_planning
        ,[investigations_and_referrals_referrals_none] none_
        ,[investigations_and_referrals_referrals_other] other
        ,[investigations_and_referrals_referrals_treatment_preparation] treatment_preparation
        ,[investigations_and_referrals_referrals_inpatient_care] inpatient_care
        ,[investigations_and_referrals_referrals_inpatient_care_this_facility] inpatient_care_this_facility
        ,[investigations_and_referrals_referrals_nutritional_support] nutritional_support
        ,[investigations_and_referrals_referrals_tb_treatment_dot_program] tb_treatment_dot_program
        ,[investigations_and_referrals_referrals_ca_cx] ca_cx
        ,[investigations_and_referrals_inpatient_care] referrals_inpatient_care
        ,[investigations_and_referrals_other_referral] other_referral
        ,null psychosocial_support
        FROM [SmartCareReports].[dbo].[Art45_ArvEligibilityInteraction1]
        union
        SELECT 
            patient_art_number
        ,[patient_guid]
        ,overview_interaction_time InteractionDate
        ,[investigation_and_referrals_referrals_consented_to_hbc] consented_to_hbc
        ,[investigation_and_referrals_referrals_adherence_counselling] adherence_counselling
        ,[investigation_and_referrals_referrals_community_health_worker] community_health_worker
        ,[investigation_and_referrals_referrals_family_planning] family_planning
        ,[investigation_and_referrals_referrals_none] none_
        ,[investigation_and_referrals_referrals_other]  other
        ,[investigation_and_referrals_referrals_treatment_preparation] treatment_preparation
        ,[investigation_and_referrals_referrals_inpatient_care] inpatient_care
        ,[investigation_and_referrals_referrals_inpatient_care_this_facility] inpatient_care_this_facility
        ,[investigation_and_referrals_referrals_nutritional_support] nutritional_support
        ,[investigation_and_referrals_referrals_tb_treatment_dot_program] tb_treatment_dot_program
        ,[investigation_and_referrals_referrals_ca_cx] ca_cx
        ,[investigation_and_referrals_inpatient_care] referrals_inpatient_care
        ,[investigation_and_referrals_other_referrals] other_referral
        ,null psychosocial_support
        FROM [SmartCareReports].[dbo].[Art45_InitialHistoryAndPhysicalInteraction1]
        union
        SELECT 
            patient_art_number
        ,[patient_guid]
        ,overview_interaction_time InteractionDate
        ,[investigation_and_referrals_referrals_consented_to_hbc] referrals_consented_to_hbc
        ,[investigation_and_referrals_referrals_adherence_counseling] adherence_counseling
        ,[investigation_and_referrals_referrals_community_health_worker] community_health_worker
        ,[investigation_and_referrals_referrals_family_planning] family_planning
        ,[investigation_and_referrals_referrals_none] none_
        ,[investigation_and_referrals_referrals_other] other
        ,[investigation_and_referrals_referrals_treatment_preparation] treatment_preparation
        ,[investigation_and_referrals_referrals_inpatient_care] inpatient_care
        ,[investigation_and_referrals_referrals_inpatient_care_this_facility] inpatient_care_this_facility
        ,[investigation_and_referrals_referrals_nutritional_support] nutritional_support
        ,[investigation_and_referrals_referrals_tb_treatment_dot_program] tb_treatment_dot_program
        ,null ca_cx
        ,null referrals_inpatient_care
        ,null other_referral
        ,[investigation_and_referrals_referrals_psychosocial_support] psychosocial_support
        FROM [SmartCareReports].[dbo].[Art45_PaedsInitialHistoryAndPhysicalInteraction1]
        union 
        SELECT                 
        [patient_art_number]
        ,[patient_guid]
        ,overview_interaction_time InteractionDate
        ,[investigation_and_referrals_referrals_consented_to_hbc] referrals_consented_to_hbc
        ,[investigation_and_referrals_referrals_adherence_counseling] adherence_counseling
        ,[investigation_and_referrals_referrals_community_health_worker] community_health_worker
        ,[investigation_and_referrals_referrals_family_planning] family_planning
        ,[investigation_and_referrals_referrals_none] none_
        ,[investigation_and_referrals_referrals_other] other
        ,[investigation_and_referrals_referrals_treatment_preparation] treatment_preparation
        ,[investigation_and_referrals_referrals_inpatient_care] inpatient_care
        ,[investigation_and_referrals_referrals_inpatient_care_this_facility] care_this_facility
        ,[investigation_and_referrals_referrals_nutritional_support] nutritional_support
        ,null ca_cx
        ,[investigation_and_referrals_referrals_tb_treatment_dot_program] treatment_dot_program
        ,[investigation_and_referrals_inpatient_care] referrals_inpatient_care
        ,[investigation_and_referrals_other_referrals]  other_referrals
        ,[investigation_and_referrals_referrals_psychosocial_support] psychosocial_support
        FROM [SmartCareReports].[dbo].[PaedsClinicalFollowUpInteraction2]
        union
        SELECT  
        [patient_art_number]
        ,[patient_guid]
        ,overview_interaction_time InteractionDate
        ,[investigation_and_referrals_referrals_consented_to_hbc] referrals_consented_to_hbc
        ,[investigation_and_referrals_referrals_adherence_counseling] adherence_counseling
        ,[investigation_and_referrals_referrals_community_health_worker] community_health_worker
        ,[investigation_and_referrals_referrals_family_planning] family_planning
        ,[investigation_and_referrals_referrals_none] none_
        ,[investigation_and_referrals_referrals_other] other
        ,[investigation_and_referrals_referrals_treatment_preparation] treatment_preparation
        ,[investigation_and_referrals_referrals_inpatient_care] inpatient_care
        ,[investigation_and_referrals_referrals_inpatient_care_this_facility] inpatient_care_this_facility
        ,[investigation_and_referrals_referrals_nutritional_support] nutritional_support
        ,null ca_cx
        ,[investigation_and_referrals_referrals_tb_treatment_dot_program] tb_treatment_dot_program
        ,[investigation_and_referrals_inpatient_care] referrals_inpatient_care
        ,[investigation_and_referrals_other_referrals] other_referrals
        ,[investigation_and_referrals_referrals_psychosocial_support] psychosocial_support
        FROM [SmartCareReports].[dbo].[PaedsInitialHistoryAndPhysicalInteraction2]
        ) x 
        join crtRegistrationInteraction r on x.patient_guid = r.patientId_int
        join GuidMap g on r.patientId_int = g.NaturalNumber
        left join [Address] a on g.MappedGuid = a.PatientGUID and AddressType = 'Current'
        where InteractionDate between @startdate and @enddate) z
        join [ClientViralLoadTestResult] vl on z.patientId_int = vl.PatientId
        join ( SELECT [PatientId]
            ,max(InteractionDate) latestVlDate
            FROM [ClientViralLoadTestResult]
            Group by PatientId
        ) vl1
        ON vl.[PatientId] = vl1.[PatientId] and vl.InteractionDate = vl1.latestVlDate
        join (SELECT Distinct w.PatientId,t.LatestDate,w.WhoStageToday
        FROM [cdc_fdb_db].[dbo].[ClientHivWhoStage] w
        --where w.WhoStageToday = 1 OR w.WhoStageToday = 2
        join (
        SELECT  PatientId,
                Max([VisitDate]) as LatestDate
            FROM [cdc_fdb_db].[dbo].[ClientHivWhoStage] 
            --WHERE [PatientId] = '1787049'
            GROUP BY  PatientId)
            
            t on  t.LatestDate = w.VisitDate and t.PatientId = w.PatientId
            where w.WhoStageToday = 1 OR w.WhoStageToday = 2
            group by w.PatientId,t.LatestDate,w.WhoStageToday
        ) f
        on f.PatientId = patientId_int
        order by z.InteractionDate DESC          
    """)
    result = engine.execute(sql)
    rows = [dict(row) for row in result.fetchall()]
    return rows