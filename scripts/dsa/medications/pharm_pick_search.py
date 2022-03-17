from typing import List, Optional

from sqlalchemy import text
from sqlalchemy.engine.base import Engine

def get_pharm_pick_search_list(engine: Engine, start_date: str, end_date: str, art_number: str) -> Optional[List[dict]]:
    """
    Returns a list of appointment pharm picks that match search criteria.

    :param engine: SQLAlchemy database engine object
    :param start_date: Start date of the query
    :param end_date: End date of the query
    :return: List of dictonaries
    """      
    
    sql = text(
    f"""
    DECLARE @ProvinceId as varchar(2)
    DECLARE @DistrictId as varchar(3)
    DECLARE @FacilityId as varchar(9)
    DECLARE @StartDate as datetime
    DECLARE @EndDate as datetime
    DECLARE @sender varchar(max)
    DECLARE @YearToday INT = YEAR(GETDATE())

    SET @StartDate = '{start_date}'
    SET @EndDate = '{end_date}'
    SET @ProvinceId = (select Value from Setting where Name = 'ProvinceId')
    SET @DistrictId = (select Value from Setting where Name = 'DistrictId')
    SET @FacilityId = (select Value from Setting where Name = 'FacilityHmisCode')
    set @sender = ''


    if (object_id('tempdb..#facilityFilter')) is not null
    drop table #facilityFilter
    select distinct SubSiteId
                    into #facilityFilter
    from dbo.fn_GetFacilitySubSiteIds(@ProvinceId, @DistrictId, @FacilityId)

    if (object_id('tempdb..#tempClinicalLatePatientList')) is not null
    drop table #tempClinicalLatePatientList
    CREATE TABLE #tempClinicalLatePatientList
    (
        PatientId                              int,
        [category]                             [varchar](1)     NULL,
        [ArtID]                                [varchar](100)   NULL,
        [SurName]                              [varchar](100)   NULL,
        [FirstName]                            [varchar](100)   NULL,
        [Sex]                                  [varchar](1)     NULL,
        [Age]                                  [int]            NULL,
        [PhoneNumber]                          [varchar](50)    NULL,
        [MobilePhoneNumber]                    [varchar](50)    NULL,
        [HousePlotNumber]                      [varchar](50)    NULL,
        [StreetName]                           [varchar](255)   NULL,
        [POBox]                                [varchar](50)    NULL,
        [OnARVs]                               [varchar](10)    NULL,
        [DaysLatePharmacy]                     [int]            NULL,
        [DaysLateClinical]                     [int]            NULL,
        [NextClinicalVisit]                    [datetime]       NULL,
        [LastClinicalVisit]                    [datetime]       NULL,
        [NextPharmacyVisit]                    [datetime]       NULL,
        [LastPharmacyVisit]                    [datetime]       NULL,
        [LastCD4Count]                         [decimal](18, 0) NULL,
        [facility]                             [varchar](4000)  NULL,
        [loginName]                            [varchar](50)    NULL,
        [ClinicalLastWeek]                     [decimal](18, 0) NULL,
        [ClinicalTwoWeeksAgo]                  [decimal](18, 0) NULL,
        [ScheduledClinicalThisWeek]            [decimal](18, 0) NULL,
        [ScheduledClinicalNextWeek]            [decimal](18, 0) NULL,
        [PharmacyLastWeek]                     [decimal](18, 0) NULL,
        [PharmacyTwoWeeksAgo]                  [decimal](18, 0) NULL,
        [ScheduledPharmacyThisWeek]            [decimal](18, 0) NULL,
        [ScheduledPharmacyNextWeek]            [decimal](18, 0) NULL,
        [EnrolledOnArtActive]                  [decimal](18, 0) NULL,
        [ARTActive_1_30_Days]                  [decimal](18, 0) NULL,
        [ARTActive_31_60_Days]                 [decimal](18, 0) NULL,
        [ARTActive_61_90_Days]                 [decimal](18, 0) NULL,
        [ARTActive_91_180_Days]                [decimal](18, 0) NULL,
        [ARTActive_Greater180_Days]            [decimal](18, 0) NULL,
        [EnrolledNonArtActive]                 [decimal](18, 0) NULL,
        [NonARTActive_1_30_Days]               [decimal](18, 0) NULL,
        [NonARTActive_31_60_Days]              [decimal](18, 0) NULL,
        [NonARTActive_61_90_Days]              [decimal](18, 0) NULL,
        [NonARTActive_91_180_Days]             [decimal](18, 0) NULL,
        [NonARTActive_Greater180_Days]         [decimal](18, 0) NULL,
        [ARTActive_1_30_Days_Percent]          [decimal](18, 2) NULL,
        [ARTActive_31_60_Days_Percent]         [decimal](18, 2) NULL,
        [ARTActive_61_90_Days_Percent]         [decimal](18, 2) NULL,
        [ARTActive_91_180_Days_Percent]        [decimal](18, 2) NULL,
        [ARTActive_Greater180_Days_Percent]    [decimal](18, 2) NULL,
        [NonARTActive_1_30_Days_Percent]       [decimal](18, 2) NULL,
        [NonARTActive_31_60_Days_Percent]      [decimal](18, 2) NULL,
        [NonARTActive_61_90_Days_Percent]      [decimal](18, 2) NULL,
        [NonARTActive_91_180_Days_Percent]     [decimal](18, 2) NULL,
        [NonARTActive_Greater180_Days_Percent] [decimal](18, 2) NULL,
        [Province]                             [varchar](50)    NULL,
        [District]                             [varchar](50)    NULL,
        [EndDate]                              [datetime]       NULL,
        NotActive                              bit
    );

    -- Add indexing for speed
    CREATE NONCLUSTERED INDEX tempClinicalLatePatientList_PatientId_Idx ON #tempClinicalLatePatientList (PatientId);

    --we add the aggregate row
    insert into #tempClinicalLatePatientList (category)
    values (0)

    --******************************************************************
    --#  Get patients of Interest 
    --# clients who have an IHAP interaction and came for an ART service
    if (object_id('tempdb..#clientsOfInterest')) is not null
        drop table #clientsOfInterest
    select distinct c.PatientId
                    into #clientsOfInterest
    from ClientClinicalCareDates c
            join ClientHivSummary s on c.PatientId = s.PatientId
    where EditLocation in (select SubSiteId
                            from #facilityFilter)
        and InteractionDate <= @endDate
        and Sex in ('M', 'F')
        and IhapDate is not null
        and IhapDate <= @EndDate
    --and has a pharmacy visit, or maybe not

    --# Get patient next appointment date based on how many drugs have been dispensed
    if (object_id('tempdb..#nextAppointmentByDrugsDispensed')) is not null
        drop table #nextAppointmentByDrugsDispensed
    select PatientId,
            InteractionDate,
            DATEADD(day, duration, InteractionDate) AS NextAppointmentDateByDispensation
            into #nextAppointmentByDrugsDispensed
            from ClientArvsDate
            where Duration IS NOT NULL
            order by InteractionDate desc
        --select * from #nextAppointmentByDrugsDispensed where PatientId = '202291' order by NextAppointmentDateByDispensation desc

    if (object_id('tempdb..#nextPharmacyVisit')) is not null
        drop table #nextPharmacyVisit
    select PatientId,
            VisitDate,
            NextAppointmentDateByDispensation
            into #nextPharmacyVisit
    from (
            select PatientId,
                    VisitDate,
                    NextAppointmentDateByDispensation,
                    row_number()
                        over ( partition by PatientId
                        order by VisitDate desc, NextAppointmentDateByDispensation desc ) sn
            from (
                    select distinct p.PatientId,
                                    DATEADD(dd, DATEDIFF(dd, 0, InteractionDate), 0)           VisitDate,
                                    DATEADD(day, duration, InteractionDate) NextAppointmentDateByDispensation
                    from ClientArvsDate dp
                            join crtPharmacyVisit p on dp.PatientId = p.PatientId
                        join #clientsOfInterest c on p.PatientId = c.PatientId
                        join crctPharmacyDispensation d
                                on p.InteractionId = d.InteractionId
                        join DrugproductsCachedView drug on d.PhysicalDrugid = drug.MedDrugid
                    where GenericName = 'sulfamethoxazole+trimethoprim'
                    or DrugClass = 'Antiretrovirals'
                    and DATEADD(dd, DATEDIFF(dd, 0, VisitDate), 0) <=
                        @EndDate
                ) a
        ) b
    where sn = 1
    update #nextPharmacyVisit
    set NextAppointmentDateByDispensation = VisitDate
    where (NextAppointmentDateByDispensation < VisitDate or NextAppointmentDateByDispensation is null)
    --#  select * from #nextPharmacyVisit

  --Remove all clients that do not have a pharmacy visit with arv or cotrimoxazole
  delete
  from #clientsOfInterest
  where PatientId not in (
    select distinct PatientId
    from #nextPharmacyVisit)
  --#  select * from #clientsOfInterest

  if object_id('tempdb..#artNumbers') is not null
    drop table #artNumbers
  select PatientId,
         ArtNumber
         into #artNumbers
  from (
         SELECT i.PatientId,
                ArtNumber,
                ROW_NUMBER()
                    Over ( Partition by i.PatientId
                      order by VisitDate desc ) as OrderSeq
         FROM dbo.crtIhap i
                join #clientsOfInterest c on i.PatientId = c.patientId
         WHERE ArtNumber is not null
           and VisitDate <= @EndDate
       ) b
  where OrderSeq = 1
  --#  select * from #artNumbers

  if (object_id('tempdb..#ageAsAtEndDate')) is not null
    drop table #ageAsAtEndDate
  select s.PatientId,
         1                                    category,
         FirstName,
         SurName,
         case 
            when (a.PhoneNumber is not null  and a.PhoneNumber <> '') then a.PhoneNumber
            when (a.PhoneNumber is not null and a.PhoneNumber = '' and a.MobilePhoneNumber is not null and a.MobilePhoneNumber <> '') then a.MobilePhoneNumber 
          else 'Unknown' end as PhoneNumber,
         a.HousePlotNumber,
         a.StreetName,
         a.POBox,
         ArtStartDate,
         case
           when ArtStartDate is not null and ArtStartDate <= @EndDate
             then 'Yes'
           else 'No' end                      OnArvs,
         '****-***-******-*****-***-******-*' ArtNumber
         into #ageAsAtEndDate
  from ClientHivSummary s
         join #clientsOfInterest c on s.PatientId = c.patientId
         join GuidMap g on g.NaturalNumber = s.PatientId and g.MappedGuid = g.OwningGuid
         left join
       (
         select PatientGUID,MobilePhoneNumber,HousePlotNumber,StreetName,PhoneNumber,POBox
         from (
                select row_number() over ( partition by PatientGUID order by PatientGUID desc) sn,*
                from [Address]
                where AddressType = 'Current'
              ) x
         where x.sn = 1
       ) a on a.PatientGUID = g.OwningGuid
         join (
    select PatientId,
           firstname,
           surname
    from (
           select patientid_int                                 PatientId,
                  upper(FirstName)                              FirstName,
                  upper(SurName)                                SurName,
                  row_number()
                      over ( partition by patientid_int
                        order by SurName desc, FirstName desc ) sn
           from crtRegistrationInteraction
         ) reg
    where sn = 1
  ) regDetails on s.patientId = regDetails.PatientId
  where IhapDate is not null
    and IhapDate <= @EndDate
  update a
  set a.ArtNumber = case
                      when ihap.ArtNumber is null
                        then ''
                      else ihap.ArtNumber end
  from #ageAsAtEndDate a
         left join #artNumbers ihap on a.patientId = ihap.PatientId
  --#  select * from #ageAsAtEndDate where ArtNumber is not null

  if (object_id('tempdb..#mostRecentVisitAtFacility')) is not null
    drop table #mostRecentVisitAtFacility
  select c.PatientId,
         MAX(InteractionDate) InteractionDate
         into #mostRecentVisitAtFacility
  from ClientClinicalCareDates c
         join #clientsOfInterest i on c.PatientId = i.PatientId
  where InteractionDate <= @EndDate
    and EditLocation in (select SubSiteId
                         from #facilityFilter)
  group by c.PatientId
  --#  select * from #mostRecentVisitAtFacility

  if (object_id('tempdb..#dead')) is not null
    drop table #dead
  select PatientId,
         VisitDate,
         'Death' DiscontinuationReason
         into #dead
  from fn_GetPatientDeadDateAsOf(@EndDate, default)
  --#  select * from #dead

  if (object_id('tempdb..#discontinued')) is not null
    drop table #discontinued
  select PatientId,
         DATEADD(dd, DATEDIFF(dd, 0, VisitDate), 0) VisitDate,
         DiscontinuationReason
         into #discontinued
  from (
         select PatientId,
                VisitDate,
                ROW_NUMBER()
                    over ( partition by patientid
                      order by VisitDate desc ) seq,
                (case
                   when (PatientMadeInactive = 1 and (PatientTransferOut is null or PatientTransferOut = 0))
                     then 'InActive'
                   when (PatientTransferOut = 1)
                     then 'TO'
                   when (PatientMadeInactive = 1 or PatientTransferOut = 1)
                     then 'InAtive/TO'
                  end)
                                                DiscontinuationReason
         from crtPatientStatus
         where (PatientMadeInactive = 1 or PatientTransferOut = 1)
           and DATEADD(dd, DATEDIFF(dd, 0, VisitDate), 0) <= @EndDate
           and EditLocation in (select SubSiteId
                                from #facilityFilter)
       ) g
  where seq = 1;

   -- Identify trans outs via new passerby functionality
    INSERT INTO #Discontinued(PatientId, VisitDate, DiscontinuationReason)
    SELECT ceo.PatientIdInt, DATEADD(dd, DATEDIFF(dd, 0, cmd.VisitDate), 0) VisitDate, 'PasserBy'
    FROM crtElmisOverview ceo
             JOIN crtMedicationsDispensed cmd
                  ON ceo.InteractionIdInt = cmd.InteractionID
    WHERE ceo.ArtPasserBy = 1 -- Indexed column for speed
      AND (ceo.EditLocationId IN (SELECT SubSiteId FROM #facilityFilter));
    -- only exclude trans-out at Facility Level -- @AggregationLevel = 'Facility';

  delete
  from d
  from #discontinued d
         join #mostRecentVisitAtFacility c
              on d.PatientId = c.PatientId
  where d.VisitDate < c.InteractionDate
  --#  select * from #discontinued

  alter table #ageAsAtEndDate
    add DeadBeforeEnd bit
  alter table #ageAsAtEndDate
    add NotActive bit
  alter table #ageAsAtEndDate
    add DeadDate datetime
  alter table #ageAsAtEndDate
    add DiscontinuationDateBeforeEnd datetime
  alter table #ageAsAtEndDate
    add MostRecentVisit datetime

  --#  select * From #mostRecentVisitAtFacility
  update c
  set MostRecentVisit = InteractionDate
  from #ageAsAtEndDate c
         join #mostRecentVisitAtFacility v
              on c.PatientId = v.PatientId
  update c
  set c.DeadBeforeEnd = 1,
      DeadDate        = VisitDate,
      NotActive       = 1
  from #ageAsAtEndDate c
         join #dead d --has patients who are made inactive or transfered out
              on c.PatientId = d.PatientId
  --#  select * From #ageAsAtEndDate where DeadBeforeEnd = 1
  --#  select * From #dead

  update c
  set c.NotActive                  = 1,
      DiscontinuationDateBeforeEnd = VisitDate
  from #ageAsAtEndDate c
         join #discontinued d --has patients who are made inactive or transfered out
              on c.PatientId = d.PatientId
  --#  select * From #ageAsAtEndDate where NotActive = 1 

  --LastClinicalVisit
  if (object_id('tempdb..#lastClinicalVisit')) is not null
    drop table #lastClinicalVisit
  select PatientId,
         max(InteractionDate) InteractionDate
         into #lastClinicalVisit
  from ClientClinicalCareDates
  where VisitType in (315, 55, 27, 44, 50, 24, 47, 30, 41, 42, 59, 39, 53, 48, 40, 54, 28,
                      317, 327, 328, 339, 333, 330, 347, 337, 350, 353,
                      344 --all art45 interactions except patient status
    )
    and InteractionDate is not null
    and InteractionDate <= @EndDate
  group by PatientId
  --# select * from #lastClinicalVisit where PatientId = 600741
  --600741

  alter table #ageAsAtEndDate
    add MostRecentClinicalVisit datetime
  update #ageAsAtEndDate
  set MostRecentClinicalVisit = InteractionDate
  from #ageAsAtEndDate c
         join #lastClinicalVisit d
              on c.PatientId = d.PatientId
  --#  select * from #ageAsAtEndDate

  --NextClinicalVisit
  alter table #ageAsAtEndDate
    add NextClinicalVisit datetime, DaysLateClinical int
  update #ageAsAtEndDate
  set NextClinicalVisit = DATEADD(dd, DATEDIFF(dd, 0, b.NextAppointmentDateByDispensation), 0)
      --DaysLateClinical =  case when datediff(D,NextAppointmentDate,@EndDate) < 0 then 0 else datediff(D,NextAppointmentDate,@EndDate)end
  from #ageAsAtEndDate a
         inner join
       (
          select cv.PatientId,
                DATEADD(day, duration, InteractionDate) AS NextAppointmentDateByDispensation,
                ROW_NUMBER()
                    Over ( Partition by cv.PatientId
                      order by VisitDate desc,  DATEADD(day, duration, InteractionDate) desc ) as OrderSeq
         from crtClinicalVisit cv
		 JOIN ClientArvsDate  cad on cv.PatientId = cad.PatientId
         where VisitType in (315, 55, 27, 44, 50, 24, 47, 30, 41, 42, 59, 39, 53, 48, 40, 54, 28,
                             317, 327, 328, 339, 333, 330, 347, 337, 350, 353,
                             344 --all art45 interactions except patient status
           )
           and DATEADD(day, duration, InteractionDate) is not null
           --get rid of erroneous entries, we only accept values no further than 12 months
           and DATEADD(day, duration, InteractionDate) <= DATEADD(mm, 12, VisitDate)
           and DATEADD(day, duration, InteractionDate) > VisitDate
           and VisitDate <= @EndDate
       ) b
       on a.PatientId = b.PatientId
  where OrderSeq = 1
  --# select * from #lastClinicalVisit where PatientId = 600741

  update #ageAsAtEndDate
  set NextClinicalVisit = MostRecentClinicalVisit
  where (NextClinicalVisit < MostRecentClinicalVisit or NextClinicalVisit is null)
  update #ageAsAtEndDate
  set DaysLateClinical = case
                           when datediff(D, NextClinicalVisit, @EndDate) < 0
                             then 0
                           else datediff(D, NextClinicalVisit, @EndDate) end


  --#  select * from #ageAsAtEndDate

  alter table #ageAsAtEndDate
    add NextPharmacyVisit datetime, DaysLatePharmacy int
  alter table #ageAsAtEndDate
    add LastPharmacyVisit datetime
  update c
  set NextPharmacyVisit = n.NextAppointmentDateByDispensation,
      LastPharmacyVisit = VisitDate,
      DaysLatePharmacy  = case
                            when datediff(D, n.NextAppointmentDateByDispensation, @EndDate) < 0
                              then 0
                            else datediff(D, n.NextAppointmentDateByDispensation, @EndDate) end
  from #ageAsAtEndDate c
         join
       #nextPharmacyVisit n on c.PatientId = n.PatientId
	   join
	   #nextAppointmentByDrugsDispensed dp on c.PatientId = dp.PatientId
  --#  select * from #ageAsAtEndDate

  insert into #tempClinicalLatePatientList (PatientId,
                                            category,
                                            ArtID,
                                            SurName,
                                            FirstName,
                                            PhoneNumber,
                                            HousePlotNumber,
                                            StreetName,
                                            POBox,
                                            OnARVs,
                                            NextClinicalVisit,
                                            LastClinicalVisit,
                                            NextPharmacyVisit,
                                            LastPharmacyVisit,
                                            DaysLatePharmacy,
                                            DaysLateClinical,
                                            NotActive)
  select PatientId,
         Category,
         ArtNumber,
         SurName,
         Firstname,
         PhoneNumber,
         HousePlotNumber,
         StreetName,
         POBox,
         OnArvs,
         NextClinicalVisit,
         MostRecentClinicalVisit,
         NextPharmacyVisit,
         LastPharmacyVisit,
         DaysLatePharmacy,
         DaysLateClinical,
         NotActive
  from #ageAsAtEndDate
  update #tempClinicalLatePatientList
  set ClinicalLastWeek = 1
  where datediff(W, LastClinicalVisit, @EndDate) = 1
  update #tempClinicalLatePatientList
  set PharmacyLastWeek = 1
  where datediff(W, LastPharmacyVisit, @EndDate) = 1
  update #tempClinicalLatePatientList
  set ClinicalTwoWeeksAgo = 1
  where datediff(W, LastClinicalVisit, @EndDate) = 2
  update #tempClinicalLatePatientList
  set PharmacyTwoWeeksAgo = 1
  where datediff(W, LastPharmacyVisit, @EndDate) = 2
  update #tempClinicalLatePatientList
  set ScheduledClinicalThisWeek = 1
  where datediff(W, NextClinicalVisit, @EndDate) = 0
  update #tempClinicalLatePatientList
  set ScheduledPharmacyThisWeek = 1
  where datediff(W, NextPharmacyVisit, @EndDate) = 0
  update #tempClinicalLatePatientList
  set ScheduledClinicalNextWeek = 1
  where datediff(W, NextClinicalVisit, @EndDate) = -1
  update #tempClinicalLatePatientList
  set ScheduledPharmacyNextWeek = 1
  where datediff(W, NextPharmacyVisit, @EndDate) = -1
  --#  select * from #tempClinicalLatePatientList

  
  if object_id('tempdb..#currentCD4Counts') is not null
    drop table #currentCD4Counts
  select PatientId,
         CD4Count,
         InteractionDate
         into #currentCD4Counts
  from (
         select a.PatientId,
                InteractionDate,
                LabTestValue                                   CD4Count,
                ROW_NUMBER()
                    over (partition by a.PatientId
                      order by InteractionDate desc ) Seq
         from ClientCd4CountTestResult a
                join #clientsOfInterest c on a.PatientId = c.PatientId
         where InteractionDate <= @EndDate
       ) b
  where Seq = 1;
  CREATE INDEX Indx_CurrentCD4Counts_PatientId
    ON #currentCD4Counts (PatientId);
  --#  select * From #currentCD4Counts

  --Last CD4 Count
  update #tempClinicalLatePatientList
  set LastCD4Count = b.Cd4Count
  from #tempClinicalLatePatientList a
         join #currentCD4Counts b
              on a.PatientId = b.PatientId
  --#  select * from #tempClinicalLatePatientList 

  update #tempClinicalLatePatientList
  set ClinicalLastWeek =
        (select count(*)
         from #tempClinicalLatePatientList
         where ClinicalLastWeek = 1
           and category = 1)
  where Category = 0
  update #tempClinicalLatePatientList
  set ClinicalTwoWeeksAgo =
        (select count(*)
         from #tempClinicalLatePatientList
         where ClinicalTwoWeeksAgo = 1
           and category = 1)
  where category = 0
  update #tempClinicalLatePatientList
  set ScheduledClinicalThisWeek =
        (select count(*)
         from #tempClinicalLatePatientList
         where ScheduledClinicalThisWeek = 1
           and category = 1)
  where category = 0
  update #tempClinicalLatePatientList
  set ScheduledClinicalNextWeek =
        (select count(*)
         from #tempClinicalLatePatientList
         where ScheduledClinicalNextWeek = 1
           and Category = 1)
  where category = 0
  update #tempClinicalLatePatientList
  set PharmacyLastWeek =
        (select count(*)
         from #tempClinicalLatePatientList
         where PharmacyLastWeek = 1
           and category = 1)
  where category = 0
  update #tempClinicalLatePatientList
  set PharmacyTwoWeeksAgo =
        (select count(*)
         from #tempClinicalLatePatientList
         where PharmacyTwoWeeksAgo = 1
           and Category = 1)
  where category = 0
  update #tempClinicalLatePatientList
  set ScheduledPharmacyThisWeek =
        (select count(*)
         from #tempClinicalLatePatientList
         where ScheduledPharmacyThisWeek = 1
           and Category = 1)
  where category = 0
  update #tempClinicalLatePatientList
  set ScheduledPharmacyNextWeek =
        (select count(*)
         from #tempClinicalLatePatientList
         where ScheduledPharmacyNextWeek = 1
           and Category = 1)
  where category = 0

  --# select * from #tempClinicalLatePatientList where notActive = 1

  update #tempClinicalLatePatientList
  set EnrolledOnArtActive = 1
  where OnArvs = 'Yes'
    and NotActive is null
    and category = 1
  update #tempClinicalLatePatientList
  set EnrolledOnArtActive = (
    select count(*)
    from #tempClinicalLatePatientList
    where EnrolledOnArtActive = 1
  )
  where category = 0
  update #tempClinicalLatePatientList
  set OnARVs = 'No'
  where OnARVs is null
    and category = 1


  --#  select * From #tempClinicalLatePatientList

  update #tempClinicalLatePatientList
  set ARTActive_1_30_Days       = (case
                                     when DaysLatePharmacy between 1 and 30
                                       then 1
                                     else null end),
      ARTActive_31_60_Days      = (case
                                     when DaysLatePharmacy between 31 and 60
                                       then 1
                                     else null end),
      ARTActive_61_90_Days      = (case
                                     when DaysLatePharmacy between 61 and 90
                                       then 1
                                     else null end),
      ARTActive_91_180_Days     = (case
                                     when DaysLatePharmacy between 91 and 180
                                       then 1
                                     else null end),
      ARTActive_Greater180_Days = (case
                                     when DaysLatePharmacy > 180
                                       then 1
                                     else null end)
  where category = 1
    and NotActive is null
    and OnArvs = 'Yes'

  --update #tempClinicalLatePatientList set EnrolledNonArtActive = 1 where OnArvs = 'Yes' and NotActive is null and category = 1
  update #tempClinicalLatePatientList
  set EnrolledNonArtActive = 1
  where OnArvs = 'No'
    and NotActive is null
    and category = 1
  update #tempClinicalLatePatientList
  set EnrolledNonArtActive = (
    select count(*)
    from #tempClinicalLatePatientList
    where EnrolledNonArtActive = 1
  )
  where category = 0
  update #tempClinicalLatePatientList
  set NonARTActive_1_30_Days       = (case
                                        when DaysLatePharmacy between 1 and 30
                                          then 1
                                        else null end),
      NonARTActive_31_60_Days      = (case
                                        when DaysLatePharmacy between 31 and 60
                                          then 1
                                        else null end),
      NonARTActive_61_90_Days      = (case
                                        when DaysLatePharmacy between 61 and 90
                                          then 1
                                        else null end),
      NonARTActive_91_180_Days     = (case
                                        when DaysLatePharmacy between 91 and 180
                                          then 1
                                        else null end),
      NonARTActive_Greater180_Days = (case
                                        when DaysLatePharmacy > 180
                                          then 1
                                        else null end)
  where category = 1
    and NotActive is null
    and OnArvs = 'No'
  --# select * from #tempClinicalLatePatientList

  --Art and Active
  update #tempClinicalLatePatientList
  set ARTActive_1_30_Days = (
    select count(*)
    from #tempClinicalLatePatientList
    where ARTActive_1_30_Days = 1
      and category = 1)
  where category = 0
  update #tempClinicalLatePatientList
  set ARTActive_31_60_Days = (
    select count(*)
    from #tempClinicalLatePatientList
    where ARTActive_31_60_Days = 1
      and category = 1)
  where category = 0
  update #tempClinicalLatePatientList
  set ARTActive_61_90_Days = (
    select count(*)
    from #tempClinicalLatePatientList
    where ARTActive_61_90_Days = 1
      and category = 1)
  where category = 0
  update #tempClinicalLatePatientList
  set ARTActive_91_180_Days = (
    select count(*)
    from #tempClinicalLatePatientList
    where ARTActive_91_180_Days = 1
      and category = 1)
  where category = 0
  update #tempClinicalLatePatientList
  set ARTActive_Greater180_Days = (
    select count(*)
    from #tempClinicalLatePatientList
    where ARTActive_Greater180_Days = 1
      and category = 1)
  where category = 0


  --Non art but active
  update #tempClinicalLatePatientList
  set NonARTActive_1_30_Days = (
    select count(*)
    from #tempClinicalLatePatientList
    where NonARTActive_1_30_Days = 1
      and category = 1)
  where category = 0
  update #tempClinicalLatePatientList
  set NonARTActive_31_60_Days = (
    select count(*)
    from #tempClinicalLatePatientList
    where NonARTActive_31_60_Days = 1
      and category = 1)
  where category = 0
  update #tempClinicalLatePatientList
  set NonARTActive_61_90_Days = (
    select count(*)
    from #tempClinicalLatePatientList
    where NonARTActive_61_90_Days = 1
      and category = 1)
  where category = 0
  update #tempClinicalLatePatientList
  set NonARTActive_91_180_Days = (
    select count(*)
    from #tempClinicalLatePatientList
    where NonARTActive_91_180_Days = 1
      and category = 1)
  where category = 0
  update #tempClinicalLatePatientList
  set NonARTActive_Greater180_Days = (
    select count(*)
    from #tempClinicalLatePatientList
    where NonARTActive_Greater180_Days = 1
      and category = 1)
  where category = 0
  --# select * from #tempClinicalLatePatientList

  DECLARE @activeAndEnrolled int, @activeAndnotEnrolled int
  set @activeAndnotEnrolled = (select EnrolledNonArtActive
                               from #tempClinicalLatePatientList
                               where category = 0)
  set @activeAndEnrolled = (select EnrolledOnArtActive
                            from #tempClinicalLatePatientList
                            where category = 0)
  update #tempClinicalLatePatientList
  set NonARTActive_1_30_Days_Percent  =
    (case
           when NonARTActive_1_30_Days is not null and @activeAndnotEnrolled > 0
             then (NonARTActive_1_30_Days * 100.0) / @activeAndnotEnrolled
           else 0 end),
      NonARTActive_31_60_Days_Percent      = (case
                                                when NonARTActive_31_60_Days is not null and @activeAndnotEnrolled > 0
                                                  then (NonARTActive_31_60_Days * 100.0) / @activeAndnotEnrolled
                                                else 0 end),
      NonARTActive_61_90_Days_Percent      = (case
                                                when NonARTActive_61_90_Days is not null and @activeAndnotEnrolled > 0
                                                  then (NonARTActive_61_90_Days * 100.0) / @activeAndnotEnrolled
                                                else 0 end),
      NonARTActive_91_180_Days_Percent     = (case
                                                when NonARTActive_91_180_Days is not null and @activeAndnotEnrolled > 0
                                                  then (NonARTActive_91_180_Days * 100.0) / @activeAndnotEnrolled
                                                else 0 end),
      NonARTActive_Greater180_Days_Percent = (case
                                                when NonARTActive_Greater180_Days is not null and
                                                     @activeAndnotEnrolled > 0
                                                  then (NonARTActive_Greater180_Days * 100.0) / @activeAndnotEnrolled
                                                else 0 end),

      ARTActive_1_30_Days_Percent          =
        (case
           when ARTActive_1_30_Days is not null and @activeAndEnrolled > 0
             then (ARTActive_1_30_Days * 100.0) / @activeAndEnrolled
           else 0 end),
      ARTActive_31_60_Days_Percent         = (case
                                                when ARTActive_31_60_Days is not null and @activeAndEnrolled > 0
                                                  then (ARTActive_31_60_Days * 100.0) / @activeAndEnrolled
                                                else 0 end),
      ARTActive_61_90_Days_Percent         = (case
                                                when ARTActive_61_90_Days is not null and @activeAndEnrolled > 0
                                                  then (ARTActive_61_90_Days * 100.0) / @activeAndEnrolled
                                                else 0 end),
      ARTActive_91_180_Days_Percent        = (case
                                                when ARTActive_91_180_Days is not null and @activeAndEnrolled > 0
                                                  then (ARTActive_91_180_Days * 100.0) / @activeAndEnrolled
                                                else 0 end),
      ARTActive_Greater180_Days_Percent    = (case
                                                when ARTActive_Greater180_Days is not null and @activeAndEnrolled > 0
                                                  then (ARTActive_Greater180_Days * 100.0) / @activeAndEnrolled
                                                else 0 end)
  where Category = 0
  --#  select * From #tempClinicalLatePatientList
  update #tempClinicalLatePatientList
  set province = (select top 1 name
                  from province
                  where Code = @ProvinceId)
  update #tempClinicalLatePatientList
  set district = (select top 1 name
                  from district
                  where districtseq = @DistrictId)
  update #tempClinicalLatePatientList
  set EndDate = @EndDate

  -- ============================================================================================
  -- intercept viral load stuff here on #tempClinicalLatePatientList

  ALTER TABLE #tempClinicalLatePatientList
    ADD DueForViralLoad VARCHAR(1);
  ALTER TABLE #tempClinicalLatePatientList
    ADD ScheduledViralLoadTestDate DATETIME;
  ALTER TABLE #tempClinicalLatePatientList
    ADD LateForViralLoad VARCHAR(MAX);

  IF (OBJECT_ID('tempdb..#ViralLoadDetails')) IS NOT NULL
    DROP TABLE #ViralLoadDetails;
  CREATE TABLE #ViralLoadDetails
  (
    PatientId                         INT,
    LastTestOrEarliestHivPositiveDate DATE,
    LastViralLoadResult               VARCHAR(MAX),
    YearsOnART                        INT,
    DueDate                           DATE,
    DaysLate                          INT
  );

  -- Get patient IDs
  INSERT INTO #ViralLoadDetails(PatientId)
  SELECT PatientId
  FROM #tempClinicalLatePatientList;

  -- Set LastTestOrEarliestHivPositiveDate based on latest VL test, also set LastViralLoadResult
  UPDATE c
  SET c.LastTestOrEarliestHivPositiveDate = labs.InteractionDate,
      c.LastViralLoadResult               = labs.LabTestValue
  FROM #ViralLoadDetails c
         JOIN (
    SELECT PatientId,
           InteractionDate,
           LabTestValue,
           (ROW_NUMBER()
               OVER (PARTITION BY PatientId
                 ORDER BY InteractionDate DESC)) AS seq
    FROM ClientViralLoadTestResult
	WHERE PatientId IN (SELECT c.PatientId FROM #ViralLoadDetails c) -- early filters are faster
  ) labs ON labs.PatientId = c.PatientId
  WHERE labs.seq = 1;

  -- Then set LastTestOrEarliestHivPositiveDate based on HIV Positive Date (OldestHIVPositive, IHAPDate, ARTStartDate or CareStartDate) for those with no LastViralLoadResult
  -- Also set YearsOnART using (ArtStartDate,IhapDate, CareStartDate), exclude OldestHivPosTestDate as treatment not guaranteed
  UPDATE c
  SET c.LastTestOrEarliestHivPositiveDate = COALESCE(c.LastTestOrEarliestHivPositiveDate, chs.OldestHivPosTestDate,
                                                     chs.ArtStartDate, chs.IhapDate,
                                                     chs.CareStartDate), -- dont overrwite existing LastTestOrEarliestHivPositiveDate if present
      c.YearsOnART                        = DATEDIFF(YY, COALESCE(chs.ArtStartDate, chs.IhapDate, chs.CareStartDate),
                                                     @EndDate)
  FROM #ViralLoadDetails c
         JOIN ClientHivSummary chs
              ON chs.PatientId = c.PatientId;

  -- Calculate DueDate for clients with a valid LastViralLoadResult
  UPDATE c
  SET c.DueDate = (CASE
                     WHEN (CONVERT(DECIMAL, c.LastViralLoadResult) <= 1000 AND c.YearsOnArt >= 1)
                       THEN DATEADD(MONTH, 12, c.LastTestOrEarliestHivPositiveDate) -- on art for over a year and supressed, come back in 12 months
                     WHEN (CONVERT(DECIMAL, c.LastViralLoadResult) <= 1000 AND c.YearsOnArt = 0)
                       THEN DATEADD(MONTH, 6, c.LastTestOrEarliestHivPositiveDate) -- on art for less than a year and suppressed, come back in 6 months
                     WHEN (CONVERT(DECIMAL, c.LastViralLoadResult) > 1000)
                       THEN DATEADD(MONTH, 3, c.LastTestOrEarliestHivPositiveDate) -- unsupressed, come back in 6 months
    END)
  FROM #ViralLoadDetails c
  WHERE c.LastViralLoadResult IS NOT NULL
    and c.LastTestOrEarliestHivPositiveDate IS NOT NULL
    AND dbo.fn_IsSCNumeric(c.LastViralLoadResult) = 1;

  -- Calculate DueDate for clients without a LastViralLoadResult but with a LastTestOrEarliestHivPositiveDate
  UPDATE c
  SET c.DueDate = DATEADD(MM, 3, c.LastTestOrEarliestHivPositiveDate)
  FROM #ViralLoadDetails c
  WHERE c.LastTestOrEarliestHivPositiveDate IS NOT NULL
    AND c.DueDate IS NULL;

  -- calculate DaysLate using DueDate where DueDate < EndDate
  UPDATE c
  SET c.DaysLate = DATEDIFF(DD, c.DueDate, @EndDate)
  FROM #ViralLoadDetails c
  WHERE c.DueDate < @EndDate;

  -- Finally update the output table
  UPDATE o
  SET o.DueForViralLoad            = (CASE
                                        WHEN (c.DueDate <= @EndDate AND c.DaysLate >= 0)
                                          THEN 'Y' -- Due when DaysLate is 0 [on same day] or greater
                                        WHEN ((c.DueDate <= @EndDate AND c.DueDate < GETDATE()) OR c.DueDate > @EndDate)
                                          THEN 'N'
    END),
      o.ScheduledViralLoadTestDate = c.DueDate,
      o.LateForViralLoad = (CASE
                            WHEN (c.DueDate <= @EndDate AND c.DaysLate > 0)
                              THEN CONVERT(VARCHAR, c.DaysLate) + ' day(s)' -- only late when days greater than 0
        END)
  FROM #tempClinicalLatePatientList o
         JOIN #ViralLoadDetails c
              ON o.PatientId = c.PatientId;

  
  select tmp.SurName + ', ' + tmp.FirstName                         'ClientName',
        r.Sex,
        Age = @YearToday - r.BirthYear,
         PhoneNumber                                        'PhoneNumber',
         PObox + ', ' + HousePlotNumber + ', ' + StreetName 'Address',
         ArtID                                              'ArtNumber',
         CONVERT(varchar, LastPharmacyVisit, 103)            'LastPharmacyVisit',
         CONVERT(varchar, NextPharmacyVisit, 103)            'NextPharmacyVisit',
         DaysLatePharmacy                                   'DaysLate',
         OnARVs                                             'OnARVs',
         LastCD4Count                                       'LastCD4Count',
         DueForViralLoad                                    'DueForVL',
         CONVERT(varchar, ScheduledViralLoadTestDate, 103)   'ScheduledTestDate',
         LateForViralLoad                                   'DaysLateVL',
         vl.LastViralLoadResult  'VLCount'
    -- Script Edited above by Prince Musole to make the names returned accessible in a Javascript Object with the dot notation
  from #tempClinicalLatePatientList tmp
  left join #ViralLoadDetails vl on tmp.PatientId = vl.PatientId
  left join crtPatientStatus stat
  left join crtRegistrationInteraction r on r.patientId_int = stat.PatientId
    --added this to remove patients that died/inactivated or transfered out using patient status
                   on tmp.PatientId = stat.PatientId
    where tmp.SurName is not null
    and tmp.FirstName is not null
    and (stat.PatientTransferOut = 0 or stat.PatientTransferOut is null)
    and (stat.PatientMadeInactive = 0 or stat.PatientMadeInactive is null)
    and (stat.PatientDied = 0 or stat.PatientDied is null)
    and NotActive is null -- remove all the dead/transferred out (not active)	 
    and DaysLatePharmacy > 0 
	  and DaysLatePharmacy < 365  -- between 0 and 365 days late 
    and Category = 1
  and ArtID LIKE '%{art_number}%'
    --and (NextPharmacyVisit BETWEEN @StartDate AND @EndDate) 
  order by LastPharmacyVisit desc;
          
    """)
    result = engine.execute(sql)
    rows = [dict(row) for row in result.fetchall()]
    print("Pharm Pick: ", len(rows))
    return rows