from typing import List, Optional

from sqlalchemy import text
from sqlalchemy.engine.base import Engine

def get_appointments_count(engine: Engine, start_date: str, end_date: str) -> Optional[List[dict]]:
    """
    Returns a count of appointments for the given date range.
    
    :param engine: SQLAlchemy database engine object
    :param start_date: Start date of the query
    :param end_date: End date of the query
    :return: List of dictonaries
    """
    sql = text(
        f"""
        DECLARE @StartDate DATE
    DECLARE @EndDate DATE
    DECLARE @ProvinceId VARCHAR(2)
    DECLARE @DistrictId VARCHAR(3)
    DECLARE @FacilityId VARCHAR(9)
    DECLARE @LTFUThreshold INTEGER

     --SET @StartDate = '2020-01-01'
 --SET @EndDate = '2020-07-01'

SET @StartDate = '{start_date}'
SET @EndDate = '{end_date}'
SET @ProvinceId = (select Value from Setting where Name = 'ProvinceId')
SET @DistrictId = (select Value from Setting where Name = 'DistrictId')
SET @FacilityId = (select Value from Setting where Name = 'FacilityHmisCode')
    SET @LTFUThreshold = 30
    SET NOCOUNT ON



	IF (object_id('tempdb..#tempTodayList')) IS NOT NULL
        DROP TABLE #tempTodayList;
    CREATE TABLE #tempTodayList
    (
        PatientId           INT PRIMARY KEY,
        FirstName           VARCHAR(48),
        SurName             VARCHAR(48),
        PatientGUID         VARCHAR(36),
        NextClinicalVisit   DATE,
        -- Sex                 VARCHAR(3),
        DueForClinicalVisit VARCHAR(3) DEFAULT 'No',
        LastClinicalVisit   DATE,
        NextPharmacyVisit   DATE,
        DueForPharmacyVisit VARCHAR(3) DEFAULT 'No',
        LastPharmacyVisit   DATE,
        DaysLateClinical    INT,
        DaysLatePharmacy    INT,
        OnARVs              INT        DEFAULT 0,
        ArtID               VARCHAR(MAX),
        facilityName        VARCHAR(MAX),
        ProvinceNames       VARCHAR(MAX),
        DistrictNames       VARCHAR(MAX),
        StartDate           DATE,
        EndDate             DATE,
		VLDueToday          VARCHAR(3) DEFAULT 'No'
    );

     SET @StartDate = DATEADD(dd, DATEDIFF(dd, 0, @EndDate), 0);-- Important

    IF (object_id('tempdb..#HmisCodes')) IS NOT NULL
        DROP TABLE #HmisCodes
    CREATE TABLE #HmisCodes
    (
        SubSiteId INT
    )

    INSERT into #HmisCodes
    SELECT SubSiteId
    from fn_GetFacilitySubSiteIds(@ProvinceId, @DistrictId, @FacilityId) 
    declare
        @facility_name varchar(100)
    if (@FacilityId IS NULL OR @FacilityId = '')
        Set @facility_name = 'All'
    else
        set @facility_name = (select dbo.fn_GetFacilityNames(@ProvinceId, @DistrictId, @FacilityId, ', '))
            
    declare
        @ProvinceNames varchar(50)
    if (@ProvinceId IS NULL OR @ProvinceId = '')
        set @ProvinceNames = 'All'
    else
        set @ProvinceNames = (select dbo.fn_GetProvinceNames(@ProvinceId, @DistrictId, @FacilityId, ', '))
            
    declare
        @DistrictNames varchar(50)
    if (@DistrictId IS NULL OR @DistrictId = '')
        set @DistrictNames = 'All'
    else
        set @DistrictNames = (select dbo.fn_GetDistrictNames(@ProvinceId, @DistrictId, @FacilityId, ', '))
            
-- Any client whos latest Clinical or Pharmacy Visit is due in the period
    INSERT INTO #tempTodayList(PatientId)
    SELECT DISTINCT z.PatientId
    FROM (
             SELECT PatientId, MAX(NextVisitDate) NextVisitDate
             FROM ClientClinicalCareDates
			 WHERE ArtOrNonArtClinicalCare = 1 
             GROUP BY PatientId
             UNION
             SELECT PatientId, MAX(DATEADD(day, duration, InteractionDate)) NextAppointmentDate
             FROM ClientArvsDate
             GROUP BY PatientId
         ) z
    WHERE z.NextVisitDate BETWEEN @StartDate AND @EndDate;
    
    UPDATE c
    SET c.FirstName   = r.FirstName,
        c.SurName     = r.SurName,
        c.PatientGUID = r.PatientId
        -- c.Sex = r.Sex
    FROM #tempTodayList C
             JOIN crtRegistrationInteraction r
                  ON c.PatientId = r.patientId_int;
    
    UPDATE c
    SET c.OnARVs = 1
    FROM #tempTodayList c
             JOIN ClientHivSummary chs
                  ON c.PatientId = chs.PatientId
    WHERE chs.ArtStartDate IS NOT NULL;
    
    UPDATE c
    SET c.ArtID = i.ArtNumber
    FROM #tempTodayList c
             JOIN crtIHAP i
                  ON c.PatientId = i.PatientId;
    
    UPDATE c
    SET c.LastClinicalVisit = z.InteractionDate
    FROM #tempTodayList c
             JOIN (
        SELECT PatientId, MAX(InteractionDate) InteractionDate
        FROM ClientClinicalCareDates
        WHERE PatientId IN (SELECT PatientId FROM #tempTodayList)
          AND InteractionDate <= @StartDate
        GROUP BY PatientId
    ) z ON z.PatientId = c.PatientId;
    
    UPDATE c
    SET c.LastPharmacyVisit = z.InteractionDate
    FROM #tempTodayList c
             JOIN (
        SELECT PatientId, MAX(InteractionDate) InteractionDate
        FROM ClientArvsDate
        WHERE PatientId IN (SELECT PatientId FROM #tempTodayList)
          AND InteractionDate <= @StartDate
        GROUP BY PatientId
    ) z ON z.PatientId = c.PatientId;
    
    UPDATE c
    SET c.NextClinicalVisit = z.NextVisitDate
    FROM #tempTodayList c
             JOIN (
        SELECT cccd.PatientId, MAX(cccd.NextVisitDate) NextVisitDate
        FROM #tempTodayList ttdl
                 JOIN ClientClinicalCareDates cccd
                      ON ttdl.PatientId = cccd.PatientId
        GROUP BY cccd.PatientId
    ) z ON z.PatientId = c.PatientId;
    
    UPDATE c
    SET c.NextPharmacyVisit = z.NextAppointmentDate
    FROM #tempTodayList c
             JOIN (
        SELECT cccd.PatientId, MAX(cccd.NextAppointmentDate) NextAppointmentDate
        FROM #tempTodayList ttdl
                 JOIN ClientArvsDate cccd
                      ON ttdl.PatientId = cccd.PatientId
        GROUP BY cccd.PatientId
    ) z ON z.PatientId = c.PatientId;
    
    UPDATE c
    SET c.DaysLatePharmacy = (CASE
                                  WHEN c.NextPharmacyVisit > @EndDate THEN 0
                                  ELSE DATEDIFF(DD, c.NextPharmacyVisit, @EndDate)
        END)
    FROM #tempTodayList c
    WHERE c.NextPharmacyVisit IS NOT NULL;
    
    UPDATE c
    SET c.DaysLateClinical = (CASE
                                  WHEN c.NextClinicalVisit > @EndDate THEN NULL
                                  ELSE DATEDIFF(DD, c.NextClinicalVisit, @EndDate)
        END)
    FROM #tempTodayList c
    WHERE c.NextClinicalVisit IS NOT NULL;
    
    UPDATE c
    SET c.DueForPharmacyVisit = 'Yes'
    FROM #tempTodayList c
    WHERE c.NextPharmacyVisit BETWEEN @StartDate AND @EndDate;
    
    UPDATE c
    SET c.DueForClinicalVisit = 'Yes'
    FROM #tempTodayList c
    WHERE c.NextClinicalVisit BETWEEN @StartDate AND @EndDate;

	--add vl yes if client is due today

	 -- ============================================================================================
  -- intercept viral load stuff here on #tempClinicalLatePatientList

		--ALTER TABLE #tempClinicalLatePatientList
		--ADD DueForViralLoad VARCHAR(1);
		--ALTER TABLE #tempClinicalLatePatientList
		--ADD ScheduledViralLoadTestDate DATETIME;
		--ALTER TABLE #tempClinicalLatePatientList
		--ADD LateForViralLoad VARCHAR(MAX);

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
		FROM #tempTodayList;
		
		-- Set LastTestOrEarliestHivPositiveDate based on latest VL test, also set LastViralLoadResult
		UPDATE vl
		SET vl.LastTestOrEarliestHivPositiveDate = labs.InteractionDate,
			vl.LastViralLoadResult               = labs.LabTestValue
		FROM #ViralLoadDetails vl
				JOIN (
		SELECT PatientId,
				InteractionDate,
				LabTestValue,
				(ROW_NUMBER()
					OVER (PARTITION BY PatientId
						ORDER BY InteractionDate DESC)) AS seq
		FROM ClientViralLoadTestResult
		WHERE PatientId IN (SELECT vl.PatientId FROM #ViralLoadDetails vl) -- early filters are faster
		) labs ON labs.PatientId = vl.PatientId
		WHERE labs.seq = 1;

		-- Then set LastTestOrEarliestHivPositiveDate based on HIV Positive Date (OldestHIVPositive, IHAPDate, ARTStartDate or CareStartDate) for those with no LastViralLoadResult
		-- Also set YearsOnART using (ArtStartDate,IhapDate, CareStartDate), exclude OldestHivPosTestDate as treatment not guaranteed
		UPDATE vl
		SET vl.LastTestOrEarliestHivPositiveDate = COALESCE(vl.LastTestOrEarliestHivPositiveDate, chs.OldestHivPosTestDate,
															chs.ArtStartDate, chs.IhapDate,
															chs.CareStartDate), -- dont overrwite existing LastTestOrEarliestHivPositiveDate if present
			vl.YearsOnART                        = DATEDIFF(YY, COALESCE(chs.ArtStartDate, chs.IhapDate, chs.CareStartDate),
															@EndDate)
		FROM #ViralLoadDetails vl
				JOIN ClientHivSummary chs
					ON chs.PatientId = vl.PatientId;

		-- Calculate DueDate for clients with a valid LastViralLoadResult
		UPDATE vl
		SET vl.DueDate = (CASE
							WHEN (CONVERT(DECIMAL, vl.LastViralLoadResult) <= 1000 AND vl.YearsOnArt >= 1)
							THEN DATEADD(MONTH, 12, vl.LastTestOrEarliestHivPositiveDate) -- on art for over a year and supressed, come back in 12 months
							WHEN (CONVERT(DECIMAL, vl.LastViralLoadResult) <= 1000 AND vl.YearsOnArt = 0)
							THEN DATEADD(MONTH, 6, vl.LastTestOrEarliestHivPositiveDate) -- on art for less than a year and suppressed, come back in 6 months
							WHEN (CONVERT(DECIMAL, vl.LastViralLoadResult) > 1000)
							THEN DATEADD(MONTH, 3, vl.LastTestOrEarliestHivPositiveDate) -- unsupressed, come back in 6 months
		END)
		FROM #ViralLoadDetails vl
		WHERE vl.LastViralLoadResult IS NOT NULL
		and vl.LastTestOrEarliestHivPositiveDate IS NOT NULL
		AND dbo.fn_IsSCNumeric(vl.LastViralLoadResult) = 1;

		-- Calculate DueDate for clients without a LastViralLoadResult but with a LastTestOrEarliestHivPositiveDate
		UPDATE vl
		SET vl.DueDate = DATEADD(MM, 3, vl.LastTestOrEarliestHivPositiveDate)
		FROM #ViralLoadDetails vl
		WHERE vl.LastTestOrEarliestHivPositiveDate IS NOT NULL
		AND vl.DueDate IS NULL;

		-- calculate DaysLate using DueDate where DueDate < EndDate
		UPDATE vl
		SET vl.DaysLate = DATEDIFF(DD, vl.DueDate, @EndDate)
		FROM #ViralLoadDetails vl
		WHERE vl.DueDate < @EndDate;

		-- Finally update the output table
		UPDATE c
		SET c.VLDueToday            = (CASE
											WHEN (vl.DueDate <= @EndDate AND vl.DaysLate >= 0)
												THEN 'Yes' -- Due when DaysLate is 0 [on same day] or greater
											WHEN ((vl.DueDate <= @EndDate AND vl.DueDate < GETDATE()) OR vl.DueDate > @EndDate)
												THEN 'No'
		END)

		FROM #tempTodayList c JOIN #ViralLoadDetails vl ON c.PatientId = vl.PatientId;
  -- ============================================================================================



-- Final touches
    UPDATE c
    SET c.facilityName  = @facility_name,
        c.ProvinceNames = @ProvinceNames,
        c.DistrictNames = @DistrictNames,
        c.StartDate     = @StartDate,
        c.EndDate       = @EndDate
    FROM #tempTodayList c;
    
    --================================
    --Section added by Trevor to ensure that date range is returned if no row is inserted in #tempTodayList in previous insert statement
    declare
        @number_of_rows as int
    set @number_of_rows = 1
    set @number_of_rows = (select count(*)
                           from #tempTodayList)
    if (@number_of_rows = 0)
        INSERT INTO #tempTodayList
        select 0                       as PatientId,
               null                    as FirstName,
               null                    as SurName,
               null                    as PatientGUID,
            --    null                     as Sex,
               null                    as NextClinicalVisit,
               null                    as DueForClinicalVisit,
               null                    as LastClinicalVisit,
               null                    as NextPharmacyVisit,
               null                    as DueForPharmacyVisit,
               null                    as LastPharmacyVisit,
               null                    as DaysLateClinical,
               null                    as DaysLatePharmacy,
               null                    as OnARVs,
               null                    as ArtID,
               (select @facility_name) as facilityName,
               (select @ProvinceNames) as ProvinceNames,
               (select @DistrictNames) as DistrictNames,
               (select @StartDate)     as StartDate,
               (select @EndDate)       as EndDate,
			   null                    as VLDueToday

			   
	IF (object_id('tempdb..#currentAddresses')) IS NOT NULL
        DROP TABLE #currentAddresses;
	select distinct PatientGuid, PhoneNumber, MobilePhoneNumber 
	into #currentAddresses
	from Address 
	where AddressType = 'Current'


	IF (object_id('tempdb..#allDue')) IS NOT NULL
        DROP TABLE #allDue;

    SELECT FirstName,
           SurName,
        --    Sex,
		   PatientId,
           PatientGUID,
           NextClinicalVisit,
           DueForClinicalVisit,
           LastClinicalVisit,
           NextPharmacyVisit,
           DueForPharmacyVisit,
           LastPharmacyVisit,
           DaysLateClinical,
           DaysLatePharmacy,
           Case OnARVs
               When 1
                   then 'Yes'
               When 0
                   then 'No'
               End       as OnARVs,
           ArtID,
           facilityName,
           ProvinceNames AS Province,
           DistrictNames AS District,
           StartDate,
           EndDate,
		   VLDueToday,
		   case when (DueForClinicalVisit = 'Yes' and DueForPharmacyVisit = 'Yes') then 'CV + PV' 
		   when   (DueForClinicalVisit = 'Yes' and DueForPharmacyVisit = 'No') then 'CV'
		   when (DueForClinicalVisit = 'No' and DueForPharmacyVisit = 'Yes') then 'PV'
		   when (DueForClinicalVisit = 'No' and DueForPharmacyVisit = 'No') then 'None'
		   else '' end as visitDue
		   INTO #allDue
    FROM #tempTodayList

-- SELECT 
-- z.FirstName,
-- z.SurName,
-- z.ArtNumber,
-- z.PhoneNumber,
-- z.Sex,
-- z.Age,
-- z.DueForPharmacyVisit,
-- z.DueForClinicalVisit,
-- z.LastClinicalVisit,
-- z.ArtStartDate,
-- z.NextClinicalVisit,
-- z.VLDueToday,
-- z.LastViralLoadResult,
-- z.OnARVs,
-- FROM 
-- 	(
        select distinct
        al.FirstName + ' ' + al.SurName as ClientName,  
	al.ArtID ArtNumber, 
    r.Sex,
	al.DueForPharmacyVisit,
	al.DueForClinicalVisit,
	case 
	when (ad.PhoneNumber is not null  and ad.PhoneNumber <> '') then ad.PhoneNumber
	when (ad.PhoneNumber is not null and ad.PhoneNumber = '' and ad.MobilePhoneNumber is not null) then ad.MobilePhoneNumber 
	else '' end as PhoneNumber,
	al.OnARVs, 
    CONVERT(varchar, al.LastClinicalVisit, 103) as LastClinicalVisit,
    CONVERT(varchar, al.NextClinicalVisit, 103) as NextClinicalVisit,
    CONVERT(varchar, h.ArtStartDate, 103) as ArtStartDate,
    Age = dbo.fn_Age(r.DateOfBirth, @EndDate),
    al.NextPharmacyVisit,
case
    when (st.PatientMadeInactive = 1) then 'Inactive'
    when (st.PatientMadeInactive = 0) then 'Active'
    when (st.PatientMadeInactive IS NULL) then 'Active'
end as ClientStatus,
	al.VLDueToday,
	vl.LastViralLoadResult,
    al.PatientId
	from #allDue al
	join GuidMap mp on al.PatientId = mp.NaturalNumber 
	left join #currentAddresses ad on ad.PatientGUID = mp.MappedGuid
	left JOIN #ViralLoadDetails vl ON vl.PatientId = al.PatientId 
    join crtRegistrationInteraction r on mp.NaturalNumber = r.patientId_int
    join ClientHivSummary h on al.PatientId = h.PatientId
    left join crtPatientStatus st on al.PatientId = st.PatientId
    full outer join (select PatientId, MAX(VisitDate) as LastVisit from crtPatientStatus group by PatientId) st1
    on st.PatientId = st1.PatientId AND st.VisitDate = st1.LastVisit
	where 
	(al.DueForPharmacyVisit = 'Yes' 
	and DaysLatePharmacy = 0
	) 
	OR (al.DueForClinicalVisit = 'Yes' 
	and DaysLateClinical = 0
	) 
    --AND al.NextClinicalVisit BETWEEN @StartDate AND @EndDate
    --AND vl.LastViralLoadResult IS NOT NULL     
""")
    result = engine.execute(sql)
    rows = [dict(row) for row in result.fetchall()]
    return rows