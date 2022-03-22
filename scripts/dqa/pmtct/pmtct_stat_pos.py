from typing import List, Optional

from sqlalchemy import text
from sqlalchemy.engine.base import Engine

def get_pmtc_stat_pos(engine: Engine, start_date: str, end_date: str) -> Optional[List[dict]]:
	"""
    Returns a list of pmtc_stat_pos data
    :param engine: SQLAlchemy database engine object
    :param start_date: Start date of the query
    :param end_date: End date of the query
    :return: List of dictonaries
    """        
	sql = text(
	f"""
	--USE [cdc_fdb_db]
    --GO


	--USE [cdc_fdb_db]
    --GO


	DECLARE @StartDate DATE
	DECLARE @EndDate DATE
	DECLARE @ProvinceId VARCHAR(2)
	DECLARE @DistrictId VARCHAR(3)
	DECLARE @FacilityId VARCHAR(9)
	DECLARE @LTFUThreshold INTEGER

	SET NOCOUNT ON
	SET @StartDate = '{start_date}'
	SET @EndDate = '{end_date}'
	SET @ProvinceId = (select Value from Setting where Name = 'ProvinceId')
	SET @DistrictId = (select Value from Setting where Name = 'DistrictId')
	SET @FacilityId = (select Value from Setting where Name = 'FacilityHmisCode')
	SET @LTFUThreshold = 30


		--From facilities, get and filter only the current facility in the current province and district
	IF (OBJECT_ID('tempdb..#CurrentFacility')) IS NOT NULL
	DROP TABLE #CurrentFacility -- Renamed from facility filter
	SELECT DISTINCT SubSiteId
					INTO #CurrentFacility
	FROM dbo.fn_GetFacilitySubSiteIds(@ProvinceId, @DistrictId, @FacilityId)

	-- Step 1: throw all PrimaryAndSecondary guids into temp table
	IF (object_id('tempdb..#staging')) is not null
	DROP TABLE #staging;

	CREATE TABLE #staging
	(
		PatientId            INT,
		Nupin				 VARCHAR(32),
		Phone				 VARCHAR(32),
		VisitDate            DATETIME,
		Age					 INT,	
		IndexName            VARCHAR(MAX),
		Sex                  VARCHAR(1),
		EntryPoint           VARCHAR(MAX),
		HIVTestDate          DATETIME,
		HIVResult            VARCHAR(MAX),
		DateEnrolled         DATETIME,
		ARTNumber            VARCHAR(MAX),
		DateInitiated        DATETIME,
		IndexAddress		 VARCHAR(MAX)
	);



	INSERT INTO #staging(PatientId,Sex,VisitDate)
	SELECT PatientId,
			chs.Sex,
		chs.LastClinicalVisit
	FROM ClientHivSummary chs
		
	WHERE chs.LastClinicalVisit >= @StartDate
	AND chs.LastClinicalVisit <= @EndDate    -- seen/registered in the period
	AND (chs.ArtStartDate IS NOT NULL --is HIV positive by ART
	OR chs.IhapDate IS NOT NULL --is HIV positive by enrollment
	OR chs.CareStartDate IS NOT NULL --is HIV positive by HIV Care
	OR chs.OldestHivPosTestDate IS NOT NULL) --is HIV positive by test
	



	CREATE INDEX IDX_Staging_PatientId
	ON #staging (PatientId);

	-- Step 2: remove duplicate entries via key-pair binding. Order by patient ID so that whomever was registered first takes precedence
	DELETE x
	FROM (SELECT *, rn=row_number() OVER (PARTITION BY PatientId ORDER BY PatientId) FROM #staging) x
	WHERE rn > 1;

	-- Step 4: Join with ClientHivSummary tables to get index name
	UPDATE c
	SET c.IndexName   = reg.FirstName + ' ' + reg.SurName,
		c.Nupin = reg.PatientID,
		c.Age    = dbo.fn_Age(reg.DateOfBirth, @EndDate),
		c.Sex         = reg.Sex
	FROM #staging c
		JOIN crtRegistrationInteraction reg ON reg.patientId_int = c.PatientId;


	-- Update the Index entry point from Adult CBS first
	UPDATE c
	SET c.EntryPoint = (CASE
						WHEN (adultCbs.EntryPoint_OPD = 1) THEN 'OPD'
						WHEN (adultCbs.EntryPoint_IPD = 1) THEN 'IPD'
						WHEN (adultCbs.EntryPoint_MCH = 1) THEN 'MCH'
						WHEN (adultCbs.EntryPoint_VMMC = 1) THEN 'VMMC'
						WHEN (adultCbs.EntryPoint_HTS = 1) THEN 'HTS'
						WHEN (adultCbs.EntryPoint_TBClinic = 1) THEN 'TB Clinic'
						WHEN (adultCbs.EntryPoint_STIsClinic = 1) THEN 'STI Clinic'
						WHEN (adultCbs.EntryPoint_Community = 1) THEN 'Community'
						WHEN (adultCbs.EntryPoint_IndexTesting = 1) THEN 'Index Testing'
						WHEN (adultCbs.EntryPoint_TransitionFromPaediatrics = 1) THEN 'Transition from Paed'
						ELSE NULL END)
	FROM #staging c
		JOIN crtCaseBasedSurveillanceAdults adultCbs ON adultCbs.PatientId = c.PatientId;

	-- Update the Index entry point from Paed CBS second, only where "EntryPoint" values are null
	UPDATE c
	SET c.EntryPoint = (CASE
						WHEN (paedCbs.EntryPoint = '0') THEN 'OPD'
						WHEN (paedCbs.EntryPoint = '1') THEN 'IPD'
						WHEN (paedCbs.EntryPoint = '2') THEN 'MCH'
						WHEN (paedCbs.EntryPoint = '3') THEN 'VMMC'
						WHEN (paedCbs.EntryPoint = '4') THEN 'HTC'
						WHEN (paedCbs.EntryPoint = '5') THEN 'TB Clinic'
						WHEN (paedCbs.EntryPoint = '6') THEN 'STI Clinic'
						WHEN (paedCbs.EntryPoint = '7') THEN 'Community'
						WHEN (paedCbs.EntryPoint = '8') THEN 'Index Testing'
						WHEN (paedCbs.EntryPoint = '9') THEN 'Other'
						ELSE paedCbs.EntryPointOther
	END)
	FROM #staging c
		JOIN crtCaseBasedSurveillancePaeds paedCbs ON paedCbs.PatientId = c.PatientId
	WHERE c.EntryPoint IS NULL;

	-- Update the HIV Data
	UPDATE c
	SET c.HIVTestDate   = chs.OldestHivPosTestDate,
		c.DateEnrolled  = chs.IhapDate,
		c.DateInitiated = COALESCE(chs.ArtStartDate, chs.CareStartDate)
	FROM #staging c
		JOIN ClientHivSummary chs
				ON (chs.PatientId = c.PatientId) OR (chs.PatientId = c.PatientId) ;

	-- Update the Contact IHAP Number
	UPDATE c
	SET c.ArtNumber = ihap.ArtNumber
	FROM #staging c
		JOIN crtIHAP ihap
				ON (ihap.PatientId = c.PatientId) OR (ihap.PatientId = c.PatientId);

	-- Update the Contact HIV test result
	UPDATE c
	SET c.HIVResult = (CASE
						WHEN (x.HivTestResult = 0) THEN 'Indeterminate'
						WHEN (x.HivTestResult = 1) THEN 'Positive'
						WHEN (x.HivTestResult = 2) THEN 'Negative'
						ELSE 'Unknown'
	END)
	FROM #staging c
		JOIN (SELECT *, rn=row_number() OVER (PARTITION BY PatientId ORDER BY HIVTestDate DESC)
				FROM ClientHivTestResult) x
				ON (x.PatientId = c.PatientId) OR (x.PatientId = c.PatientId)
	WHERE x.rn = 1;


	-- Step 12.1: Update INDEX address
	UPDATE c
	SET  c.IndexAddress = address.StreetName + ' ' + address.CommunityName + ' ' + address.DirectionsToAddressFromClinic
	FROM #staging c
		Join GuidMap g on g.NaturalNumber = c.PatientId and g.MappedGuid = g.OwningGuid
			JOIN Address address ON (address.PatientGuid = g.MappedGuid)
	WHERE address.AddressType = 'Current';
	
	UPDATE c
	SET c.Phone = (case 
		when (address.PhoneNumber is not null  and address.PhoneNumber <> '') then address.PhoneNumber
		when (address.PhoneNumber is not null and address.PhoneNumber = '' and address.MobilePhoneNumber is not null and address.MobilePhoneNumber <> '') then address.MobilePhoneNumber
		else 'Unknown'
		END)
	FROM #staging c
		Join GuidMap g on g.NaturalNumber = c.PatientId and g.MappedGuid = g.OwningGuid
			JOIN Address address ON (address.PatientGuid = g.MappedGuid)
	WHERE address.AddressType = 'Current';

	select Nupin,
			ARTNumber,
			CONVERT(varchar, VisitDate, 103) as VisitDate,
			IndexName,
			ISNULL(Phone, 'Unknown') as Phone,
			IndexAddress,
			Age,
			Sex,
			EntryPoint,
			CONVERT(varchar, HIVTestDate, 103) as HIVTestDate,
			HIVResult,
			ISNULL(CONVERT(varchar, DateEnrolled, 103), 'Unknown') as DateEnrolled,
			ISNULL(CONVERT(varchar, DateInitiated, 103), 'Unknown') as DateInitiated,
			EntryPoint
	from #staging 
	WHERE #staging.HIVResult = 'Positive' 
	AND #staging.EntryPoint IS NOT NULL
	AND Age <= 15
	order by Nupin 



	 """)

	result = engine.execute(sql)
	rows = [dict(row) for row in result.fetchall()]
	return rows 