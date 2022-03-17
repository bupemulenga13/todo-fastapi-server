from typing import List, Optional

from sqlalchemy import text
from sqlalchemy.engine.base import Engine

def get_tx_new_list(engine: Engine, start_date: str, end_date: str) -> Optional[List[dict]]:
    """
    Returns a list of the TX New indicator.
    
    :param engine: SQLAlchemy engine object
    :param start_date: Start date of the query
    :param end_date: End date of the query
    :return: List of TX New dictonaries
    """

    sql = text(f"""
            DECLARE @StartDate DATE
            DECLARE @EndDate DATE
            DECLARE @ProvinceId VARCHAR(2)
            DECLARE @DistrictId VARCHAR(3)
            DECLARE @FacilityId VARCHAR(9)
            DECLARE @OnArtMode BIT = 1 
            DECLARE @LTFUThreshold INTEGER
        SET @StartDate = (select DATEADD(dd, DATEDIFF(dd, 0, DATEADD(day,-30,getdate())), 0))
        SET @EndDate = (select DATEADD(dd, DATEDIFF(dd, 0, DATEADD(day,30,getdate())), 0))
        SET @ProvinceId = (select Value from Setting where Name = 'ProvinceId')
        SET @DistrictId = (select Value from Setting where Name = 'DistrictId')
        SET @FacilityId = (select Value from Setting where Name = 'FacilityHmisCode')

            

            SET @LTFUThreshold = 30

                --Trancate the seconds off the start AND end date time
            SET @StartDate ='{start_date}' --DATEADD(dd, DATEDIFF(dd, 0, @StartDate), 0)
            SET @EndDate = '{end_date}'--DATEADD(dd, DATEDIFF(dd, 0, @EndDate), 0)
            SET NOCOUNT ON

            IF (object_id('tempdb..#currentAddresses')) IS NOT NULL
            DROP TABLE #currentAddresses;
        select distinct PatientGuid, PhoneNumber, MobilePhoneNumber 
        into #currentAddresses
        from Address 
        where AddressType = 'Current'

            IF OBJECT_ID('tempdb..#Discontinued') IS NOT NULL
                DROP TABLE #Discontinued;
            IF OBJECT_ID('tempdb..#DeadClients') IS NOT NULL
                DROP TABLE #DeadClients;
            IF OBJECT_ID('tempdb..#MostRecentHIVStatusAsOfEndDate') IS NOT NULL
                DROP TABLE #MostRecentHIVStatusAsOfEndDate;
            IF OBJECT_ID('tempdb..#CurrentOnARTByClinicalVisit') IS NOT NULL
                DROP TABLE #CurrentOnARTByClinicalVisit;
            IF OBJECT_ID('tempdb..#CurrentOnARTByDrugs') IS NOT NULL
                DROP TABLE #CurrentOnARTByDrugs;
            IF OBJECT_ID('tempdb..#CurrentOnARTByVisitAndDrugs') IS NOT NULL
                DROP TABLE #CurrentOnARTByVisitAndDrugs;
            IF OBJECT_ID('tempdb..#CurrentOnArt') IS NOT NULL
                DROP TABLE #CurrentOnArt;
            IF (object_id('tempdb..#AllCurrPrEPClients')) IS NOT NULL
                DROP TABLE #AllCurrPrEPClients;

                IF (OBJECT_ID('tempdb..#CurrentFacility')) IS NOT NULL
                DROP TABLE #CurrentFacility -- Renamed FROM facility filter
            SELECT DISTINCT SubSiteId
            INTO #CurrentFacility
            FROM dbo.fn_GetFacilitySubSiteIds(@ProvinceId, @DistrictId, @FacilityId)

                    CREATE TABLE #Discontinued
            (
                PatientId             INT,
                VisitDate             DATE,
                DiscontinuationReason VARCHAR(32)
            );
            CREATE TABLE #DeadClients
            (
                PatientId             INT PRIMARY KEY,
                VisitDate             DATE,
                DiscontinuationReason VARCHAR(32)
            );
            CREATE TABLE #MostRecentHIVStatusAsOfEndDate
            (
                PatientId  INT PRIMARY KEY,
                TestResult INT,
                TestDate   DATE
            );
            CREATE TABLE #CurrentOnARTByClinicalVisit
            (
                PatientId             INT PRIMARY KEY,
                Sex                   VARCHAR(1),
                InteractionDate       DATE,
                DateOfBirth           DATE,
                AgeAsAtEnd            INT,
                AgeArtStart           INT,
                CurrentHivStatus      INT,
                CurrentHivStatusDate  DATE,
                OldestHivPosTestDate  DATE,
                ArtStartDate          DATE,
                ArtStartLocation      VARCHAR(9),
                ArtStartEditLocation  INT,
                CareStartDate         DATETIME,
                CareStartLocation     VARCHAR(9),
                CareStartEditLocation INT
            );
            CREATE TABLE #CurrentOnARTByDrugs
            (
                PatientId       INT PRIMARY KEY,
                InteractionDate DATE
            );
            CREATE TABLE #CurrentOnARTByVisitAndDrugs
            (
                PatientId                    INT PRIMARY KEY,
                InteractionDate              DATE,
                DeadBeforeEnd                BIT,
                NotActive                    BIT,
                DeadDate                     DATE,
                DiscontinuationDateBeforeEnd DATE,
                CurrentHivStatus             INT,
                DateOfBirth                  DATE,
                CareStartDate                DATE,
                ArtStartDate                 DATE,
                OldestHivPosTestDate         DATE,
                ArtStartEditLocation         INT,
                Sex                          VARCHAR(1),
                AgeAsAtEnd                   INT,
                AgeArtStart                  INT
            );
            CREATE TABLE #CurrentOnArt
            (
                PatientId    INT PRIMARY KEY,
                EditLocation INT,
                Age          INT,
                AgeLastVisit INT,
                Sex          VARCHAR(1),
                ArtStartDate DATE
            );
            CREATE TABLE #AllCurrPrEPClients
            (
                PatientId         INT PRIMARY KEY,
                Sex               VARCHAR(1),
                Age               INT,
                InitVisitDate     DATE,
                FollowUpVisitDate DATE
            );

                -- Declare facility aggregation level
            DECLARE
                @AggregationLevel VARCHAR(32);
            SET @AggregationLevel = (SELECT Value FROM Setting WHERE Name = 'AggregationLevel');


                -- Step 1: All recent HIV tests (latest entry per client, sorted by result (1 pos, 2 neg))
            -- This function already returns one unique result per client :)
            INSERT INTO #MostRecentHIVStatusAsOfEndDate (PatientId, TestResult, TestDate)
            SELECT PatientId, TestResult, TestDate
            FROM dbo.fn_ClientHivTestResult_GetMostRecent(@EndDate);

            DELETE
            FROM #MostRecentHIVStatusAsOfEndDate
            WHERE PatientId
                    NOT IN (SELECT DISTINCT PatientId
                            FROM ClientClinicalCareDates
                            WHERE EditLocation IN (SELECT SubSiteId FROM #CurrentFacility));
                                -- This client returns ALL clients in a dataset! Thus we need to filter to clients who were seen at least ONCE at this location (think of national level run)

                                    -- Step 2: Find all on ART by Clinical visit, including latest interaction date and hiv status (one entry per client)
            INSERT INTO #CurrentOnARTByClinicalVisit (PatientId, InteractionDate)
            SELECT c.PatientId,
                c.InteractionDate
            FROM (SELECT c.PatientId,
                        InteractionDate,
                        ROW_NUMBER()
                                OVER (PARTITION BY c.PatientId
                                    ORDER BY InteractionDate DESC) AS seq
                FROM ClientClinicalCareDates c
                WHERE (InteractionDate <= @EndDate AND EditLocation IN (SELECT SubSiteId FROM #CurrentFacility))
                ) c
            WHERE c.seq = 1;

                UPDATE c
            SET c.Sex                   = chs.Sex,
                c.DateOfBirth           = chs.DateOfBirth,
                c.OldestHivPosTestDate  = chs.OldestHivPosTestDate,
                c.ArtStartDate          = chs.ArtStartDate,
                c.ArtStartLocation      = chs.ArtStartLocation,
                c.ArtStartEditLocation  = chs.ArtStartEditLocation,
                c.CareStartDate         = chs.CareStartDate,
                c.CareStartLocation     = chs.CareStartLocation,
                c.CareStartEditLocation = chs.CareStartEditLocation,
                c.AgeArtStart           = dbo.fn_Age(chs.DateOfbirth, chs.ArtStartDate),
                c.AgeAsAtEnd            = dbo.fn_Age(chs.DateOfbirth, @EndDate)
            FROM #CurrentOnARTByClinicalVisit c
                    join ClientHivSummary chs
                        ON c.PatientId = chs.PatientId
                            AND chs.Sex in ('M', 'F');

            UPDATE c
            SET c.CurrentHivStatus    =recentStatus.TestResult,
                c.CurrentHivStatusDate= recentStatus.TestDate
            FROM #CurrentOnARTByClinicalVisit c
                    JOIN #MostRecentHIVStatusAsOfEndDate recentStatus --Get the most recent HIV status if present in temp table of HIV Status info of all clients
                        ON c.PatientId = recentStatus.PatientId;

                            -- Condition 3: On ART by Pharmacy Dispensation (one entry per client)
            INSERT INTO #CurrentOnARTByDrugs(s.PatientId, InteractionDate)
            SELECT s.PatientId, MAX(s.InteractionDate) InteractionDate
            FROM ClientArvsDate s -- Join to identify positive clients only, this is due to Prophylaxis concerns :)
                    JOIN (SELECT PatientId
                        FROM #CurrentOnARTByClinicalVisit
                        UNION
                        SELECT PatientId
                        FROM #MostRecentHIVStatusAsOfEndDate
                        WHERE TestResult = 1
            ) chs
                        ON chs.PatientId = s.PatientId
            WHERE s.InteractionDate <= @EndDate
                AND (
                (@OnArtMode = 1 AND DATEADD(dd, Duration, s.InteractionDate) >=
                                        DATEADD(dd, -@LTFUThreshold, @EndDate))
            OR (@OnArtMode = 0 AND s.InteractionDate >= @StartDate)
            )
            GROUP BY s.PatientId;

            -- Bucket, notice this doesnt include labs. Someone who is Positive but yet to receive an ART Service is Dispensation isnt on "Treatent"
            INSERT INTO #CurrentOnARTByVisitAndDrugs(PatientId, InteractionDate)
            SELECT PatientId, MAX(InteractionDate) InteractionDate
            FROM (
                    --includes any positive clients who had an ART Clinical Visit before the end of the reporting period and (CRITICAL) arent LTFU
                    SELECT PatientId, InteractionDate
                    FROM #CurrentOnARTByClinicalVisit
                    WHERE CurrentHivStatus = 1
                    AND ArtStartDate IS NOT NULL
                    AND ArtStartDate <= @EndDate
                    AND (
                            (@OnArtMode = 1 AND InteractionDate >= DATEADD(DD, -@LTFUThreshold, @EndDate)) OR
                            (@OnArtMode = 0 AND InteractionDate >= @StartDate)
                        )
                    UNION
                    --includes any positive clients who had an ARV dispensation before the end of the reporting period
                    SELECT PatientId, InteractionDate
                    FROM #CurrentOnARTByDrugs --LTFU threshold implicitly covered (see source)
                ) a
            GROUP BY PatientId;

            -- update relevan metadata
            UPDATE c
            SET c.DateOfBirth          = chs.DateOfBirth,
                c.CareStartDate        = chs.CareStartDate,
                c.ArtStartDate         = chs.ArtStartDate,
                c.OldestHivPosTestDate = chs.OldestHivPosTestDate,
                c.InteractionDate      = chs.LastClinicalVisit,
                c.ArtStartEditLocation = chs.ArtStartEditLocation,
                c.Sex                  = chs.Sex,
                c.AgeAsAtEnd           = dbo.fn_Age(chs.DateOfbirth, @EndDate),
                c.AgeArtStart          = dbo.fn_Age(chs.DateOfbirth, chs.ArtStartDate)
            FROM #CurrentOnARTByVisitAndDrugs c
                    JOIN ClientHivSummary chs
                        ON chs.PatientId = c.PatientId;

            UPDATE c
            SET c.CurrentHivStatus = chs.TestResult
            FROM #CurrentOnARTByVisitAndDrugs c
                    JOIN #MostRecentHIVStatusAsOfEndDate chs
                        ON chs.PatientId = c.PatientId;


            -- Identify the dead
            INSERT INTO #DeadClients(PatientId, VisitDate, DiscontinuationReason)
            SELECT dd.PatientId, dd.VisitDate, 'Death' AS DiscontinuationReason
            FROM fn_GetPatientDeadDateAsOf(@EndDate, default) dd;
            -- This function already returns one unique result per client :)

            --set dead clients in bucket
            UPDATE c
            SET c.DeadBeforeEnd              = 1,
                c.NotActive                  = 1,
                DeadDate                     = VisitDate,
                DiscontinuationDateBeforeEnd = VisitDate
            FROM #CurrentOnARTByVisitAndDrugs c
                    JOIN #DeadClients d
                        ON c.PatientId = d.PatientId;

            -- Identify trans-outs with respect to aggregation mode
            INSERT INTO #Discontinued(PatientId, VisitDate, DiscontinuationReason)
            SELECT PatientId, DATEADD(dd, DATEDIFF(dd, 0, VisitDate), 0) VisitDate, DiscontinuationReason
            FROM (SELECT PatientId,
                        VisitDate,
                        ROW_NUMBER()
                                OVER (PARTITION BY patientid
                                    ORDER BY VisitDate DESC) seq,
                        (CASE
                            WHEN (PatientMadeInactive = 1 and (PatientTransferOut is null or PatientTransferOut = 0))
                                THEN 'InActive'
                            WHEN (PatientTransferOut = 1)
                                THEN 'TO'
                            WHEN (PatientMadeInactive = 1 OR PatientTransferOut = 1)
                                THEN 'InActive/TO'
                            END)                             DiscontinuationReason
                FROM crtPatientStatus
                WHERE (PatientMadeInactive = 1 OR PatientTransferOut = 1)
                    AND DATEADD(dd, DATEDIFF(dd, 0, VisitDate), 0) <= @EndDate
                    AND (EditLocation IN (SELECT SubSiteId FROM #CurrentFacility))
                    -- only exclude trans-out at Facility Level -- @AggregationLevel = 'Facility' AND
                ) g
            WHERE seq = 1;

            -- Identify trans outs via new passerby functionality
            INSERT INTO #Discontinued(PatientId, VisitDate, DiscontinuationReason)
            SELECT ceo.PatientIdInt, DATEADD(dd, DATEDIFF(dd, 0, cmd.VisitDate), 0) VisitDate, 'PasserBy'
            FROM crtElmisOverview ceo
                    JOIN crtMedicationsDispensed cmd
                        ON ceo.InteractionIdInt = cmd.InteractionID
            WHERE ceo.ArtPasserBy = 1 -- Indexed column for speed
            AND (ceo.EditLocationId IN (SELECT SubSiteId FROM #CurrentFacility))
            AND ceo.PatientIdInt NOT IN (SELECT PatientId FROM #Discontinued);
            -- only exclude trans-out at Facility Level -- @AggregationLevel = 'Facility';

            --Remove those marked as discontinued yet have visits after (implied reactivation?)
            DELETE
            FROM d
            FROM #Discontinued d
                    JOIN ClientClinicalCareDates c ON d.PatientId = c.PatientId
            WHERE c.EditLocation IN (SELECT SubSiteId FROM #CurrentFacility)
            AND c.InteractionDate <= @EndDate
            AND d.VisitDate < c.InteractionDate;
            -- < not <= (yes factor time, e.g Final Followup before being trans-out same day)

            --set inactive clients in bucket
            UPDATE c
            SET c.NotActive                  = 1,
                DiscontinuationDateBeforeEnd = VisitDate
            FROM #CurrentOnARTByVisitAndDrugs c
                    JOIN #Discontinued dx
                        ON c.PatientId = dx.PatientId;

            -- Insert those who started PrEP in the period
            INSERT INTO #AllCurrPrEPClients(PatientId)
            SELECT DISTINCT initial.patient_guid
            FROM Art45_PrEPInitialInteraction1 initial
            WHERE initial.overview_interaction_time BETWEEN @StartDate AND @EndDate;

            -- Insert those still on PrEP in the period (follow-ups)
            INSERT INTO #AllCurrPrEPClients(PatientId)
            SELECT DISTINCT followUp.patient_guid
            FROM Art45_PrEPFollowUpInteraction1 followUp
            WHERE followUp.patient_guid NOT IN (SELECT PatientId FROM #AllCurrPrEPClients)
            AND followUp.overview_interaction_time BETWEEN @StartDate AND @EndDate;

            -- then demographic data
            UPDATE c
            SET c.Sex = chs.Sex,
                c.Age = convert(varchar, DATEDIFF(YEAR, chs.DateOfBirth, GETDATE()))
            FROM #AllCurrPrEPClients c
                    JOIN ClientHivSummary chs
                        ON c.PatientId = chs.PatientId;

            -- Initial visit date
            UPDATE c
            SET c.InitVisitDate = z.overview_interaction_time
            FROM #AllCurrPrEPClients c
                    JOIN (SELECT patient_guid, MIN(overview_interaction_time) overview_interaction_time
                        FROM Art45_PrEPInitialInteraction1
                        WHERE (patient_guid IN (SELECT PatientId FROM #AllCurrPrEPClients) AND
                                (eligibility_plan_prescription_start_pr_ep = 1
                                    OR investigations_and_referrals_pr_epd_ispensed_one_month = 1
                                    OR (drug_orders_1_med_order_text_map IS NOT NULL AND
                                        drug_orders_1_is_regimen = 1)))
                        GROUP BY patient_guid) z
                        ON c.PatientId = z.patient_guid;

            -- FollowUp properties
            UPDATE c
            SET c.FollowUpVisitDate = z.overview_interaction_time
            FROM #AllCurrPrEPClients c
                    JOIN (SELECT patient_guid, MAX(overview_interaction_time) overview_interaction_time
                        FROM Art45_PrEPFollowUpInteraction1
                        WHERE patient_guid IN (SELECT PatientId FROM #AllCurrPrEPClients)
                            AND overview_interaction_time >= @StartDate
                            AND (risk_status_client_wants_to_continue_pr_ep <> 0
                            OR risk_status_reasons_for_not_continuing_pr_ep_client_has_one_consistent_sexual_partner <> 1
                            OR risk_status_reasons_for_not_continuing_pr_ep_nolonger_involved_in_unsafe_practices <> 1
                            OR risk_status_reasons_for_not_continuing_pr_ep_partner_on_art_vls_uppressed <> 1
                            OR plan_plan_method <> 2
                            OR (drug_orders_1_med_order_text_map IS NOT NULL AND drug_orders_1_is_regimen = 1))
                        GROUP BY patient_guid) z
                        ON c.PatientId = z.patient_guid;

            INSERT INTO #CurrentOnArt(PatientId, EditLocation, Age, AgeLastVisit, Sex, ArtStartDate)
            SELECT z.PatientId,
                chs.LastClinicalVisitEditLocation,
                dbo.fn_Age(chs.DateOfbirth, @EndDate),
                dbo.fn_Age(chs.DateOfbirth, z.InteractionDate),
                chs.Sex,
                chs.ArtStartDate
            FROM #CurrentOnARTByVisitAndDrugs z
                    JOIN ClientHivSummary chs
                        ON chs.PatientId = z.PatientId
            WHERE (
                    (
                            @OnArtMode = 1
                            AND z.NotActive IS NULL
                            AND z.DeadBeforeEnd IS NULL
                        ) OR @OnArtMode = 0
                )
            AND z.PatientId NOT IN (SELECT PatientId FROM #AllCurrPrEPClients);

        IF (object_id('tempdb..#TX_New_ClientsOfInterest')) IS NOT NULL
                DROP TABLE #TX_New_ClientsOfInterest;
            SELECT chs.PatientId, chs.ArtStartDate, dbo.fn_age(cri.DateOfbirth, chs.ArtStartDate) AgeArtStart, chs.Sex
            INTO #TX_New_ClientsOfInterest
            FROM #CurrentOnArt chs -- Stop REWRITING LOGIC FROM SCRATCH, use tables which already exist. TX_NEW is a subset of TX_CURR
            JOIN crtRegistrationInteraction cri ON cri.patientid_int = chs.PatientId
                    LEFT JOIN crtPatientLocator loc ON chs.PatientId = loc.PatientId
            WHERE ArtStartDate >= @StartDate

            select DISTINCT
                reg.FirstName + ' ' + reg.SurName as ClientName, 
                CONVERT(VARCHAR,txNew.ArtStartDate, 103) AS ArtStartDate,
                reg.AgeAtDateOfEnrollment,
                dbo.fn_Age(reg.DateOfBirth, @EndDate) as Age,
                reg.ArtNumber,
                reg.Sex,
                case 
                when (ad.PhoneNumber is not null  and ad.PhoneNumber <> '') then ad.PhoneNumber
                when (ad.PhoneNumber is not null and ad.PhoneNumber = '' and ad.MobilePhoneNumber is not null) then ad.MobilePhoneNumber 
                else '' end as PhoneNumber,
                vl.LabTestValue,
                CONVERT(VARCHAR, vl.InteractionDate, 103) as VLResultDate,
                CONVERT(VARCHAR, dt.NextVisitDate, 103) AS NextVisitDate,
                CONVERT(VARCHAR,sm.PriorClinicalVisitDate, 103) as PriorClinicalVisitDate
                --wh.WhoStageToday,
                --wh.VisitDate
                from #TX_New_ClientsOfInterest txNew
                LEFT JOIN (SELECT PatientId, MAX(ArtStartDate) as ArtStartDate FROM #TX_New_ClientsOfInterest group by PatientId) txNew1
                on txNew.PatientId = txNew1.PatientId and txNew.ArtStartDate = txNew1.ArtStartDate
                join crtRegistrationInteraction reg on reg.patientId_int = txNew.PatientId
                join GuidMap mp on reg.patientId_int = mp.NaturalNumber
                LEFT join #currentAddresses ad on mp.MappedGuid = ad.PatientGUID
                LEFT JOIN ClientViralLoadTestResult vl on txNew.PatientId = vl.PatientId
                LEFT JOIN ClientClinicalCareDates dt on txNew.PatientId = dt.PatientId
                JOIN (SELECT PatientId, MAX(NextVisitDate) as NextVisitDate FROM ClientClinicalCareDates GROUP BY PatientId) dt1
                on dt.PatientId = dt1.PatientId AND dt.NextVisitDate = dt1.NextVisitDate
                LEFT JOIN ClientHivSummary sm on txNew.PatientId = sm.PatientId
                WHERE txNew.ArtStartDate BETWEEN @StartDate AND @EndDate
                ORDER BY ArtStartDate DESC       
    """)
    result = engine.execute(sql)
    rows = [dict(row) for row in result.fetchall()]
    return rows


