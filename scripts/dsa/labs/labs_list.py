from typing import List, Optional

from sqlalchemy import text
from sqlalchemy.engine.base import Engine

def get_labs_list(engine: Engine, start_date: str, end_date: str) -> Optional[List[dict]]:
    """
    Returns a list of all labs.

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


    IF (object_id('tempdb..#currentAddresses')) IS NOT NULL
    DROP TABLE #currentAddresses;
    select distinct PatientGuid, PhoneNumber, MobilePhoneNumber 
    into #currentAddresses
    from Address 
    where AddressType = 'Current'
    
    SET @StartDate = '{start_date}'
    SET @EndDate = '{end_date}'
    SET @ProvinceId = (select Value from Setting where Name = 'ProvinceId')
    SET @DistrictId = (select Value from Setting where Name = 'DistrictId')
    SET @FacilityId = (select Value from Setting where Name = 'FacilityHmisCode')
    SET NOCOUNT ON
    
    
    IF object_id('tempdb..#temptbl') is not null
    DROP TABLE #temptbl
    SELECT DISTINCT
	  registration.PatientID,
    registration.FirstName + ' ' + registration.SurName as ClientName,
    registration.Sex,    
    CONVERT(varchar, registration.DateofBirth, 103) as DateOfBirth,
    Age = @YearToday - registration.BirthYear,
    case 
    when (ad.PhoneNumber is not null  and ad.PhoneNumber <> '') then ad.PhoneNumber
    when (ad.PhoneNumber is not null and ad.PhoneNumber = '' and ad.MobilePhoneNumber is not null and ad.MobilePhoneNumber <> '') then ad.MobilePhoneNumber 
    else 'UNKNOWN' 
    end as PhoneNumber,
	  a.LabTestID,      
	  labTypes.LabTypeName,
    CONVERT(varchar, a.LabOrderDate, 103)                                                   as LabOrderDate,
    labOrderStatus.OrderStatusName,
    CONVERT(varchar, a.DateOrderStatusChanged, 103)  as DateOrderStatusChanged,
	  ISNULL(CONVERT(varchar, d.InteractionDate, 103), 'unknown') as LabResultsDate,
    ISNULL(a.SpecialInstructions, 'None')                            as OrderSpecialInstructions,
    a.OrderAccessionNumber,
    labTestsDictionary.TestUnit                                      as LabTestName,
    ISNULL(c.LabTestValue, 'Pending')                                       as LabTestValue
    
    INTO #temptbl
    --select OrderStatusID,*
    FROM
    (
        SELECT
          b.PatientID,
          a.OriginatingServiceCode,
          a.DateOrderStatusChanged,
          a.LabOrderDate,
          a.LabNumberSamples,
          a.InteractionID         as LabOrderDetailInteractionID,
          a.EditLocationSeqNumber as LabOrderEditLocationSeqNumber,
          b.InteractionID         as LabOrderInteractionID,
          a.LaboratoryOrderID,
          a.OrderStatusID,
          a.OrderPriorityID,
          a.LabTestID,
          a.SpecialInstructions,
          a.OrderAccessionNumber,
          b.Deprecated            as LabOrderDeprecated
          FROM
          crctLaboratoryOrderDetails a
          LEFT JOIN crtLaboratoryOrders b
          ON b.InteractionID = a.InteractionID
          AND b.EditLocation = a.EditLocation
          AND b.EditLocationSeqNumber = a.EditLocationSeqNumber
          WHERE
          b.Deprecated = 0 and DATEADD(dd, DATEDIFF(dd, 0, a.LabOrderDate), 0) >= @StartDate and DATEADD(dd, DATEDIFF(dd, 0, a.LabOrderDate), 0) <= @EndDate
          ) a
          
          LEFT JOIN crctLaboratoryResultDetails c
          ON c.LaboratoryOrderID = a.LaboratoryOrderID AND c.LabTestID = a.LabTestID
          
          LEFT JOIN crtLaboratoryResults d
          ON d.InteractionID = c.InteractionID
          AND d.EditLocationSeqNumber = c.EditLocationSeqNumber

          LEFT JOIN LabTestsDictionary labTestsDictionary
          ON labTestsDictionary.LabTestID = a.LabTestID
          
          LEFT JOIN GUIDMap h
          ON h.naturalNumber = a.PatientID
          
          LEFT JOIN GUIDMap n
          ON n.naturalNumber = a.LabOrderDetailInteractionID

          LEFT JOIN #currentAddresses ad on h.MappedGuid = ad.PatientGUID
          
          LEFT JOIN Registration registration
          ON registration.PatientGUID = h.MappedGUID
          
          LEFT JOIN LabTestsValidation labTestsValidation
          ON labTestsValidation.LabTestID = labTestsDictionary.LabTestID AND labTestsValidation.Sex = registration.Sex
          -- validations must be against gender
          
          LEFT JOIN LabOrderPriorities labOrderPriorities
          ON labOrderPriorities.LabOrderPriorityID = a.OrderPriorityID
          
          LEFT JOIN LabOrderStatus labOrderStatus
          ON labOrderStatus.LabOrderStatusID = a.OrderStatusID
          
          LEFT JOIN ServiceCodes serviceCodes
          ON serviceCodes.ServiceCode = a.OriginatingServiceCode
          
          LEFT JOIN LabTypes labTypes
          ON labTypes.LabTypeID = labTestsDictionary.LabTypeID
          
          WHERE (d.Deprecated <> 1 OR d.Deprecated is null)
          AND a.LabOrderDate BETWEEN @StartDate AND @EndDate
          
          
          select *
          from #temptbl;
          
    """)
    result = engine.execute(sql)
    rows = [dict(row) for row in result.fetchall()]
    return rows