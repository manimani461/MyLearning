
WITH temp as (
SELECT
    *,
    eventObj = GetArrayElement(event,0),
    UDF.flattenCustomDimensions(I.context.custom.dimensions) as dim,
    UDF.flattenCustomDimensions(I.context.custom.metrics) as dimm
    FROM [lleap-ia-events] as I
), err as (
SELECT 
    *,
    errObj = GetArrayElement(basicException,0)
    FROM [lleap-ia-exceptions] as E
)




SELECT
    id = internal.data.id, 
    name = eventObj.Name,
    version = context.application.version,
    eventTime = context.data.eventTime,
    userId = context.[user].anonId,
    sessionId = context.session.id,
    ip = context.location.clientip,
    country = SUBSTRING(context.location.country,0,50),   

    sessionFilename = SUBSTRING (temp.dim.[Session Filename],0,100),
    sessionType = temp.dim.[Session Type],
    profileSettings = SUBSTRING (temp.dim.[Profile Settings],0,50),
    simulatorType = temp.dim.[Simulator Type],
    simulatorSystemType = temp.dim.[Simulator System Type],
    debriefSystemType = temp.dim.[Debrief System Type],
    elapsedTimeMs = CAST (temp.dimm.[Elapsed Time (ms)].value as bigint),
    sessionStartResult = temp.dim.[Session Start Result],
    errorMessage = temp.dim.[Error Message],
    sessionRunId = temp.dim.[Session Run ID]
    
INTO [lleap-pbi-ia-events-sessions-sql]
FROM temp WHERE 
(eventObj.Name = 'Session Duration'
OR eventObj.Name = 'Session Start Fail') 
and context.location.clientip != '79.160.140.131'
and context.location.clientip != '79.160.140.0'
and context.location.clientip != '77.241.110.0'
and context.location.clientip != '212.60.115.0'


SELECT
    id = internal.data.id, 
    name = eventObj.Name,
    version = context.application.version,
    eventTime = context.data.eventTime,
    userId = context.[user].anonId,
    sessionId = context.session.id,
    ip = context.location.clientip,
    country = SUBSTRING(context.location.country,0,50),      

    debriefSystemName = temp.dim.[Debrief System Name],
    debriefSystemType = temp.dim.[Debrief System Type],
    debriefSystemIp = temp.dim.[Debrief System IP],
    debriefSystemVersion = temp.dim.[Debrief System Version],
    elapsedTimeMs = CAST (temp.dimm.[Elapsed Time (ms)].value as bigint),
    sessionRunId = temp.dim.[Session Run ID]
    
INTO [lleap-pbi-ia-events-debriefs-sql]
FROM temp
WHERE 
(eventObj.Name = 'Debrief System Connect'
OR eventObj.Name = 'Debrief System Open')
and context.location.clientip != '79.160.140.131'
and context.location.clientip != '79.160.140.0'
and context.location.clientip != '77.241.110.0'
and context.location.clientip != '212.60.115.0'

 

SELECT
    id = internal.data.id, 
    name = eventObj.Name,
    version = context.application.version,
    eventTime = context.data.eventTime,
    userId = context.[user].anonId,
    sessionId = context.session.id,
    ip = context.location.clientip,
    country = SUBSTRING(context.location.country,0,50),     

    simulatorType = temp.dim.[Simulator Type],
    simulatorServerType = temp.dim.[Simulator Server Type],
    simulatorSerial = temp.dim.[Simulator Serial],
    simulatorName = SUBSTRING (temp.dim.[Simulator Name],0,30),
    simulatorBatteryStatus = temp.dim.[Simulator Battery Status],
    simulatorConnectionMode = temp.dim.[Simulator Connection Mode],
    simulatorBatteryLevel = CAST (temp.dimm.[Simulator Battery Level].value as bigint),
    simulatorProtocolVersion = temp.dim.[Simulator Protocol Version],
    simulatorApplicationVersion = temp.dim.[Simulator Application Version],
    warningType = temp.dim.[Warning Type],
    mac = temp.dim.[MAC],
    simulatorBatteryTemperatureCelsius = CAST (temp.dimm.[BatteryTemperatureCelsius].value as bigint),
    simulatorCompressionCount = CAST (temp.dimm.[CompNum].value as bigint),
    simulatorCompressorOnTime = CAST (temp.dimm.[CompressorOnTime].value as bigint),
    simulatorBloodAmount = CAST (temp.dimm.[BloodAmnt].value as bigint),
    simulatorBattery1CycleCount = CAST (temp.dimm.[Battery1CycleCount].value as bigint),
    simulatorBattery2CycleCount = CAST (temp.dimm.[Battery2CycleCount].value as bigint),
    simulatorBatteryOnTime = CAST (temp.dimm.[OnTimeBatt].value as bigint),
    simulatorPowerOnTime = CAST (temp.dimm.[OnTimePwr].value as bigint),
    simulatorPowerOnCount = CAST (temp.dimm.[PwrOnNum].value as bigint),
    simulatorVentilationCount = CAST (temp.dimm.[VentNum].value as bigint)

INTO [lleap-pbi-ia-events-sims-sql]
FROM temp
WHERE 
(eventObj.Name = 'Simulator Connect' 
OR eventObj.Name = 'Simulator Status Collection'
OR eventObj.Name = 'Simulator Hardware Statistics'
OR eventObj.Name = 'Simulator Hardware Warning')
and context.location.clientip != '79.160.140.131'
and context.location.clientip != '79.160.140.0'
and context.location.clientip != '77.241.110.0'
and context.location.clientip != '212.60.115.0'




SELECT
    id = internal.data.id, 
    name = eventObj.Name,
    version = context.application.version,
    eventTime = context.data.eventTime,
    userId = context.[user].anonId,
    sessionId = context.session.id,
    ip = context.location.clientip,
    country = SUBSTRING(context.location.country,0,50),      
    
    installedCulture = temp.dim.[Installed Culture],
    language = temp.dim.[Language],
    screenResolution = temp.dim.[Screen Resolution],
    osVersion = temp.dim.[OS Version],
    osArchitecture = temp.dim.[OS Architecture],
    serialNumber = temp.dim.[Serial Number]
    
INTO [lleap-pbi-ia-events-users-sql]
FROM temp WHERE eventObj.Name = 'Application Start'
and context.location.clientip != '79.160.140.131'
and context.location.clientip != '79.160.140.0'
and context.location.clientip != '77.241.110.0'
and context.location.clientip != '212.60.115.0'




SELECT
   userId = context.[user].anonId
INTO [lleap-pbi-ia-users-sql]
FROM temp WHERE context.[user].anonId IS NOT NULL
and context.location.clientip != '79.160.140.131'
and context.location.clientip != '79.160.140.0'
and context.location.clientip != '77.241.110.0'
and context.location.clientip != '212.60.115.0'

SELECT
   sessionId = context.session.id
INTO [lleap-pbi-ia-sessions-sql]
FROM temp WHERE context.session.id IS NOT NULL
and context.location.clientip != '79.160.140.131'
and context.location.clientip != '79.160.140.0'
and context.location.clientip != '77.241.110.0'
and context.location.clientip != '212.60.115.0'

SELECT
sessionRunId = temp.dim.[Session Run ID]
INTO [lleap-pbi-ia-session-runs-sql]
FROM temp WHERE temp.dim.[Session Run ID] IS NOT NULL
and context.location.clientip != '79.160.140.131'
and context.location.clientip != '79.160.140.0'
and context.location.clientip != '77.241.110.0'
and context.location.clientip != '212.60.115.0'

SELECT
mac = temp.dim.[MAC]
INTO [lleap-pbi-ia-mac-sql]
FROM temp WHERE temp.dim.[MAC] IS NOT NULL AND temp.dim.[MAC] != ""
and context.location.clientip != '79.160.140.131'
and context.location.clientip != '79.160.140.0'
and context.location.clientip != '77.241.110.0'
and context.location.clientip != '212.60.115.0'




SELECT
    id = internal.data.id, 
    version = context.application.version,
    eventTime = context.data.eventTime,
    userId = context.[user].anonId,
    sessionId = context.session.id,
    ip = context.location.clientip,
    country = SUBSTRING(context.location.country,0,50),
    exceptionType = SUBSTRING(errObj.exceptionType,0,50),
    assembly = SUBSTRING(errObj.assembly,0,100),
    outerMessage = SUBSTRING(errObj.outerExceptionMessage,0,200),
    innerMessage = SUBSTRING(errObj.innermostExceptionMessage,0,200),
    failedUserCodeMethod = SUBSTRING(errObj.failedUserCodeMethod,0,100)
INTO [lleap-pbi-ia-exceptions-sql]
FROM err as E
WHERE context.location.clientip != '79.160.140.131'
and context.location.clientip != '79.160.140.0'
and context.location.clientip != '77.241.110.0'
and context.location.clientip != '212.60.115.0'
