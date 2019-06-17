WITH flat AS (
SELECT
    eventMeta.ArrayValue.name as EventName,
    input.internal.data.id as EventId,
    input.context.data.eventTime as EventTime,
    input.context.location.continent as Continent,
    input.context.location.country as Country,
    input.context.location.province as Province,
    input.context.location.city as City,
    UDF.flattenDimensions(input.context.custom.dimensions) as Dimensions
FROM mdtblobs input
CROSS APPLY GetArrayElements(Input.[event]) as eventMeta
)

SELECT
    event.EventId as id,
    event.EventName as EventName,
    event.EventTime as EventTime,
    event.Country as Country,
    event.City as City,
    event.Dimensions.EXISTINGEDITIONID as EXISTINGEDITIONID,
    event.Dimensions.EXISTINGKEYBOARDLOCALE as EXISTINGKEYBOARDLOCALE,
    event.Dimensions.EXISTINGOSINSTALLED as EXISTINGOSINSTALLED,
    event.Dimensions.EXISTINGPRODUCTNAME as EXISTINGPRODUCTNAME,
    event.Dimensions.EXISTINGRELEASEID as EXISTINGRELEASEID,
    event.Dimensions.EXISTINGTIMEZONENAME as EXISTINGTIMEZONENAME,
    event.Dimensions.EXISTINGUILANGUAGE as EXISTINGUILANGUAGE,
    event.Dimensions.EXISTINGUSERLOCALE as EXISTINGUSERLOCALE,
    event.Dimensions.SERIALNUMBER as SERIALNUMBER,
    event.Dimensions.MAKE as MAKE,
    event.Dimensions.MODEL as MODEL,
    event.Dimensions.COMPUTERTYPE as COMPUTERTYPE,
    event.Dimensions.MEMORY as MEMORY,
    event.Dimensions.PROCESSORSPEED as PROCESSORSPEED,
    event.Dimensions.CAPABLEARCHITECTURE as CAPABLEARCHITECTURE,
    event.Dimensions.HOSTNAME as HOSTNAME,
    event.Dimensions.IPADDRESS001 as IPADDRESS001,
    event.Dimensions.MACADDRESS001 as MACADDRESS001,
    event.Dimensions.OSDisk as OSDisk,
    event.Dimensions.DestinationDisk as DestinationDisk,
    event.Dimensions.FirmwareProductKey as FirmwareProductKey,
    event.Dimensions.KEYBOARDLOCALE as KEYBOARDLOCALE,
    event.Dimensions.ISONBATTERY as ISONBATTERY,
    event.Dimensions.ISUEFI as ISUEFI,
    event.Dimensions.ISVM as ISVM,
    event.Dimensions.DEPLOYMENTMETHOD as DEPLOYMENTMETHOD,
    event.Dimensions.WDSSERVER as WDSSERVER,
    event.Dimensions.DEPLOYROOT as DEPLOYROOT,
    event.Dimensions.EVENTSERVICE as EVENTSERVICE,
    event.Dimensions.LMDriverOption as LMDriverOption,
    event.Dimensions.TASKSEQUENCENAME as TASKSEQUENCENAME,
    event.Dimensions.TIMEZONENAME as TIMEZONENAME,
    event.Dimensions.UILANGUAGE as UILANGUAGE,
    event.Dimensions.USERLOCALE as USERLOCALE,
    event.Dimensions.LaerdalLicenseCode as LaerdalLicenseCode,
    event.Dimensions.LaerdalLicenseKey as LaerdalLicenseKey,
    event.Dimensions.OSCURRENTVERSION as OSCURRENTVERSION,
    event.Dimensions.MDTRevisionID as MDTRevisionID,
    event.Dimensions.ProductInstallerProducts as ProductInstallerProducts,
    event.Dimensions.LLEAPRevisionID as LLEAPRevisionID,
    event.Dimensions.LLEAPSoftwareVersion as LLEAPSoftwareVersion,
    event.Dimensions.PMRevisionID as PMRevisionID,
    event.Dimensions.PMSoftwareVersion as PMSoftwareVersion,
    event.Dimensions.RQIEURevisionID as RQIEURevisionID,
    event.Dimensions.RQIEUSoftwareVersion as RQIEUSoftwareVersion,
    event.Dimensions.RQIUSRevisionID as RQIUSRevisionID,
    event.Dimensions.RQIUSSoftwareVersion as RQIUSSoftwareVersion,
    event.Dimensions.SonoSimRevisionID as SonoSimRevisionID,
    event.Dimensions.SonoSimSoftwareVersion as SonoSimSoftwareVersion,
    event.Dimensions.SimViewRevisionID as SimViewRevisionID,
    event.Dimensions.SimViewSoftwareVersion as SimViewSoftwareVersion,
    event.Dimensions.ResetConfigXML as ResetConfigXML,
    event.Dimensions.PM as media_PM,
    event.Dimensions.DiskSize as media_DiskSize,
    event.Dimensions.RQIEU as media_RQIEU,
    event.Dimensions.MediaGuid as media_MediaGuid,
    event.Dimensions.Created as media_Created,
    event.Dimensions.ComputerName as media_ComputerName,
    event.Dimensions.SimView as media_SimView,
    event.Dimensions.LLEAP as media_LLEAP,
    event.Dimensions.Username as media_Username,
    event.Dimensions.RunAsAdmin as media_RunAsAdmin,
    event.Dimensions.SimViewRevision as media_SimViewRevision,
    event.Dimensions.SonoSim as media_SonoSim,
    event.Dimensions.RQIUS as media_RQIUS,
    event.Dimensions.UserDomain as media_UserDomain
INTO JRMDTPowerBI
FROM flat as event