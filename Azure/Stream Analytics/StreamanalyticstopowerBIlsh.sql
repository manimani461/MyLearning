
WITH temp as (
SELECT
    *,
    eventObj = GetArrayElement(event,0),
    UDF.flattenCustomDimensions(I.context.custom.dimensions) as dim
    FROM [lleap-lsh-events] as I

),
tempPages as (
SELECT
    *,
    eventObjPages = GetArrayElement([view],0),
    UDF.flattenCustomDimensions(IPage.context.custom.dimensions) as dimPage
    FROM [lleap-lsh-pages] as IPage

)



SELECT
    ID = internal.data.id, 
    Name = eventObj.Name,
    Version = context.application.version,
    [Event Time] = context.data.eventTime,
    [User ID] = context.[user].anonId,
    [Session ID] = context.session.id,
    [IP] = context.location.clientip,
    [Country] = context.location.country,   

    temp.dim.[Installed Culture],
    temp.dim.[Language],
    temp.dim.[Screen Resolution],
    temp.dim.[OS Version],
    temp.dim.[OS Architecture],
    temp.dim.[Organization],
    temp.dim.[Contact],
    temp.dim.[Email],
    temp.dim.[Computer]
    
INTO [lleap-pbi-lsh-events-users]
FROM temp WHERE (eventObj.Name = 'Application Start'
OR eventObj.Name = 'Computer Registered'
OR eventObj.Name = 'Data Collection Allowed'
OR eventObj.Name = 'Data Collection Declined')
and context.location.clientip != '79.160.140.131'
and context.location.clientip != '79.160.140.0'
and context.location.clientip != '77.241.110.0'
and context.location.clientip != '212.60.115.0'




SELECT
    ID = internal.data.id, 
    Name = eventObj.Name,
    Version = context.application.version,
    [Event Time] = context.data.eventTime,
    [User ID] = context.[user].anonId,
    [Session ID] = context.session.id,
    [IP] = context.location.clientip,
    [Country] = context.location.country,   

    temp.dim.[Download Version],
    temp.dim.[Download Location],
    temp.dim.[Download Title],
    temp.dim.[Installation Return Code],
    temp.dim.[Elapsed Time (ms)],
    temp.dim.[Download Speed (MB/s)],
    temp.dim.[Download Size (MB)],
    temp.dim.[Download Time (ms)],
    temp.dim.[Error ID],
    temp.dim.[Error Message]
    
INTO [lleap-pbi-lsh-events-updates]
FROM temp WHERE (eventObj.Name = 'AutoUpdate Install Started'
OR eventObj.Name = 'AutoUpdate Install Error'
OR eventObj.Name = 'AutoUpdate Install Completed'
OR eventObj.Name = 'AutoUpdate Ignore'
OR eventObj.Name = 'AutoUpdate Download Started'
OR eventObj.Name = 'AutoUpdate Download Deleted'
OR eventObj.Name = 'AutoUpdate Download Completed')
and context.location.clientip != '79.160.140.131'
and context.location.clientip != '79.160.140.0'
and context.location.clientip != '77.241.110.0'
and context.location.clientip != '212.60.115.0'


 

SELECT
    ID = internal.data.id, 
    Name = eventObjPages.Name,
    Version = context.application.version,
    [Event Time] = context.data.eventTime,
    [User ID] = context.[user].anonId,
    [Session ID] = context.session.id,
    [IP] = context.location.clientip,
    [Country] = context.location.country   
    
INTO [lleap-pbi-lsh-events-pages]
FROM tempPages
WHERE
context.location.clientip != '79.160.140.131'
and context.location.clientip != '79.160.140.0'
and context.location.clientip != '77.241.110.0'
and context.location.clientip != '212.60.115.0'



