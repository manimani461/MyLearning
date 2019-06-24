$URI = "http://marketinformation.natgrid.co.uk/MIPIws-public/public/publicwebservice.asmx?WSDL"

$xmlFile = 'D:\Code\Sources\NG\soapall.xml'

$File = 'D:\Data\NG\NGdata_'

$DataObjectList = @('NTS Energy offtaken, powerstations total',

'NTS Energy offtaken, LDZ offtake total',

'NTS Energy offtaken, industrial offtake total',

'Storage, Daily Aggregated Stock level, D+1',

'System Entry Energy, Bacton-Perenco, D+1',

'System Entry Energy, BactonSeal, D+1',

'System Entry Energy, Bacton-Shell, D+1',

'System Entry Energy, Bacton-Tullow, D+1',

'System Entry Energy, Borrow, D+1',

'System Entry Energy, Easington-Amethyst, D+1',

'System Entry Energy, Easington-Dimlington, D+1',

'System Entry Energy, Easington-WestSole',

'System Entry Energy, Easington-York, D+1',

'System Entry Energy, STFergus-Mobil, D+1',

'System Entry Energy, STFergus-Shell, D+1',

'System Entry Energy, STFergus-NSMP, D+1',

'System Entry Energy, Teesside-BP, D+1',

'System Entry Energy, Teesside-PX, D+1',

'System Entry Energy, Theddlethorpe, D+1')

 

$count = 1

 

function NGDataExport($URI,$xmlFile,$FileCSV )

{

$result = (iwr $URI –infile $xmlFile –contentType "text/xml" –method POST)

 

$ReturnXml =$result.Content

# Replace content

 

$ReturnXml = $ReturnXml.Replace('<?xml version="1.0" encoding="utf-8"?>',"")

$ReturnXml = $ReturnXml.Replace('<soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">',"<Envelope>")

$ReturnXml = $ReturnXml.Replace('soap:Body',"Body")

$ReturnXml = $ReturnXml.Replace('GetPublicationDataWMResponse xmlns="http://www.NationalGrid.com/MIPI/"',"GetPublicationDataWMResponse")

$ReturnXml = $ReturnXml.Replace('soap:Envelope',"Envelope")

 

# Parse XML

$xmlcontent = $ReturnXml

$xml = New-Object -TypeName System.Xml.XmlDocument

$xml.LoadXml($xmlcontent)

 

# Convert to CSV

 

$xml.Envelope.Body.GetPublicationDataWMResponse.GetPublicationDataWMResult.CLSMIPIPublicationObjectBE.PublicationObjectData.CLSPublicationObjectDataBE| Export-Csv $FileCSV -NoTypeInformation -Delimiter:"," -Encoding:UTF8

 

}

 

foreach($object in $DataObjectList) 

{

 

$xmldataobject = $object.ToString()

$dateTo = (Get-Date -format "yyyy-MM-dd").ToString()

$dateFrom = "2010-01-01"

$xml = [xml](Get-Content $xmlFile)

$xml.Envelope.Body.GetPublicationDataWM.reqObject.FromDate = $dateFrom

$xml.Envelope.Body.GetPublicationDataWM.reqObject.ToDate = $dateTo

$xml.Envelope.Body.GetPublicationDataWM.reqObject.PublicationObjectNameList.string = $xmldataobject

$xml.Save($xmlFile)

$Filecsv = $File + $count+ ".csv"

NGDataExport $URI $xmlFile $FileCSV

$count = $count + 1

 

} 

 
