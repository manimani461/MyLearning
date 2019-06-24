

$today = (Get-Date).ToString("yyyy-MM-dd")


$url = "http://www.smart.grtgaz.com/api/v1/en/consommation/export/Zone.csv?startDate=2010-01-01&endDate="+$today+"&range=daily"
$Path = "D:\Data\Grtgaz\grtgaz.csv"

$FileHistory = "D:\Data\grtgaz\History\grtgaz_"+$(((get-date).ToUniversalTime()).ToString("yyyyMMddThhmmZ"))+".csv"



$Result = Invoke-WebRequest -Uri $url -OutFile $Path -Proxy http://proxy.gassco.no:4040/ 

$Result | Export-csv -Path $FileHistory -Append -Force -NoTypeInformation



