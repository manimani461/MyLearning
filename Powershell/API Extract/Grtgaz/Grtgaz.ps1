

$today = (Get-Date).ToString("yyyy-MM-dd")


$url = "http://www.smart.grtgaz.com/api/v1/en/consommation/export/Zone.csv?startDate=2010-01-01&endDate="+$today+"&range=daily"
$Path = "D:\Data\Grtgaz\grtgaz.csv"



Invoke-WebRequest -Uri $url -OutFile $Path -Proxy http://proxy.gassco.no:4040/ 



