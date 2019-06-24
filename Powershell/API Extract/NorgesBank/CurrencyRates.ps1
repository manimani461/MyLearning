$url = "https://data.norges-bank.no/api/data/EXR/B..NOK.SP?startPeriod=2011-01&format=csv"
$Outfile = "D:\Data\NorgesBank\CurrencyRates.csv"

$Result = Invoke-RestMethod -Uri $url -OutFile $Outfile -Proxy http://proxy.gassco.no:4040/



