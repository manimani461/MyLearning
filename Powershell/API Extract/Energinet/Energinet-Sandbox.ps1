#$specAsJson = Get-Content -Raw D:\Data\Energinet\Energinet.json


$url = "https://api.energidataservice.dk/datastore_search_sql?sql=SELECT%20*%20from%20%22gasflow%22%20as%20gf%20WHERE%20gf.%22GasDay%22%20%3E%20((current_timestamp%20at%20time%20zone%20%27UTC%27)-INTERVAL%20%2730%20days%27)"

#$url = "https://api.energidataservice.dk/datastore_search_sql?sql=SELECT%20*%20from%20%22gasflow%22%20LIMIT%20100"

$File = "D:\Data\Energinet\Energinet.csv"


$Result = (Invoke-WebRequest -Uri $url -UseBasicParsing ).content 

$Result = $Result | ConvertFrom-Json | select -expand result | Select   -expand records  
$out = $Result | Select GasDay,KWhToOrFromStorage, KWhFromNorthSea,KWhToDenmark,KWhToSweden,KWhToOrFromGermany| Sort-Object GasDay  

$out

Remove-Item -Path $File
$out   | Export-csv -Path $File -Append -Force -NoTypeInformation


#Get-History
#$spec = $specAsJson | ConvertFrom-Json

#$JSONResult = $spec.result.records | Select GasDay

#JSONResult



