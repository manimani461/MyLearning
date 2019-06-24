$url = "https://api.energidataservice.dk/datastore_search_sql?sql=SELECT%20*%20from%20%22gasflow%22%20LIMIT%20100"


$File = "D:\Data\Energinet\Energinet.csv"

$Result = Invoke-RestMethod -Uri $url  -Proxy http://proxy.gassco.no:4040/

$Result.result.records | Export-csv -Path $File -Append -Force -NoTypeInformation



