$grantType = "password"
$client_Id = "6F1F883B-66A5-4303-B817-2B2D738B5222"
$username = "api_gassco1"
$password = "monteldata"

#$headers.Add("Authorization","Bearer [$token]")

$requestUri = "https://api.montelnews.com/gettoken"
$requestBody = "grant_type=$grantType&client_Id=$client_Id&username=$username&password=$password"
$accessToken = Invoke-RestMethod -Method Post -Uri $requestUri  -Body $requestBody -Proxy http://proxy.gassco.no:4040/ 

$token = $accessToken.access_token

#$headers.Add("AcceptEncoding","deflate")
$headers = New-Object "System.Collections.Generic.Dictionary[[String],[String]]"
$headers.Add("Authorization", "Bearer $token")
$headers.Add("AcceptEncoding", 'deflate')

$Meta_File = "D:\Data\Montel\Sandbox\Metadata.csv" 
$Meta_File_Filtered = "D:\Data\Montel\Sandbox\Metadata_Filtered.csv" 

#$url_Metadata = "https://api.montelnews.com/spot/getprices?SpotKey=1&Fields=hours&FromDate=2016-12-05&ToDate&Currency=eur&SortType=ascending"
$url_Metadata = "https://api.montelnews.com/derivatives/getmetadataforactivecontracts"
$Meta_out = Invoke-RestMethod -Method Get  -Headers $headers -Uri $url_Metadata  -Proxy http://proxy.gassco.no:4040/

$Meta_out.Elements | Export-Csv -Path $Meta_File -NoTypeInformation -Force

Import-CSV $Meta_File| Where {  ($_.SourceId -eq 'PEGAS' -or $_.SourceId -eq 'ICE') -and ($_.CommodityGroup -eq 'Oil' -or $_.CommodityGroup -eq 'Gas' -or $_.CommodityGroup -eq 'Coal' -or $_.CommodityGroup -eq 'Green' ) }|Select-Object 'MontelSymbol'| Where { $_.MontelSymbol -notcontains ''} | Export-Csv -Path $Meta_File_Filtered -NoTypeInformation -Force 

$SymbolKeys = Import-CSV $Meta_File_Filtered

$FileHistoryJSON = "D:\Data\Montel\Sandbox\MontelData_"+$(((get-date).ToUniversalTime()).ToString("yyyyMMddThhmmZ"))+".json"
$FileHistoryCSV = "D:\Data\Montel\Sandbox\MontelData_"+$(((get-date).ToUniversalTime()).ToString("yyyyMMddThhmmZ"))+".csv"



foreach($symbol in $SymbolKeys.'MontelSymbol')
{
$url_price = "https://api.montelnews.com/derivatives/quote/get?SymbolKeys=$symbol&Fields=SymbolKey&Fields=TickerSymbol&Fields=FrontSymbol&Fields=MontelSymbol&Fields=Last&Fields=PrevSettlement&Fields=UpdateTime&Fields=High&Fields=Low&Fields=Currency&Fields=ChangePercent&Fields=Date"
$Result = Invoke-RestMethod -Method Get  -Headers $headers -Uri $url_price -Proxy http://proxy.gassco.no:4040/
$Result.Elements | Export-Csv -Path $FileHistoryCSV  -Append -Force -NoTypeInformation

}



 