﻿$grantType = "password"
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

$Meta_File = "D:\Data\Montel\Metadata.csv" 
$Meta_File_Filtered = "D:\Data\Montel\Metadata_Filtered.csv" 

#$url_Metadata = "https://api.montelnews.com/spot/getprices?SpotKey=1&Fields=hours&FromDate=2016-12-05&ToDate&Currency=eur&SortType=ascending"
$url_Metadata = "https://api.montelnews.com/derivatives/getmetadataforactivecontracts"
$Meta_out = Invoke-RestMethod -Method Get  -Headers $headers -Uri $url_Metadata  -Proxy http://proxy.gassco.no:4040/

$Meta_out.Elements | Export-Csv -Path $Meta_File -NoTypeInformation -Force

Import-CSV $Meta_File| Where { $_.SourceId -eq 'PEGAS' -or $_.SourceId -eq 'ICE' }|Select-Object 'MontelSymbol'| Where { $_.MontelSymbol -notcontains ''} | Export-Csv -Path $Meta_File_Filtered -NoTypeInformation -Force 

$SymbolKeys = Import-CSV $Meta_File_Filtered
$File = "D:\Data\Montel\MontelData.csv" 
foreach($symbol in $SymbolKeys.'MontelSymbol')
{
$Symbolkeys = $symbol
$url_price = "https://api.montelnews.com/derivatives/quote/get?SymbolKeys=$Symbolkeys&Fields=last&Fields=Tradingend&Fields=MontelSymbol"
Invoke-RestMethod -Method Get  -Headers $headers -Uri $url_price -OutFile $File -Proxy http://proxy.gassco.no:4040/
}


 