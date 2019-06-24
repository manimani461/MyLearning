$grantType = "password"
$client_Id = "6F1F883B-66A5-4303-B817-2B2D738B5222"
$username = "api_gassco1"
$password = "monteldata"

#$headers.Add("Authorization","Bearer [$token]")

$requestUri = "https://api.montelnews.com/gettoken"
$requestBody = "grant_type=$grantType&client_Id=$client_Id&username=$username&password=$password"
$accessToken = Invoke-RestMethod -Method Post -Uri $requestUri  -Body $requestBody -Proxy http://proxy.gassco.no:4040/ 

$token = $accessToken.access_token

$File = "D:\Data\Montel\data.csv"
#$url_Metadata = "https://api.montelnews.com/spot/getprices?SpotKey=1&Fields=hours&FromDate=2016-12-05&ToDate&Currency=eur&SortType=ascending"
$url_Metadata = "https://api.montelnews.com/derivatives/quote/get?SymbolKeys=ice+brn+m1&Fields=last"

#$headers.Add("AcceptEncoding","deflate")
$headers = New-Object "System.Collections.Generic.Dictionary[[String],[String]]"
$headers.Add("Authorization", "$token")
$headers.Add("AcceptEncoding", 'deflate')

Invoke-RestMethod -Method Get  -Headers $headers -Uri $url_Metadata -OutFile $File -Proxy http://proxy.gassco.no:4040/ 