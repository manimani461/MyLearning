$method = "PUT"
$file = "C:\Users\noa_mab2\Documents\Data\QAD\sct_det.csv"
$name = (Get-Item $file).Name
$headerDate = '2015-12-11'
$headers = @{"x-ms-version"="$headerDate"}
$StorageAccountName = "badevadlsg2"
$StorageContainerName = "adlsdev"
$StorageAccountKey = "w/AdJ4feLp5Pqbc9zRAtOfRRoax70KcYtik0ASoTzz6lG3kuqIOldgbHblTKbv7/BBJhf4s6UYhV76NUZEE1IQ=="
$Url = "https://$StorageAccountName.blob.core.windows.net/$StorageContainerName/Governed Data/Sources/LMAS/QAD/Staging/$name"
$body = "Hello world"
$xmsdate = (get-date -format r).ToString()
$headers.Add("x-ms-date",$xmsdate)
$contentLength = (Get-Item $file).Length
$headers.Add("Content-Length","$contentLength")
$headers.Add("x-ms-blob-type","BlockBlob")
$contentType = "application/json"
#$headers.Add("Content-Type","$contentType")

$signatureString = "$method$([char]10)$([char]10)$([char]10)$contentLength$([char]10)$([char]10)$([char]10)$([char]10)$([char]10)$([char]10)$([char]10)$([char]10)$([char]10)"
#Add CanonicalizedHeaders
$signatureString += "x-ms-blob-type:" + $headers["x-ms-blob-type"] + "$([char]10)"
$signatureString += "x-ms-date:" + $headers["x-ms-date"] + "$([char]10)"
$signatureString += "x-ms-version:" + $headers["x-ms-version"] + "$([char]10)"
#Add CanonicalizedResource
$uri = New-Object System.Uri -ArgumentList $url
$signatureString += "/" + $StorageAccountName + $uri.AbsolutePath

$dataToMac = [System.Text.Encoding]::UTF8.GetBytes($signatureString)

$accountKeyBytes = [System.Convert]::FromBase64String($StorageAccountKey)

$hmac = new-object System.Security.Cryptography.HMACSHA256((,$accountKeyBytes))
$signature = [System.Convert]::ToBase64String($hmac.ComputeHash($dataToMac))

$headers.Add("Authorization", "SharedKey " + $StorageAccountName + ":" + $signature);
write-host -fore green $signatureString
write-host -fore green $headers
Invoke-RestMethod -Uri $Url -Method $method -headers $headers -InFile $file