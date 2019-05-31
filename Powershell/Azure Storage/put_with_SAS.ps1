

#Our source File:
$file = "C:\Users\noa_mab2\Documents\Data\QAD\sct_det.csv"

#Get the File-Name without path
$name = (Get-Item $file).Name

#The target URL wit SAS Token
$uri = "https://badevadlsg2.blob.core.windows.net/adlsdev/Governed Data/Sources/LMAS/QAD/Staging/$($name)?sv=2018-03-28&ss=bfqt&srt=sco&sp=rwdlacup&se=2019-07-05T19:48:29Z&st=2019-05-23T11:48:29Z&spr=https,http&sig=19yoMLcHpp0R2cg0IM7Otiw%2BkSJNGBv6nRj8%2B%2BRLrZQ%3D"

#Define required Headers
$headers = @{
    'x-ms-blob-type' = 'PageBlob'
}

#Upload File...
Invoke-RestMethod -Uri $uri -Method Put -Headers $headers -InFile $file  -TimeoutSec 3600
