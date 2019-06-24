#Need to use TLS 1.2 against gassco site, powershell uses TLS 1.0 by default. 
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$url = "https://flow.gassco.no/realTimeFlow"
$Path = "D:\Data\Gassco\Gassco.csv"
$Stamp = (Get-Date).toString("ddMMyyyy")

$PathHistory = "D:\Data\Gassco\History\Gassco_"+$(((get-date).ToUniversalTime()).ToString("yyyyMMddThhmmZ"))+".csv"

$logfile = "D:\Code\Sources\Gassco\Logs\Gassco_$Stamp.log"

#Start-Transcript -Path $logfile -Append 

Remove-Item -Path $Path -ErrorAction SilentlyContinue

$Result = (Invoke-WebRequest -Uri $url -UseBasicParsing ).content 
 $timestamp = ($Result | ConvertFrom-Json| Select-Object timestamp).timestamp
 $Result | ConvertFrom-Json| Select-Object -ExpandProperty nodes|Select-Object *,@{Name='timestamp';Expression = {$timestamp}} | Export-Csv -Path $Path -NoTypeInformation -Force
 $Result | ConvertFrom-Json| Select-Object -ExpandProperty nodes|Select-Object *,@{Name='timestamp';Expression = {$timestamp}} | Export-Csv -Path $PathHistory -NoTypeInformation -Force

 #Stop-Transcript