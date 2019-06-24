$agsiurl = @("https://agsi.gie.eu/api/data/FR","https://agsi.gie.eu/api/data/DE",

"https://agsi.gie.eu/api/data/NL",

"https://agsi.gie.eu/api/data/BE",

"https://agsi.gie.eu/api/data/IT" )

 

$alsiurl = @("https://alsi.gie.eu/api/data/21W0000000000451/FR/21X000000001331I",

"https://alsi.gie.eu/api/data/63W179356656691A/FR/21X0000000010679",

"https://alsi.gie.eu/api/data/63W631527814486R/FR/21X0000000010679",

"https://alsi.gie.eu/api/data/63W943693783886F/FR/21X000000001070K",

"https://alsi.gie.eu/api/data/21W0000000001245/BE/21X000000001006T",

"https://alsi.gie.eu/api/data/21W0000000000079/NL/21X000000001063H",

"https://alsi.gie.eu/api/data/21W0000000000362/ES/21X000000001352A",

"https://alsi.gie.eu/api/data/21W000000000039X/ES/21X000000001254A",

"https://alsi.gie.eu/api/data/21W000000000038Z/ES/21X000000001254A",

"https://alsi.gie.eu/api/data/21W0000000000370/ES/21X000000001254A",

"https://alsi.gie.eu/api/data/21W0000000000338/ES/18XRGNSA-12345-V",

"https://alsi.gie.eu/api/data/21W0000000000354/ES/18XTGPRS-12345-G",

"https://alsi.gie.eu/api/data/21X000000001360B/IT/",

"https://alsi.gie.eu/api/data/59W0000000000011/IT/26X00000117915-0",

"https://alsi.gie.eu/api/data/21W0000000000443/IT/21X000000001109G",

"https://alsi.gie.eu/api/data/16WTGNL01------O/PT/21X0000000013619",

"https://alsi.gie.eu/api/data/21W000000000096L/PL/53XPL000000PLNG6",

"https://alsi.gie.eu/api/data/21W0000000001253/LT/21X0000000013740")

 

$Path = 'D:\Data\ALSI\'


 

Function AlsiDataExport($url , $Path)

{
 $TerminalFrom = (($url -split '/')[-3])
$CountryCode = (($url -split '/')[-2])
$TerminalTo = (($url -split '/')[-1])

$code = $TerminalFrom +"_"+$CountryCode +"_"+ $TerminalTo



$NewPath = $Path + "ALSI_"+ $code + ".csv"


$response = Invoke-RestMethod -v -H @{'x-key' = '6decec532c4f381d76377e1c37e0a0e8'} $url -Proxy http://proxy.gassco.no:4040/ 



$response | 

Select-Object *,@{Name='TerminalFrom';Expression = {$TerminalFrom}},@{Name='CountryCode';Expression = {$CountryCode}},@{Name='TerminalTo';Expression = {$TerminalTo}} | Export-Csv -Path $NewPath -NoTypeInformation -Force

}

Function AgsiDataExport($url , $Path, $code)
{
$response = Invoke-RestMethod -v -H @{'x-key' = '6decec532c4f381d76377e1c37e0a0e8'} $url -Proxy http://proxy.gassco.no:4040/ 
$response | Select-Object *,@{Name='CountryCode';Expression = {$code}}| Export-Csv  -Path $Path  -NoTypeInformation -Force
}

 

# AGSI Export

 

foreach ($url in $agsiurl)

{

 

$code = ($url -split '/')[-1]

$NewPath = $Path + "AGSI_"+ $code + ".csv"

AgsiDataExport $url $NewPath $code

}

 

# ALSI Export

 

foreach ($url in $alsiurl)

{



AlsiDataExport $url $Path

}

