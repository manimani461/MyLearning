

$Stamp = (Get-Date).toString("ddMMyyyy")
$logfile = "D:\Code\Sources\Snam\log\SNAM_$Stamp.log"

Start-Transcript -Path $logfile -Append 

Function ExcelCSV ($FileName)

{
$pwd = "D:\Data\Snam\"


$excelFile = "$pwd" + $FileName + ".xls"
$Excel = New-Object -ComObject Excel.Application
$Excel.Visible = $false
$Excel.DisplayAlerts = $false
$wb = $Excel.Workbooks.Open($excelFile)
$count = 1
foreach ($ws in $wb.Worksheets)

    {

    $Name = $FileName + $ws.Name
    $ws.SaveAs("$pwd" + $Name + ".csv", 6)

        if ($count -eq 3)

        {
        Write-Output "True"

        Break

        }

        $count = $count + 1

    }

    $Excel.Quit()

}


$pwd = "D:\Data\Snam\*"

Remove-Item $pwd

# Get Months List

$beginDate=[datetime]::Parse("2018-11-01") 
$End = Get-Date -format "yyyy-MM-dd" 
$endDate = [datetime]::Parse( $End )

$monthdiff = $endDate.month - $beginDate.month + (($endDate.Year - $beginDate.year) * 12)

$Dates = New-Object System.Collections.ArrayList

$CountMonths = 1 
for($j = 0; $j -le $monthdiff; $j++)
{

$Yearmonth = $beginDate.AddMonths($j).tostring("yyyyMM")
$Dates.Add("$Yearmonth")

}


# For Each Xl File

Foreach($obj in $Dates )

{
$Yearmon = $obj
$Year = $obj.substring(0,4)
$url = "http://www.snam.it/export/sites/snam-rp/exchange/quantita_gas_trasportato/andamento/bilancio/" + "$Year" + "/bilancio_" + "$Yearmon" + "-EN.xls"
$FileName = "snam_" + $Yearmon
$Path = "D:\Data\Snam\snam_" + $Yearmon +".xls"
Invoke-WebRequest -Uri $url -OutFile $Path -Proxy http://proxy.gassco.no:4040/ 

ExcelCSV -File $FileName 

}

# Remove Header and Footer

Get-ChildItem 'D:\Data\Snam\*.csv' | 
        ForEach-Object {
            $filecontent = get-content $_ | select-object -skip 9;
            $filecontent | select -First $($filecontent.length -7) | Set-Content -Path $_;
        };

Stop-Transcript



