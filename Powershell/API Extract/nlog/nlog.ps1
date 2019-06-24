
Function ExcelCSV ($FileName)

{

    Add-Type -ASSEMBLY "Microsoft.Office.Interop.Excel"  | out-null

    $pwd = "D:\Data\nlog\"

    $excelFile = "$pwd" + $FileName + ".xls"

    $Excel = New-Object -ComObject Excel.Application

    $Excel.Visible = $false

    $Excel.DisplayAlerts = $false

    $wb = $Excel.Workbooks.Open($excelFile)

    foreach ($ws in $wb.Worksheets)

    {

        $ws.SaveAs("$pwd" + $FileName + ".csv", 6)

    }

    $Excel.Quit()

}


$pwd = "D:\Data\nlog\*"

Remove-Item $pwd

For($i = 0 ; $i -le 10; $i++ )

{
$CurrentYear = (get-date).year
$Year = $CurrentYear - $i
$url = "https://www.nlog.nl/nlog/requestData/prodfigures?object=field&product=Gas&production=Produced&year="+ $Year +"&location=Land&unit=Nm3"
$FileName = "nlog_" + $Year
$Path = "D:\Data\nlog\nlog_" + $Year +".xls"
Invoke-WebRequest -Uri $url -OutFile $Path -Proxy http://proxy.gassco.no:4040/ 

ExcelCSV -File $FileName

}








