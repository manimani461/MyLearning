# Rolf A. Vaglid August 2018
# konrav@gassco.no / rolf.vaglid@evry.com

# Liste over URLer å laste ned
$list = Get-Content -path 'D:\Code\Sources\NPD.old\urls.txt'

# Working dir, altså hvor filene skal lastes ned til
$pwd  = "D:\Data\NPD.old\"

# Log file
$logfile = "D:\Code\Sources\NPD.old\NPD.log"

Function Write-Log {
    [CmdletBinding()]
    Param(
    [Parameter(Mandatory=$False)]
    [ValidateSet("INFO","WARN","ERROR","FATAL","DEBUG")]
    [String]
    $Level = "INFO",

    [Parameter(Mandatory=$True)]
    [string]
    $Message,

    [Parameter(Mandatory=$False)]
    [string]
    $logfile
    )

    $Stamp = (Get-Date).toString("dd/MM/yyyy HH:mm:ss")
    $Line = "$Stamp $Level $Message"
    If($logfile) {
        Add-Content $logfile -Value $Line
    }
    Else {
        Write-Output $Line
    }
}

# Looper gjennom listen med URLer, ei linje om gangen
foreach($url in $list) 
{ 
    # Sanity check, listen med URLer inneholder både filer med spesifikke filnavn, men også uten.
    if($url -match ".zip")
    {
        $filename = [System.IO.Path]::GetFileName($url)
    } 
    # Hvis fila ikke inneholder filendelse, men angir Format=CSV, da antar vi at fila er ei CSV-fil.
    elseif($url -match "Format=CSV") 
    {
        # Black magic for å finne delen av URLen som er selve filnavnet (uten .csv)
        # http://factpages.npd.no/ReportServer?/FactPages/TableView/company_reserves&rs:Command=REnder&rc:Toolbar=false&rc:Parameters=f&rs:Form
        # at=CSV&Top100=false&IpAddress=62.92.15.134&CultureCode=nb-no
        # blir altså company_reserves.csv
        $filename = $url.substring($url.lastindexof("/")+1,($url.indexof("&"))-($url.lastindexof("/")+1)) + ".csv"
    }
    else 
    {   # Verken CSV eller .zip. Hmm... Bryter og fortsetter på neste URL
        Write-Host "Unknown URL: $url"
        continue
    }

    # Den fulle stien til målfil består av workingdir + filnavn (f.eks c:\Temp\OD + blkArea.zip)
    $file = $pwd + $filename

    try 
    {   # Lager WebRequest som sjekker om URL er tilgjengelig (altså gir StatusCode == 200).
	
	# This causes webserver to spit out http status 500.
        # $req = Invoke-WebRequest -uri $url -DisableKeepAlive -Method Head -ErrorAction Stop -TimeoutSec 15 -UseBasicParsing -Proxy http://proxy.gassco.no:4040/
        #Write-Log "OK: " $url
        #Write-Host $req.StatusCode

        # URL er OK, utfører selve nedlastingen av fila til temp.fil
        $req = Invoke-WebRequest -uri $url -DisableKeepAlive -ErrorAction Stop -TimeoutSec 15 -UseBasicParsing -OutFile $file -Proxy http://proxy.gassco.no:4040/ 
	Write-Log INFO "$url : OK" $logfile
	#Start-Sleep 1000
    }
    # Hvis noe $req.StatusCode ikke er 200, så gir vi en exception og viser feilmeldingen.
    catch
    {
	Write-Log INFO "$url : FAIL" $logfile
        Write-Log INFO $_.exception $logfile
	#Send-MailMessage -to "Thomas Djønne <tdj@gassco.no>" -From "BYSQLT06 <help@gassco.no>" -Subject "Error occurred downloading file from NPD" -Body "The following error occurred. Please have a look at the attached log file. `n$_`n" -Attachments "D:\temp\Gassco\NPD\NPD.log" -SmtpServer smtpi.gassco.no
    }

    # Hvis fila er .zip, pakk ut.
    if($file -match ".zip")
    {
        Expand-Archive -Path $file -DestinationPath $pwd -Force -WarningVariable w
	# Hvis noe galt skjer under unzipping.
	if($w.Count -gt 0)
	{
		Write-Log INFO "Failed to unzip $file". $logfile
		#Send-MailMessage -to "Thomas Djønne <tdj@gassco.no>" -From "BYSQLT06 <help@gassco.no>" -Subject "Error occurred unzipping $file from NPD" -Body "An error occurred unzipping $file. `nPlease have a look at the attached log file.`n" -Attachments "C:\ProgramData\Gassco\NPD\NPD.log" -SmtpServer smtpi.gassco.no
 	}
	else
	{
		# Hvis fila ble unzipped successfully, sletter man zip-fila
		remove-item $file
	}
    } 
}

