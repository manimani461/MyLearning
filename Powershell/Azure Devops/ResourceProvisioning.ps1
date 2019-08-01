

param(

 [Parameter(Mandatory=$True)]
 [string]
 $serviceprincipleId,

 [Parameter(Mandatory=$True)]
 [string]
 $serviceprinciplekey,

  [Parameter(Mandatory=$True)]
 [string]
 $subscriptionId,

 [Parameter(Mandatory=$True)]
 [string]
 $tenantId,

 [Parameter(Mandatory=$True)]
 [string]
 $resourceGroupName,

 [Parameter(Mandatory=$True)]
 [string]
 $resourceGroupLocation,

  [Parameter(Mandatory=$True)]
 [string]
 $dataFactoryName,

   [Parameter(Mandatory=$True)]
 [string]
 $Deploymentenvironment,
 
 [Parameter(Mandatory=$True)]
 [string]
 $AASName,

 [Parameter(Mandatory=$True)]
 [string]
 $KeyVaultName,

 [Parameter(Mandatory=$True)]
 [string]
 $sqlserverName,

 [Parameter(Mandatory=$True)]
 [string]
 $adminSqlLogin,

 [Parameter(Mandatory=$True)]
 [string]
 $adminSqlpassword,

 [Parameter(Mandatory=$True)]
 [string]
 $databaseName
 
)


$Tags = @{ Audience="Internal"; BusinessOwner="runar.wigestrand@laerdal.com";Creator = "Goutham.Kumar.Tirumala@laerdal.com";Description= "Business Analytics Platform Services";LifeSpan = "Permanent" }

# Connect to Azure account

$passwd = ConvertTo-SecureString $serviceprinciplekey -AsPlainText -Force
$pscredential = New-Object System.Management.Automation.PSCredential( $serviceprincipleId, $passwd)
Connect-AzAccount -ServicePrincipal -Credential $pscredential -TenantId $tenantId

# Create Data Factory

try
{

Write-Host " Deployment Started to $Deploymentenvironment..."

$DataFactory = Get-AzDataFactoryV2 -ResourceGroupName $resourceGroupName -Name $dataFactoryName -ErrorAction SilentlyContinue

if (-not $DataFactory)
{

Write-Host " Creating New Data Fatory V2 : $dataFactoryName in  $resourceGroupName"

New-AzDataFactoryV2 -ResourceGroupName $resourceGroupName -Location  $resourceGroupLocation -Name $dataFactoryName -Tag $Tags
}
else 
{
Write-Host "Data Fatory V2 : $dataFactoryName  is already present in  $resourceGroupName"
}

$AAS = Get-AzAnalysisServicesServer -ResourceGroupName $resourceGroupName -Name $AASName -ErrorAction SilentlyContinue

if (-not $AAS)
{
Write-Host " Creating New Azure Analysis Service : $AASName in  $resourceGroupName"
New-AzAnalysisServicesServer -ResourceGroupName $resourceGroupName -Location  $resourceGroupLocation -Name $AASName -Sku D1 -Administrator "Runar.Wigestrand@laerdal.com" -Tag $Tags
}
else 
{
Write-Host "Azure Analysis Service : $AASName  is already present  in   $resourceGroupName"
}


$KeyVault = Get-AzKeyVault -ResourceGroupName $resourceGroupName -Name $KeyVaultName -ErrorAction SilentlyContinue

if (-not $KeyVault)
{
Write-Host " Creating New Azure Key Vault : $KeyVaultName in  $resourceGroupName"
New-AzKeyVault -Name $KeyVaultName -ResourceGroupName $resourceGroupName -Location $resourceGroupLocation -Tag $Tags 
}
else 
{
Write-Host "Azure Key Vault : $KeyVaultName  is already present  in   $resourceGroupName"
}


$sqlserver = Get-AzSqlServer -ResourceGroupName $resourceGroupName -Name $sqlserverName -ErrorAction SilentlyContinue

if (-not $sqlserver)
{
Write-Host " Creating New Azure sql server : $sqlserverName in  $resourceGroupName"
$sqlserv = New-AzSqlServer -ResourceGroupName $resourceGroupName -ServerName $sqlserverName -Location $resourceGroupLocation -SqlAdministratorCredentials $(New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList $adminSqlLogin, $(ConvertTo-SecureString -String $adminSqlpassword -AsPlainText -Force)) -Tags $Tags

if($?)
{
Write-Host " Creating New Azure sql Data Base : $databaseName in $sqlserverName server  in  $resourceGroupName"
New-AzSqlDatabase  -ResourceGroupName $resourceGroupName -ServerName $sqlserverName -DatabaseName $databaseName -Tags $Tags

}
}
else 
{
Write-Host "Azure sql server : $sqlserverName is already present  in   $resourceGroupName"

$sqldatabase = Get-AzSqlDatabase -ResourceGroupName $resourceGroupName -Name $databaseName -ServerName $sqlserverName -ErrorAction SilentlyContinue

if (-not $sqldatabase)
{
Write-Host " Creating New Azure sql Data Base :  $databaseName in $sqlserverName server  in  $resourceGroupName"
New-AzSqlDatabase  -ResourceGroupName $resourceGroupName -ServerName $sqlserverName -DatabaseName $databaseName  -Tags $Tags
}
else 
{
Write-Host "Azure sql Data Base :  $databaseName in $sqlserverName server  in  $resourceGroupName already present"
}

}

}

catch
{

Write-Host " Deployment Failed  ...!!!"
write-Host "$_.Exception.Message"

}
