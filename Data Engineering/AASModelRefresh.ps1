
$AzureCred = Get-AutomationPSCredential -Name "AASRefreshCredential"
Add-AzureAnalysisServicesAccount -RolloutEnvironment 'eastus2.asazure.windows.net' -ServicePrincipal -Credential $AzureCred -TenantId "b223ef84-eb37-44e0-8ef9-6b666a35bdce"
Invoke-ProcessASDatabase -server "asazure://aspaaseastus2.asazure.windows.net/itbissasdev" -DatabaseName "RQI_Insights" -RefreshType Full
