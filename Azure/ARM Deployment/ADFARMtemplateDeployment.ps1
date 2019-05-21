
# Login Azure Rm Template

#Login-AzureRmAccount


$resourceGroupName = "AvanadeTest3"

$resourceGroupLocation = "West Europe"

$dataFactoryName = "TestAvandetask03"

$triggerName = "ArmTemplateTestTrigger"

$DeploymentName = "AzureDataFactoryBlobtoADLS"

$TemplateFile = "C:\Users\mabotula\Desktop\arm_template\AllinOne\arm_template.json"

$TemplateParameterFile = "C:\Users\mabotula\Desktop\arm_template\AllinOne\arm_template_parameters.json"

Get-AzureRmResourceGroup -Name $resourceGroupName -ErrorVariable notPresent -ErrorAction SilentlyContinue

if ($notPresent)
{
    Write-Output "Resource group does not exist, creating new Resource group : $resourceGroupName "
    New-AzureRmResourceGroup -Name $resourceGroupName -Location $resourceGroupLocation
    Write-Output "$resourceGroupName is created Now "
    Write-Output "========================================================================================"
    Write-Output "Proceeding with ARM Template Deployment"
}
else
{
    Write-Output "Resource group  exist, Resource Group Name :$resourceGroupName  "
    Write-Output "Proceeding with ARM Template Deployment"
}


New-AzureRmResourceGroupDeployment -Name $DeploymentName -ResourceGroupName $resourceGroupName -TemplateFile $TemplateFile -TemplateParameterFile $TemplateParameterFile


# Trigger 

#Get-AzureRmDataFactoryV2Trigger -ResourceGroupName $resourceGroupName -DataFactoryName $dataFactoryName -Name $triggerName

#Start-AzureRmDataFactoryV2Trigger -ResourceGroupName $resourceGroupName -DataFactoryName $dataFactoryName -TriggerName $triggerName

#Get-AzureRmDataFactoryV2Trigger -ResourceGroupName $resourceGroupName -DataFactoryName $dataFactoryName -TriggerName $triggerName


