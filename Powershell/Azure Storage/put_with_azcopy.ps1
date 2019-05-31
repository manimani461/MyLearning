# Connect to Azure
Connect-AzAccount
# List Azure Subscriptions
Get-AzSubscription

# Define Variables
$subscriptionId = "70addefa-28b9-4fe7-a3f3-1b5d032d00df"
$storageAccountRG = "analytics-dev-shared-rg"
$storageAccountName = "badevadlsg2"
$storageContainerName = "adlsdev"
$localPath = "C:\Users\noa_mab2\Documents\Data\QAD\sct_det.csv"

# Select right Azure Subscription
Select-AzSubscription -SubscriptionId $SubscriptionId

# Get Storage Account Key
$storageAccountKey = (Get-AzStorageAccountKey -ResourceGroupName $storageAccountRG -AccountName $storageAccountName).Value[0]

# Set AzStorageContext
$destinationContext = New-AzStorageContext -StorageAccountName $storageAccountName -StorageAccountKey $storageAccountKey

# Generate SAS URI
$containerSASURI = New-AzStorageContainerSASToken -Context $destinationContext -ExpiryTime(get-date).AddSeconds(3600) -FullUri -Name $storageContainerName -Permission rw

# Upload File using AzCopy
. C:\Users\noa_mab2\Downloads\azcopy_windows_amd64_10.1.2\azcopy_windows_amd64_10.1.2\azcopy copy $localPath $containerSASURI