[CmdletBinding()]

# example of script invocation
# ./get_all_secrets.ps1 -vault_name TestVaultEhli

# return variables:
# 1. $global:exception - false if getting all key vault secrets succeeded, true if an exception was thrown
# 2. $global:all_secret_names - a list of string values each containing a name of a secret from the specified key vault

Param(

[string] $vault_name

)

# the global prefix is used so that the C# app can get the variable values after the operation is done

$global:exception = $false

# initialize an empty array of key vault secret names

$global:all_secret_names = @()

$global:all_secret_types = @()

# try to get all key vault secrets

Try
{
	
	# get all information about all secrets from the specified key vault
	
	$all_secrets = Get-AzKeyVaultSecret -VaultName $vault_name
	
	# iterate through all key vault secrets
	
	foreach ($secret in $all_secrets) 
	{
	
		# add secret name into the list of secret names
		
		$global:all_secret_names += $secret.Name

	}
Write-Host $global:all_secret_names

}

# getting the secrets failed - abort operation

Catch
{

    $global:exception = $true

}