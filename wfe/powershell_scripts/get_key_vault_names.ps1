[CmdletBinding()]

# example of script invocation
# ./get_key_vault_names

# return variables:
# 1. $global:exception - false if login succeeded, true if an exception was thrown
# 2. $global:all_key_vault_names - array of string values that represent all key vault names for the currently authenticated user

$global:exception = $false

# initialize an empty array of key vault names

$global:all_key_vault_names = @()

Try
{
    # check if the user is already logged in

    $allKeyVaults = Get-AzKeyVault

	# iterate through all key vaults
	
	foreach ($keyVault in $allKeyVaults) 
	{
	
		# add secret name into the list of secret names
		
		$global:all_key_vault_names += $keyVault.VaultName

	}

}
Catch
{
    $global:exception = $true
}