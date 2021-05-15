[CmdletBinding()]

# example of script invocation
# ./get_secret_value.ps1 -vault_name TestVaultEhli -secret_name Ehlimana

# return variables:
# 1. $global:exception - false if getting the secret value succeeded, true if an exception was thrown
# 2. $global:secret_value - a string value containing the value of the specified secret from the specified key vault

Param(

[string] $vault_name,

[string] $secret_name

)

# the global prefix is used so that the C# app can get the variable values after the operation is done

$global:exception = $false

# initialize an empty secret value

$global:secret_value = ""

# try to get key vault secret

Try
{
	
	# get secret value from key vault
	
	$global:secret_value = (Get-AzKeyVaultSecret -vaultName $vault_name -name $secret_name).SecretValueText

}


# getting the secret failed - abort operation

Catch
{

    $global:exception = $true

}