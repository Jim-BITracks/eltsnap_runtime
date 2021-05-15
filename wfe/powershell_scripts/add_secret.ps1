[CmdletBinding()]

# example of script invocation
# ./add_secret.ps1 -vault_name TestVaultEhli -secret_name Ehlimana -secret_value SomeValue -content_type text

# return variables:
# 1. $global:exception - false if adding the secret succeeded, true if an exception was thrown

Param(

[string] $vault_name,

[string] $secret_name,

[string] $secret_value,

[string] $content_type

)

# the global prefix is used so that the C# app can get the variable values after the operation is done

$global:exception = $false

# try to add a secret to the key vault

Try
{
	
	# convert the specified value to a secure string prior to its adding to the key vault
	
	$secure_secret_value = ConvertTo-SecureString $secret_value -AsPlainText -Force
	
	# add the specified secret to the key vault

    Set-AzKeyVaultSecret -VaultName $vault_name -Name $secret_name -SecretValue $secure_secret_value -ContentType $content_type

}


# adding the secret failed - abort operation

Catch
{

    $global:exception = $true

}