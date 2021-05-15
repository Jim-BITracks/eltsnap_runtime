[CmdletBinding()]

# example of script invocation
# ./azure_authenticate.ps1 -login Y (force login the user)
# ./azure_authenticate.ps1 -login N (login the user only if they are not already logged in)

# return variables:
# 1. $global:exception - false if login succeeded, true if an exception was thrown

Param(

[string] $login

)

$global:exception = $false

Try
{
    # check if the user is already logged in

    $content = Get-AzContext

    $userLoggedIn = !([string]::IsNullOrEmpty($content.Account))

    # the user is already logged in and they need to login again - logout first

    if ($userLoggedIn -and $login -eq "Y")
    {
        Disconnect-AzAccount
    }

    # login the user in all cases except when user is logged in and does not want to login again

    if (!($userLoggedIn -and !($login -eq "Y")))
    {
        Enable-AzContextAutosave

        Connect-AzAccount

    }   

    # check if the user is logged in successfully

    $content = Get-AzContext

    $global:exception = ([string]::IsNullOrEmpty($content.Account))
	$global:exceptionMessage = "OK"

}
Catch
{
    $global:exception = $true
	$e = $_.Exception
	$global:exceptionMessage = $e.Message
}