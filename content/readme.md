# eltSnap Runtime Setup

This solution contains the runtime engine used by both eltSnap and notebookSnap.

> Usage of this product confirms your agreement to the following 'Indemnification', and 'Liability disclaimer':

Indemnification: You agree to indemnify, defend and hold harmless BI Tracks, its officers, directors, employees, agents and third parties, for any losses, costs, liabilities and expenses relating to or arising out of your use of or inability to use BI Track's Services, Programs, or any related software services.

See: [Liability disclaimer](liability_disclaimer.md) for details

## System Requirements:
- Windows 10 or Windows Server 2016 (or later)
- SQL Server 2019 (or later) Express, Standard, or Enterprise Edition
- PowerShell Core 7.0 (or later)
    - PowerShell 'AZ' Module: [Link](https://docs.microsoft.com/en-us/powershell/azure/install-az-ps?view=azps-5.9.0)
- Azure Data Studio (recommended)
- Python 3.8 (or later recommended)

## Update User PATH Variable

Step 1 - Open the [Runtime Setup](runtime_setup.ipynb) (PowerShell) Notebook to append to the user PATH environment variable. This will allow you to launch the "eltSnap runtime" from _any_ folder on this machine.

Step 2 - Open the [Folder Settings](folder_settings.ipynb) (SQL) Notebook to Update the SQL Server (table) based Settings