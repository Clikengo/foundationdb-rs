$Version="6.0.15"
$BaseUrl="https://www.foundationdb.org/downloads/${Version}"

Invoke-WebRequest "${BaseUrl}/windows/installers/foundationdb-${Version}-x64.msi" -OutFile "foundationdb-${Version}-x64.msi"
Write-Host "Installing foundationdb-${Version}-x64.msi"
msiexec /i "foundationdb-${Version}-x64.msi" /quiet /passive /norestart /log install.log | Out-Null
