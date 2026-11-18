$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")
if (Test-Path "bin") { Remove-Item -Recurse -Force "bin" }
