$ErrorActionPreference = "Stop"
if (-not (Get-Command go -ErrorAction SilentlyContinue)) { Write-Error "Go 1.24+ required."; exit 1 }
Set-Location (Join-Path $PSScriptRoot "..")
& (Join-Path $PSScriptRoot "build.ps1")
