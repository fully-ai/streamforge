$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")
if (-not (Test-Path "bin\streamforge.exe")) { & (Join-Path $PSScriptRoot "build.ps1") }
& ".\bin\streamforge.exe"
