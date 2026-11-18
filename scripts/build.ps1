$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")
New-Item -ItemType Directory -Force -Path "bin" | Out-Null
go build -trimpath -o "bin\streamforge.exe" ".\cmd\streamforge"
go build -trimpath -o "bin\streamforge-cli.exe" ".\cmd\streamforge-cli"
