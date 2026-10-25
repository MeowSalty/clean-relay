[CmdletBinding()]
param(
    [ValidatePattern('^[0-9A-Za-z][0-9A-Za-z.+_-]{0,63}$')]
    [string]$Version = '0.1.0-dev'
)

$ErrorActionPreference = 'Stop'
$taskRoot = Split-Path -Parent $PSScriptRoot
$taskDist = Join-Path $taskRoot 'dist'
$taskExecutable = Join-Path $taskDist 'clean-relay.exe'
$taskPreviousGOOS = $env:GOOS
$taskPreviousGOARCH = $env:GOARCH
$taskPreviousCGO = $env:CGO_ENABLED

Push-Location -LiteralPath $taskRoot
try {
    $env:GOOS = 'windows'
    $env:GOARCH = 'amd64'
    $env:CGO_ENABLED = '0'
    New-Item -ItemType Directory -Path $taskDist -Force | Out-Null
    & go build -trimpath -ldflags "-s -w -X main.version=$Version" -o $taskExecutable ./cmd/clean-relay
    if ($LASTEXITCODE -ne 0) {
        throw 'CleanRelay build failed.'
    }
    Copy-Item -LiteralPath (Join-Path $taskRoot 'clean-relay.example.json') -Destination $taskDist -Force
    Copy-Item -LiteralPath (Join-Path $taskRoot 'docs/windows-usage.md') -Destination (Join-Path $taskDist 'README.md') -Force
    $taskHash = (Get-FileHash -LiteralPath $taskExecutable -Algorithm SHA256).Hash.ToLowerInvariant()
    [System.IO.File]::WriteAllText(
        (Join-Path $taskDist 'clean-relay.exe.sha256'),
        "$taskHash  clean-relay.exe`n",
        [System.Text.UTF8Encoding]::new($false)
    )
    Write-Output "Built Windows amd64 CleanRelay $Version in $taskDist"
}
finally {
    $env:GOOS = $taskPreviousGOOS
    $env:GOARCH = $taskPreviousGOARCH
    $env:CGO_ENABLED = $taskPreviousCGO
    Pop-Location
}
