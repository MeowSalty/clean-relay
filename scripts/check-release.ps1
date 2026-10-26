#requires -Version 7.0
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$taskRoot = Split-Path -Parent $PSScriptRoot
$taskTempParent = [System.IO.Path]::GetFullPath([System.IO.Path]::GetTempPath())
$taskTemp = Join-Path $taskTempParent ('clean-relay-release-check-' + [guid]::NewGuid().ToString('N'))
$taskVersion = '0.0.0-ci'
$taskPreviousGOAMD64 = $env:GOAMD64

try {
    New-Item -ItemType Directory -Path $taskTemp | Out-Null
    # 构建不得覆盖或发布已有的本地配置。
    $taskLocalConfig = Join-Path $taskTemp 'clean-relay.json'
    Set-Content -LiteralPath $taskLocalConfig -Value 'local-config-sentinel' -NoNewline
    $env:GOAMD64 = 'v3'
    & (Join-Path $PSScriptRoot 'build.ps1') -Version $taskVersion -OutputDirectory $taskTemp
    if ($env:GOAMD64 -ne 'v3') { throw 'Build did not restore GOAMD64.' }
    if ((Get-Content -LiteralPath $taskLocalConfig -Raw) -cne 'local-config-sentinel') {
        throw 'Build modified the local configuration.'
    }

    $taskExecutable = Join-Path $taskTemp 'clean-relay.exe'
    $taskActualVersion = & $taskExecutable -version
    if ($LASTEXITCODE -ne 0 -or $taskActualVersion -cne "clean-relay $taskVersion") {
        throw 'Executable version injection failed.'
    }
    & $taskExecutable -config (Join-Path $taskTemp 'clean-relay.example.json') -check-config
    if ($LASTEXITCODE -ne 0) { throw 'Packaged example configuration is invalid.' }

    $taskBuildInfo = & go version -m $taskExecutable
    if ($LASTEXITCODE -ne 0) { throw 'Cannot inspect the executable.' }
    foreach ($taskSetting in @('GOOS=windows', 'GOARCH=amd64', 'GOAMD64=v1', 'CGO_ENABLED=0')) {
        if (-not ($taskBuildInfo | Select-String -SimpleMatch $taskSetting)) {
            throw "Missing release build setting: $taskSetting"
        }
    }

    $taskHash = (Get-FileHash -LiteralPath $taskExecutable -Algorithm SHA256).Hash.ToLowerInvariant()
    $taskChecksum = Get-Content -LiteralPath (Join-Path $taskTemp 'clean-relay.exe.sha256') -Raw
    if ($taskChecksum -cne "$taskHash  clean-relay.exe`n") { throw 'SHA256 does not match the executable.' }
    $taskExpectedReadme = Get-FileHash -LiteralPath (Join-Path $taskRoot 'docs/windows-usage.md')
    $taskPackagedReadme = Get-FileHash -LiteralPath (Join-Path $taskTemp 'README.md')
    if ($taskExpectedReadme.Hash -ne $taskPackagedReadme.Hash) { throw 'Packaged usage instructions are stale.' }

    Write-Output 'Windows release checks passed: version, configuration, build settings, SHA256 and usage instructions.'
}
finally {
    $env:GOAMD64 = $taskPreviousGOAMD64
    $taskResolvedTemp = [System.IO.Path]::GetFullPath($taskTemp)
    $taskResolvedParent = [System.IO.Path]::GetFullPath((Split-Path -Parent $taskResolvedTemp))
    if ($taskResolvedParent.TrimEnd('\', '/') -ne $taskTempParent.TrimEnd('\', '/') -or
        -not (Split-Path -Leaf $taskResolvedTemp).StartsWith('clean-relay-release-check-')) {
        throw 'Refusing to remove an unexpected temporary directory.'
    }
    if (Test-Path -LiteralPath $taskResolvedTemp) {
        Remove-Item -LiteralPath $taskResolvedTemp -Recurse -Force
    }
}
