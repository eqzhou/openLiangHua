$ErrorActionPreference = "Stop"

$repo = "D:\openlianghua"
$stagingDir = Join-Path $repo "data\staging"
$supervisorPidFile = Join-Path $stagingDir "streamlit_supervisor.pid"
$streamlitPidFile = Join-Path $stagingDir "streamlit.pid"
$statusFile = Join-Path $stagingDir "streamlit_status.json"

function Read-PidFile {
    param([string]$Path)

    if (-not (Test-Path $Path)) {
        return $null
    }

    $rawValue = (Get-Content -Path $Path -ErrorAction SilentlyContinue | Select-Object -First 1).Trim()
    $pidValue = 0
    if (-not [int]::TryParse($rawValue, [ref]$pidValue)) {
        Remove-Item -Path $Path -Force -ErrorAction SilentlyContinue
        return $null
    }

    return $pidValue
}

function Stop-RunningProcess {
    param([int]$PidValue)

    $process = Get-Process -Id $PidValue -ErrorAction SilentlyContinue
    if ($null -eq $process) {
        return $false
    }

    Stop-Process -Id $PidValue -Force -ErrorAction SilentlyContinue
    return $true
}

function Get-ManagedListenerPids {
    $listenerPidList = @()
    $listeners = netstat -ano | Select-String "LISTENING.*:8501|:8501.*LISTENING"
    foreach ($listener in $listeners) {
        $parts = $listener.ToString().Trim() -split "\s+"
        $pidToken = $parts[-1]
        if (-not ($pidToken -match "^\d+$")) {
            continue
        }

        $pidValue = [int]$pidToken
        $processInfo = Get-CimInstance Win32_Process -Filter "ProcessId = $pidValue" -ErrorAction SilentlyContinue
        $commandLine = [string]($processInfo.CommandLine)
        if ($commandLine -match "streamlit_app\.py" -or $commandLine -match "-m\s+streamlit") {
            $listenerPidList += $pidValue
        }
    }

    return @($listenerPidList | Select-Object -Unique)
}

function Write-StoppedStatus {
    $payload = [ordered]@{
        state = "stopped"
        supervisor_pid = $null
        streamlit_pid = $null
        restart_count = 0
        last_update = (Get-Date).ToString("s")
        last_exit_code = 0
    }
    $payload | ConvertTo-Json | Set-Content -Path $statusFile -Encoding UTF8
}

$supervisorPid = Read-PidFile -Path $supervisorPidFile
$streamlitPid = Read-PidFile -Path $streamlitPidFile
$listenerPids = Get-ManagedListenerPids
$stoppedAny = $false

foreach ($pidValue in @($streamlitPid, $supervisorPid) + $listenerPids | Select-Object -Unique) {
    if ($null -eq $pidValue) {
        continue
    }
    $stoppedAny = (Stop-RunningProcess -PidValue ([int]$pidValue)) -or $stoppedAny
}

Remove-Item -Path $supervisorPidFile -Force -ErrorAction SilentlyContinue
Remove-Item -Path $streamlitPidFile -Force -ErrorAction SilentlyContinue
Write-StoppedStatus

if ($stoppedAny) {
    Write-Output "stopped"
} else {
    Write-Output "already_stopped"
}
