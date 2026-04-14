$ErrorActionPreference = "Stop"

$repo = "D:\openlianghua"
$stagingDir = Join-Path $repo "data\staging"
$statusFile = Join-Path $stagingDir "streamlit_status.json"
$supervisorPidFile = Join-Path $stagingDir "streamlit_supervisor.pid"
$streamlitPidFile = Join-Path $stagingDir "streamlit.pid"
$outLog = Join-Path $repo "logs\streamlit.out.log"
$errLog = Join-Path $repo "logs\streamlit.err.log"

function Read-LogTail {
    param(
        [string]$Path,
        [int]$Lines = 10
    )

    if (-not (Test-Path $Path)) {
        return @()
    }

    $fileStream = [System.IO.File]::Open($Path, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, [System.IO.FileShare]::ReadWrite)
    try {
        $reader = New-Object System.IO.StreamReader($fileStream)
        try {
            $content = $reader.ReadToEnd()
        } finally {
            $reader.Dispose()
        }
    } finally {
        $fileStream.Dispose()
    }

    if ([string]::IsNullOrWhiteSpace($content)) {
        return @()
    }

    return $content.Replace("`0", "").Split([Environment]::NewLine, [System.StringSplitOptions]::RemoveEmptyEntries) |
        Select-Object -Last $Lines
}

function Read-PidStatus {
    param([string]$Path)

    $pidState = [ordered]@{
        pid = $null
        running = $false
        stale = $false
    }

    if (-not (Test-Path $Path)) {
        return $pidState
    }

    $rawValue = (Get-Content -Path $Path -ErrorAction SilentlyContinue | Select-Object -First 1).Trim()
    $pidValue = 0
    if (-not [int]::TryParse($rawValue, [ref]$pidValue)) {
        $pidState.stale = $true
        Remove-Item -Path $Path -Force -ErrorAction SilentlyContinue
        return $pidState
    }

    $pidState.pid = $pidValue
    $pidState.running = $null -ne (Get-Process -Id $pidValue -ErrorAction SilentlyContinue)
    if (-not $pidState.running) {
        $pidState.stale = $true
        Remove-Item -Path $Path -Force -ErrorAction SilentlyContinue
    }

    return $pidState
}

function Get-ListenerPids {
    $listenerPidList = @()
    $listeners = netstat -ano | Select-String "LISTENING.*:8501|:8501.*LISTENING"
    foreach ($listener in $listeners) {
        $parts = $listener.ToString().Trim() -split "\s+"
        $pidToken = $parts[-1]
        if ($pidToken -match "^\d+$") {
            $listenerPidList += [int]$pidToken
        }
    }

    return @($listenerPidList | Select-Object -Unique)
}

function Test-ListenerOwnedByStreamlit {
    param(
        $StreamlitPid,
        [int[]]$ListenerPids
    )

    if ($null -eq $StreamlitPid) {
        return $false
    }

    if ($ListenerPids -contains [int]$StreamlitPid) {
        return $true
    }

    foreach ($listenerPid in $ListenerPids) {
        $processInfo = Get-CimInstance Win32_Process -Filter "ProcessId = $listenerPid" -ErrorAction SilentlyContinue
        if (($null -ne $processInfo) -and ([int]$processInfo.ParentProcessId -eq [int]$StreamlitPid)) {
            return $true
        }
    }

    return $false
}

function Write-EffectiveStatus {
    param(
        [string]$State,
        $SupervisorPid,
        $StreamlitPid,
        $LastStatus
    )

    $restartCount = 0
    $lastExitCode = 0
    if ($null -ne $LastStatus) {
        if ($null -ne $LastStatus.restart_count) {
            $restartCount = [int]$LastStatus.restart_count
        }
        if ($null -ne $LastStatus.last_exit_code) {
            $lastExitCode = [int]$LastStatus.last_exit_code
        }
    }

    $payload = [ordered]@{
        state = $State
        supervisor_pid = $SupervisorPid
        streamlit_pid = $StreamlitPid
        restart_count = $restartCount
        last_update = (Get-Date).ToString("s")
        last_exit_code = $lastExitCode
    }
    $payload | ConvertTo-Json | Set-Content -Path $statusFile -Encoding UTF8
}

$status = [ordered]@{
    supervisor_pid = $null
    streamlit_pid = $null
    supervisor_running = $false
    streamlit_running = $false
    listener_present = $false
    listener_pids = @()
    listener_matches_streamlit_pid = $false
    effective_state = "stopped"
    status_label = "stopped"
    stale_supervisor_pid = $false
    stale_streamlit_pid = $false
    stale_status = $false
    last_status = $null
    out_log_tail = @()
    err_log_tail = @()
}

$supervisorState = Read-PidStatus -Path $supervisorPidFile
$streamlitState = Read-PidStatus -Path $streamlitPidFile
$status.supervisor_pid = $supervisorState.pid
$status.supervisor_running = $supervisorState.running
$status.stale_supervisor_pid = $supervisorState.stale
$status.streamlit_pid = $streamlitState.pid
$status.streamlit_running = $streamlitState.running
$status.stale_streamlit_pid = $streamlitState.stale

if (Test-Path $statusFile) {
    $status.last_status = Get-Content -Path $statusFile -Raw -ErrorAction SilentlyContinue | ConvertFrom-Json
}

$status.listener_pids = Get-ListenerPids
$status.listener_present = $status.listener_pids.Count -gt 0
$status.listener_matches_streamlit_pid = Test-ListenerOwnedByStreamlit -StreamlitPid $status.streamlit_pid -ListenerPids $status.listener_pids

if ($status.supervisor_running -and $status.listener_present -and $status.listener_matches_streamlit_pid) {
    $status.effective_state = "running"
    $status.status_label = "running"
} elseif ($status.supervisor_running -and $status.listener_present) {
    $status.effective_state = "port_busy"
    $status.status_label = "port_busy"
} elseif ($status.supervisor_running) {
    $status.effective_state = "starting"
    $status.status_label = "starting"
} elseif ($status.listener_present) {
    $status.effective_state = "orphan_listener"
    $status.status_label = "listener_without_supervisor"
}

if (
    ($null -ne $status.last_status) -and
    ($status.last_status.state -in @("running", "starting", "restarting")) -and
    (-not $status.supervisor_running) -and
    (-not $status.listener_present)
) {
    $status.stale_status = $true
    Write-EffectiveStatus -State "stopped" -SupervisorPid $null -StreamlitPid $null -LastStatus $status.last_status
    $status.last_status = Get-Content -Path $statusFile -Raw -ErrorAction SilentlyContinue | ConvertFrom-Json
}

$status.out_log_tail = Read-LogTail -Path $outLog -Lines 10
$status.err_log_tail = Read-LogTail -Path $errLog -Lines 10

$status | ConvertTo-Json -Depth 5
