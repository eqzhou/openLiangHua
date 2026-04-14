$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $false

$repo = "D:\openlianghua"
$python = Join-Path $repo ".venv\Scripts\python.exe"
$logDir = Join-Path $repo "logs"
$stagingDir = Join-Path $repo "data\staging"
$outLog = Join-Path $logDir "streamlit.out.log"
$errLog = Join-Path $logDir "streamlit.err.log"
$supervisorLog = Join-Path $logDir "streamlit.supervisor.log"
$supervisorPidFile = Join-Path $stagingDir "streamlit_supervisor.pid"
$streamlitPidFile = Join-Path $stagingDir "streamlit.pid"
$statusFile = Join-Path $stagingDir "streamlit_status.json"

if (-not (Test-Path $logDir)) {
    New-Item -ItemType Directory -Force -Path $logDir | Out-Null
}

if (-not (Test-Path $stagingDir)) {
    New-Item -ItemType Directory -Force -Path $stagingDir | Out-Null
}

function Read-RunningPid {
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

    if ($null -eq (Get-Process -Id $pidValue -ErrorAction SilentlyContinue)) {
        Remove-Item -Path $Path -Force -ErrorAction SilentlyContinue
        return $null
    }

    return $pidValue
}

function Write-SupervisorLog {
    param([string]$Message)
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Add-Content -Path $supervisorLog -Value "[$timestamp] $Message"
}

function Write-Status {
    param(
        [string]$State,
        [int]$RestartCount,
        [int]$ExitCode,
        $StreamlitPid = $null
    )

    $payload = [ordered]@{
        state = $State
        supervisor_pid = $PID
        streamlit_pid = $StreamlitPid
        restart_count = $RestartCount
        last_update = (Get-Date).ToString("s")
        last_exit_code = $ExitCode
    }
    $payload | ConvertTo-Json | Set-Content -Path $statusFile -Encoding UTF8
}

$existingSupervisorPid = Read-RunningPid -Path $supervisorPidFile
if ($null -ne $existingSupervisorPid) {
    Write-Output "already_running:$existingSupervisorPid"
    exit 0
}

$null = Read-RunningPid -Path $streamlitPidFile

Set-Location $repo
Set-Content -Path $supervisorPidFile -Value $PID

$arguments = @(
    "-m", "streamlit", "run", "streamlit_app.py",
    "--server.port", "8501",
    "--server.headless", "true",
    "--browser.gatherUsageStats", "false"
)

$restartCount = 0
Write-SupervisorLog "dashboard supervisor started"
Write-Status -State "starting" -RestartCount $restartCount -ExitCode 0 -StreamlitPid $null

while ($true) {
    $restartCount += 1
    Write-SupervisorLog "launch attempt $restartCount"
    Remove-Item -Path $streamlitPidFile -Force -ErrorAction SilentlyContinue
    Write-Status -State "starting" -RestartCount $restartCount -ExitCode 0 -StreamlitPid $null

    $process = Start-Process `
        -FilePath $python `
        -ArgumentList $arguments `
        -WorkingDirectory $repo `
        -RedirectStandardOutput $outLog `
        -RedirectStandardError $errLog `
        -WindowStyle Hidden `
        -PassThru

    Set-Content -Path $streamlitPidFile -Value $process.Id
    Write-Status -State "running" -RestartCount $restartCount -ExitCode 0 -StreamlitPid $process.Id

    $process.WaitForExit()
    $exitCode = [int]$process.ExitCode

    Write-SupervisorLog "streamlit exited with code $exitCode"
    Remove-Item -Path $streamlitPidFile -Force -ErrorAction SilentlyContinue
    Write-Status -State "restarting" -RestartCount $restartCount -ExitCode $exitCode -StreamlitPid $null
    Start-Sleep -Seconds 5
}
