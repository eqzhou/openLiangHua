$ErrorActionPreference = 'Stop'

param(
  [string]$TargetSource = 'tushare',
  [string]$EndDate = ''
)

$root = Split-Path -Parent $PSScriptRoot
$envPath = Join-Path $root '.env'

if (-not (Test-Path $envPath)) {
  throw "Missing .env. Copy .env.example to .env and set TUSHARE_TOKEN first."
}

$envTokenLine = Get-Content $envPath |
  Where-Object { $_ -match '^\s*TUSHARE_TOKEN\s*=' } |
  Select-Object -First 1

if (-not $envTokenLine) {
  throw "TUSHARE_TOKEN is missing in .env."
}

$envToken = ($envTokenLine -split '=', 2)[1].Trim()
if (-not $envToken) {
  throw "TUSHARE_TOKEN in .env is empty."
}

$pythonCandidates = @(
  (Join-Path $root '.venv-tushare\Scripts\python.exe'),
  (Join-Path $root '.venv\Scripts\python.exe'),
  'python'
)

$python = $pythonCandidates | Where-Object { $_ -eq 'python' -or (Test-Path $_) } | Select-Object -First 1
if (-not $python) {
  throw "No Python interpreter found. Expected .venv-tushare, .venv, or python on PATH."
}

$arguments = @(
  '-m', 'src.data.tushare_workflows',
  '--mode', 'full',
  '--target-source', $TargetSource
)

if ($EndDate) {
  $arguments += @('--end-date', $EndDate)
}

Push-Location $root
try {
  $env:TUSHARE_TOKEN = $envToken
  & $python @arguments
}
finally {
  $env:TUSHARE_TOKEN = $null
  Pop-Location
}
