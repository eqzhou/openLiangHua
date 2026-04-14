$ErrorActionPreference = 'Stop'

$root = Split-Path -Parent $PSScriptRoot
$python = Join-Path $root '.venv\Scripts\python.exe'
if (-not (Test-Path $python)) {
  $python = 'python'
}

Push-Location $root
try {
  & $python -m src.db.dashboard_sync
}
finally {
  Pop-Location
}
