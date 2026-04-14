$ErrorActionPreference = 'Stop'

$root = Split-Path -Parent $PSScriptRoot
$python = Join-Path $root '.venv\Scripts\python.exe'
if (-not (Test-Path $python)) {
  $python = 'python'
}

$apiPort = 8001

Get-NetTCPConnection -LocalPort $apiPort -ErrorAction SilentlyContinue |
  Select-Object -ExpandProperty OwningProcess -Unique |
  Where-Object { $_ -and $_ -gt 0 } |
  ForEach-Object {
    Stop-Process -Id $_ -Force -ErrorAction SilentlyContinue
  }

Push-Location $root
try {
  & $python -m uvicorn src.web_api.app:app --host 0.0.0.0 --port $apiPort
}
finally {
  Pop-Location
}
