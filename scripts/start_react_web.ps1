$ErrorActionPreference = 'Stop'

$root = Split-Path -Parent $PSScriptRoot
$web = Join-Path $root 'web'

if (-not (Test-Path (Join-Path $web 'package.json'))) {
  throw "React frontend not found at $web"
}

Push-Location $web
try {
  npm run dev
}
finally {
  Pop-Location
}
