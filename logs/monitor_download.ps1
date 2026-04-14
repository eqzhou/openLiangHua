$pidFile = 'D:\openlianghua\data\staging\akshare_download.pid'
$rawDir = 'D:\openlianghua\data\raw\akshare'
$stdout = 'D:\openlianghua\logs\akshare_download.out.log'
$stderr = 'D:\openlianghua\logs\akshare_download.err.log'
while ($true) {
    Clear-Host
    Write-Host 'AKShare Download Monitor' -ForegroundColor Cyan
    Write-Host ('Time: ' + (Get-Date))
    if (Test-Path $pidFile) {
        $pidValue = (Get-Content $pidFile | Select-Object -First 1).Trim()
        Write-Host ('PID: ' + $pidValue)
        $proc = Get-Process -Id $pidValue -ErrorAction SilentlyContinue
        Write-Host ('Running: ' + [bool]$proc)
    } else {
        Write-Host 'PID file not found'
    }
    $count = 0
    if (Test-Path $rawDir) {
        $count = (Get-ChildItem $rawDir -Filter *.parquet -ErrorAction SilentlyContinue | Measure-Object).Count
    }
    Write-Host ('Cached symbols: ' + $count)
    Write-Host ''
    Write-Host 'Last stderr lines:' -ForegroundColor Yellow
    if (Test-Path $stderr) {
        Get-Content $stderr -Tail 20 -ErrorAction SilentlyContinue
    } else {
        Write-Host '(no stderr log yet)'
    }
    Write-Host ''
    Write-Host 'Refreshing every 5 seconds. Close this window when you no longer need it.' -ForegroundColor DarkGray
    Start-Sleep -Seconds 5
}
