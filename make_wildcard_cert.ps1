$dns = @(
    '*.binance.com',
    'binance.com',
    'api.binance.com',
    'fapi.binance.com',
    'dapi.binance.com',
    'fstream.binance.com',
    'stream.binance.com',
    '*.bybit.com',
    'bybit.com',
    'api.bybit.com',
    'api-testnet.bybit.com',
    'stream.bybit.com',
    'localhost',
    '127.0.0.1'
)

$scriptDir = $PSScriptRoot
if (-not $scriptDir) { $scriptDir = "." }

$cert = New-SelfSignedCertificate -DnsName $dns -CertStoreLocation 'Cert:\CurrentUser\My' -NotAfter (Get-Date).AddYears(5)
$pwd = ConvertTo-SecureString -String 'cgtehnik' -Force -AsPlainText
$pfxPath = Join-Path $scriptDir 'gateway.pfx'
$crtPath = Join-Path $scriptDir 'gateway.crt'

Export-PfxCertificate -Cert $cert -FilePath $pfxPath -Password $pwd
Export-Certificate -Cert $cert -FilePath $crtPath

# Import to CurrentUser\Root so .NET and Windows trust it
certutil -user -addstore Root $crtPath
Write-Host "WILDCARD CERTIFICATE GENERATED AND TRUSTED FOR BINANCE AND BYBIT DOMAINS!"
