$scriptDir = $PSScriptRoot
if (-not $scriptDir) { $scriptDir = "." }

$cert = Get-ChildItem Cert:\CurrentUser\My | Where-Object { $_.Subject -match 'fapi.binance.com' } | Select-Object -First 1
if ($cert) {
    $rsaKey = [System.Security.Cryptography.X509Certificates.RSACertificateExtensions]::GetRSAPrivateKey($cert)
    $keyBytes = $rsaKey.ExportPkcs8PrivateKey()
    $keyB64 = [System.Convert]::ToBase64String($keyBytes, 'InsertLineBreaks')
    $keyPem = "-----BEGIN PRIVATE KEY-----`n" + $keyB64 + "`n-----END PRIVATE KEY-----"
    [System.IO.File]::WriteAllText((Join-Path $scriptDir 'gateway.key'), $keyPem)

    $certBytes = $cert.Export([System.Security.Cryptography.X509Certificates.X509ContentType]::Cert)
    $certB64 = [System.Convert]::ToBase64String($certBytes, 'InsertLineBreaks')
    $certPem = "-----BEGIN CERTIFICATE-----`n" + $certB64 + "`n-----END CERTIFICATE-----"
    [System.IO.File]::WriteAllText((Join-Path $scriptDir 'gateway.crt'), $certPem)
    Write-Host "KEY AND CERT PEM WRITTEN SUCCESSFULLY!"
} else {
    Write-Host "Certificate not found in Cert:\CurrentUser\My!"
}
