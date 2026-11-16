# Enables Storage Manager v1 in an existing config/config.yaml, without
# touching the credentials or replacing the user's YAML with an example.
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$path = Join-Path $root 'config\config.yaml'
if (-not (Test-Path -LiteralPath $path -PathType Leaf)) { throw "Missing config: $path" }
$text = [System.IO.File]::ReadAllText($path, [System.Text.Encoding]::UTF8)
$sectionPattern = '(?m)^storage:[ \t]*(?:\r?\n|$)(?:^[ \t]+[^\r\n]*(?:\r?\n|$))*'
$match = [regex]::Match($text, $sectionPattern)
if (-not $match.Success) { throw 'storage: section not found in config/config.yaml' }
$section = $match.Value
$fields = @(
    @('enabled', 'true'),
    @('transfer_transport', '"websocket"'),
    @('websocket_binary_file_transfer', 'true')
)
foreach ($field in $fields) {
    $name = $field[0]
    $value = $field[1]
    $rx = '(?m)^([ \t]+' + [regex]::Escape($name) + ':[ \t]*)[^\r\n#]*(?:#([^\r\n]*))?(\r?)$'
    if (-not [regex]::IsMatch($section, $rx)) { throw "Missing storage setting: $name" }
    $section = [regex]::Replace($section, $rx, {
        param($m)
        $comment = if ($m.Groups[2].Success) { ' #'+$m.Groups[2].Value } else { '' }
        $m.Groups[1].Value + $value + $comment + $m.Groups[3].Value
    })
}
if ($section -ne $match.Value) {
    $backup = "$path.bak-storage-$(Get-Date -Format 'yyyyMMdd-HHmmss')"
    [System.IO.File]::Copy($path, $backup)
    $updated = $text.Substring(0, $match.Index) + $section + $text.Substring($match.Index + $match.Length)
    [System.IO.File]::WriteAllText($path, $updated, [System.Text.UTF8Encoding]::new($false))
    Write-Host "[CONFIG] Enabled storage; backup: $backup" -ForegroundColor Green
} else {
    Write-Host '[CONFIG] Storage already enabled' -ForegroundColor Green
}
New-Item -ItemType Directory -Path (Join-Path $root 'data\files') -Force | Out-Null
Write-Host '[NEXT] go run . init-db' -ForegroundColor Cyan
