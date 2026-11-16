# collect-app.ps1
# HomeCloudBackend project collector (Windows PowerShell 5.1 / PowerShell 7+)
# Run this script from anywhere: the source root is ALWAYS this script's directory.
# .git, .idea and previous <ProjectName>-export-*.txt are excluded.
# The script itself and binary files are listed in the tree, but their contents
# are NOT exported. Other files are exported as readable UTF text.
# Output is saved inside the project; prior exports are excluded from new exports.

param(
    [string]$OutputDirectory = ""
)

$ErrorActionPreference = 'Stop'

if ([string]::IsNullOrWhiteSpace($PSScriptRoot)) {
    throw 'The collector must be saved as collect-app.ps1 in the project root.'
}

$ProjectRoot = [System.IO.Path]::GetFullPath($PSScriptRoot)
$CollectorFullPath = [System.IO.Path]::GetFullPath($PSCommandPath)
$ProjectName = [System.IO.Path]::GetFileName($ProjectRoot.TrimEnd([char[]]@('\', '/')))
if ([string]::IsNullOrWhiteSpace($ProjectName)) { $ProjectName = 'project' }

if ([string]::IsNullOrWhiteSpace($OutputDirectory)) {
    $OutputDirectory = $ProjectRoot
}

$OutputDirectory = [System.IO.Path]::GetFullPath($OutputDirectory)
[void][System.IO.Directory]::CreateDirectory($OutputDirectory)

$Timestamp = Get-Date -Format 'yyyy-MM-dd_HH-mm-ss_fff'
$OutputFile = Join-Path $OutputDirectory "$ProjectName-export-$Timestamp.txt"
$OutputFullPath = [System.IO.Path]::GetFullPath($OutputFile)

$script:Entries = New-Object 'System.Collections.Generic.List[object]'
$script:Errors = New-Object 'System.Collections.Generic.List[string]'
$script:ExcludedEntries = 0
$script:TextFiles = 0
$script:SkippedBinaryFiles = 0
$script:SkippedSelfFiles = 0
$script:Symlinks = 0
$script:ReadErrors = 0
$script:TotalInputBytes = [long]0
$script:Writer = $null
$Utf8NoBom = [System.Text.UTF8Encoding]::new($false)
$Utf8Strict = [System.Text.UTF8Encoding]::new($false, $true)

# Known binary extensions: listed in the tree, never included in FILE CONTENTS.
$KnownBinaryExtensions = @(
    '.exe', '.dll', '.so', '.dylib', '.a', '.lib', '.o', '.obj', '.pdb',
    '.db', '.sqlite', '.sqlite3', '.db3', '.bin', '.dat', '.class', '.pyc',
    '.png', '.jpg', '.jpeg', '.gif', '.webp', '.ico', '.bmp', '.tif', '.tiff',
    '.avif', '.heic', '.svgz', '.ttf', '.otf', '.woff', '.woff2', '.eot',
    '.pdf', '.mp3', '.mp4', '.m4a', '.mkv', '.mov', '.webm', '.avi', '.wav',
    '.flac', '.ogg', '.opus', '.zip', '.7z', '.rar', '.tar', '.gz', '.bz2',
    '.xz', '.zst', '.jar', '.apk', '.wasm', '.iso', '.img', '.lockb'
)

function Add-Entry {
    param([string]$Kind, [System.IO.FileSystemInfo]$Item, [string]$RelativePath)
    [void]$script:Entries.Add([pscustomobject]@{
        Kind = $Kind
        FullName = $Item.FullName
        RelativePath = ($RelativePath -replace '\\', '/')
        Length = $(if ($Kind -eq 'FILE') { ([System.IO.FileInfo]$Item).Length } else { [long]0 })
    })
}

function Visit-Directory {
    param([string]$DirectoryPath, [string]$RelativePrefix)

    try {
        $items = @(Get-ChildItem -LiteralPath $DirectoryPath -Force -ErrorAction Stop | Sort-Object Name)
    } catch {
        [void]$script:Errors.Add("DIRECTORY: $RelativePrefix :: $($_.Exception.Message)")
        return
    }

    foreach ($item in $items) {
        # Only these directory names and earlier export files are absent from the tree.
        if (
            $item.Name -ieq '.git' -or
            $item.Name -ieq '.idea' -or
            (-not $item.PSIsContainer -and $item.Name -ilike "$ProjectName-export-*.txt")
        ) {
            $script:ExcludedEntries++
            continue
        }

        $rel = if ([string]::IsNullOrEmpty($RelativePrefix)) {
            $item.Name
        } else {
            "$RelativePrefix/$($item.Name)"
        }

        # Never follow junctions/symlinks: otherwise we could leave the project
        # tree or recurse forever. The link itself is represented in the report.
        if (($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
            Add-Entry -Kind 'LINK' -Item $item -RelativePath $rel
            continue
        }

        if ($item.PSIsContainer) {
            Add-Entry -Kind 'DIR' -Item $item -RelativePath $rel
            Visit-Directory -DirectoryPath $item.FullName -RelativePrefix $rel
        } else {
            # If an export is deliberately placed in the project, do not read
            # the file we are writing right now (infinite growth).
            if ([string]::Equals($item.FullName, $OutputFullPath, [System.StringComparison]::OrdinalIgnoreCase)) {
                continue
            }
            Add-Entry -Kind 'FILE' -Item $item -RelativePath $rel
        }
    }
}

function Get-ContentKind {
    param([string]$FilePath)

    $extension = [System.IO.Path]::GetExtension($FilePath).ToLowerInvariant()
    if ($KnownBinaryExtensions -contains $extension) { return 'BINARY' }

    $fileStream = $null
    try {
        $fileStream = [System.IO.FileStream]::new(
            $FilePath,
            [System.IO.FileMode]::Open,
            [System.IO.FileAccess]::Read,
            [System.IO.FileShare]::ReadWrite
        )

        # Scan the ENTIRE file, not merely the first 64 KiB. File contents
        # might look like text initially and contain binary data later.
        $buffer = [byte[]]::new(65536)
        $charBuffer = [char[]]::new(65540)
        $decoder = $Utf8Strict.GetDecoder()
        $firstChunk = $true

        while (($count = $fileStream.Read($buffer, 0, $buffer.Length)) -gt 0) {
            if ($firstChunk) {
                $firstChunk = $false
                # UTF-16/UTF-32 files with BOM are readable text; the text
                # reader below detects their encodings automatically.
                if ($count -ge 4) {
                    if ($buffer[0] -eq 0xFF -and $buffer[1] -eq 0xFE -and $buffer[2] -eq 0 -and $buffer[3] -eq 0) { return 'TEXT' }
                    if ($buffer[0] -eq 0 -and $buffer[1] -eq 0 -and $buffer[2] -eq 0xFE -and $buffer[3] -eq 0xFF) { return 'TEXT' }
                }
                if ($count -ge 2) {
                    if ($buffer[0] -eq 0xFF -and $buffer[1] -eq 0xFE) { return 'TEXT' }
                    if ($buffer[0] -eq 0xFE -and $buffer[1] -eq 0xFF) { return 'TEXT' }
                }
            }

            for ($i = 0; $i -lt $count; $i++) {
                $b = $buffer[$i]
                if ($b -eq 0) { return 'BINARY' }
                if ($b -lt 32 -and $b -ne 9 -and $b -ne 10 -and $b -ne 12 -and $b -ne 13) {
                    return 'BINARY'
                }
            }

            try {
                # A Decoder remembers incomplete UTF-8 sequences at chunk
                # boundaries; strict decoding rejects invalid bytes.
                [void]$decoder.GetChars($buffer, 0, $count, $charBuffer, 0, $false)
            } catch [System.Text.DecoderFallbackException] {
                return 'BINARY'
            }
        }

        try {
            [void]$decoder.GetChars([byte[]]::new(0), 0, 0, $charBuffer, 0, $true)
            return 'TEXT'
        } catch [System.Text.DecoderFallbackException] {
            return 'BINARY'
        }
    } finally {
        if ($null -ne $fileStream) { $fileStream.Dispose() }
    }
}

function Write-TextContent {
    param([string]$FilePath)

    $inputStream = $null
    $reader = $null
    try {
        $inputStream = [System.IO.FileStream]::new(
            $FilePath,
            [System.IO.FileMode]::Open,
            [System.IO.FileAccess]::Read,
            [System.IO.FileShare]::ReadWrite
        )
        $reader = [System.IO.StreamReader]::new($inputStream, $Utf8Strict, $true, 65536)
        while ($null -ne ($line = $reader.ReadLine())) {
            $script:Writer.WriteLine($line)
        }
    } finally {
        if ($null -ne $reader) { $reader.Dispose() }
        elseif ($null -ne $inputStream) { $inputStream.Dispose() }
    }
}

function Format-Size {
    param([long]$Bytes)
    if ($Bytes -ge 1GB) { return ('{0:N2} GiB' -f ($Bytes / 1GB)) }
    if ($Bytes -ge 1MB) { return ('{0:N2} MiB' -f ($Bytes / 1MB)) }
    if ($Bytes -ge 1KB) { return ('{0:N2} KiB' -f ($Bytes / 1KB)) }
    return "$Bytes B"
}

Write-Host "[SCAN] Project root: $ProjectRoot" -ForegroundColor Cyan
Write-Host '[SCAN] Tree: all except .git, .idea and old exports. Contents: text only (excluding this script).' -ForegroundColor Cyan
Visit-Directory -DirectoryPath $ProjectRoot -RelativePrefix ''

$EntriesSorted = @($script:Entries | Sort-Object RelativePath)
$script:Writer = [System.IO.StreamWriter]::new($OutputFile, $false, $Utf8NoBom)

try {
    $script:Writer.WriteLine('=====================================================================')
    $script:Writer.WriteLine('                   HomeCloudBackend - FULL PROJECT EXPORT')
    $script:Writer.WriteLine('=====================================================================')
    $script:Writer.WriteLine('Date: ' + (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'))
    $script:Writer.WriteLine('Project name: ' + $ProjectName)
    $script:Writer.WriteLine('Project root: ' + $ProjectRoot)
    $script:Writer.WriteLine('Excluded from tree: .git, .idea, ' + $ProjectName + '-export-*.txt')
    $script:Writer.WriteLine('Contents: text files only. Binary files and collector script: tree only.')
    $script:Writer.WriteLine('Symlinks/junctions: listed, never followed.')
    $script:Writer.WriteLine('')

    $script:Writer.WriteLine('=====================================================================')
    $script:Writer.WriteLine('                        FULL PROJECT TREE')
    $script:Writer.WriteLine('=====================================================================')
    $script:Writer.WriteLine($ProjectName + '/')
    foreach ($entry in $EntriesSorted) {
        $depth = ($entry.RelativePath -split '/').Count - 1
        $indent = '  ' * $depth
        if ($entry.Kind -eq 'DIR') {
            $script:Writer.WriteLine("$indent+ [DIR]  $($entry.RelativePath)/")
        } elseif ($entry.Kind -eq 'LINK') {
            $script:Writer.WriteLine("$indent~ [LINK] $($entry.RelativePath) [not followed]")
        } else {
            $size = Format-Size -Bytes $entry.Length
            $script:Writer.WriteLine("$indent- [FILE] $($entry.RelativePath) [$size]")
        }
    }

    $script:Writer.WriteLine('')
    $script:Writer.WriteLine('=====================================================================')
    $script:Writer.WriteLine('                           FILE CONTENTS')
    $script:Writer.WriteLine('=====================================================================')

    foreach ($entry in $EntriesSorted) {
        if ($entry.Kind -eq 'LINK') {
            $script:Symlinks++
            continue
        }
        if ($entry.Kind -ne 'FILE') { continue }

        $rel = $entry.RelativePath

        # Keep the collector in the project tree, but never export its source.
        if ([string]::Equals($entry.FullName, $CollectorFullPath, [System.StringComparison]::OrdinalIgnoreCase)) {
            $script:SkippedSelfFiles++
            Write-Host "[SKIP SELF] $rel" -ForegroundColor DarkGray
            continue
        }

        try {
            $kind = Get-ContentKind -FilePath $entry.FullName
            if ($kind -eq 'BINARY') {
                # Binary files remain in FULL PROJECT TREE with their sizes.
                # No [FILE] section and no Base64 in FILE CONTENTS.
                $script:SkippedBinaryFiles++
                Write-Host "[SKIP BINARY] $rel" -ForegroundColor DarkGray
                continue
            }

            $script:Writer.WriteLine('')
            $script:Writer.WriteLine('---------------------------------------------------------------------')
            $script:Writer.WriteLine("[FILE] $rel")
            $script:Writer.WriteLine("[SIZE] $($entry.Length) bytes")
            $script:Writer.WriteLine('[ENCODING] TEXT')
            $script:Writer.WriteLine('---------------------------------------------------------------------')

            Write-TextContent -FilePath $entry.FullName
            $script:TextFiles++
            $script:TotalInputBytes += $entry.Length
            Write-Host "[OK] $rel ($kind)" -ForegroundColor Green
        } catch {
            $script:ReadErrors++
            [void]$script:Errors.Add("FILE: $rel :: $($_.Exception.Message)")
            $script:Writer.WriteLine("[READ ERROR] $($_.Exception.Message)")
            Write-Warning "Cannot read ${rel}: $($_.Exception.Message)"
        }
    }

    $script:Writer.WriteLine('')
    $script:Writer.WriteLine('=====================================================================')
    $script:Writer.WriteLine('                              SUMMARY')
    $script:Writer.WriteLine('=====================================================================')
    $script:Writer.WriteLine("Text files: $($script:TextFiles)")
    $script:Writer.WriteLine("Binary files (tree only): $($script:SkippedBinaryFiles)")
    $script:Writer.WriteLine("Collector script (tree only): $($script:SkippedSelfFiles)")
    $script:Writer.WriteLine("Directories (listed): $(($EntriesSorted | Where-Object Kind -eq 'DIR').Count)")
    $script:Writer.WriteLine("Links (listed, not followed): $($script:Symlinks)")
    $script:Writer.WriteLine("Excluded directories/previous exports: $($script:ExcludedEntries)")
    $script:Writer.WriteLine("Read/enumeration errors: $($script:Errors.Count)")
    $script:Writer.WriteLine("Total successfully read bytes: $($script:TotalInputBytes)")
    if ($script:Errors.Count -gt 0) {
        $script:Writer.WriteLine('')
        $script:Writer.WriteLine('ERROR DETAILS:')
        foreach ($errorItem in $script:Errors) {
            $script:Writer.WriteLine(' - ' + $errorItem)
        }
    }
} finally {
    $script:Writer.Dispose()
}

$exportSize = (Get-Item -LiteralPath $OutputFile).Length
Write-Host ''
Write-Host "[DONE] $OutputFile" -ForegroundColor Green
Write-Host "[FILES] Text: $($script:TextFiles), binary skipped: $($script:SkippedBinaryFiles), self skipped: $($script:SkippedSelfFiles)" -ForegroundColor Cyan
Write-Host "[SIZE] $(Format-Size -Bytes $exportSize)" -ForegroundColor Cyan
if ($script:Errors.Count -gt 0) {
    Write-Warning "There were $($script:Errors.Count) read/enumeration errors. See the report summary."
}
Write-Host '[SECURITY] Text files may still contain passwords, config secrets and private keys.' -ForegroundColor Yellow
