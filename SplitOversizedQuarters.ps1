# ================================================================================================
#   Split-and-gzip oversized Fannie Mae quarterly files on loan boundaries,
#   then verify no loan spans two pieces and every piece is under the upload limit.
#
#   Input : E:\Fannie Mae\<quarter>.csv        (raw, pipe-delimited, no header, leading pipe)
#   Output: E:\Fannie Mae\split\<quarter>_NN.csv.gz
# ================================================================================================

$quarters  = "2002Q4", "2003Q1", "2003Q2", "2003Q3", "2003Q4"
$srcDir    = "E:\Fannie Mae"
$outDir    = "E:\Fannie Mae\split"
$lines     = 30000000                       # cut threshold; cut fires only on a loan boundary
$kbLimit   = 1020000                        # safety margin under the 1,048,576 KB hard limit
$compress  = [System.IO.Compression.CompressionLevel]::Optimal   # or ::Fastest for speed

Add-Type -AssemblyName System.IO.Compression
New-Item -ItemType Directory -Force -Path $outDir | Out-Null

function New-GzipWriter([string]$path) {
    $fs = [System.IO.File]::Create($path)
    $gz = New-Object System.IO.Compression.GzipStream($fs, $script:compress)
    $sw = New-Object System.IO.StreamWriter($gz)
    $sw.NewLine = "`n"                      # LF endings for the SAS Linux server
    return $sw
}

$allPass = $true

foreach ($q in $quarters) {

    $in = Join-Path $srcDir "$q.csv"
    if (-not (Test-Path $in)) {
        Write-Host "ERROR: $in not found - skipping." -ForegroundColor Red
        $allPass = $false
        continue
    }

    Write-Host ""
    Write-Host "=== $q ================================================================" -ForegroundColor Cyan

    # -- Split and gzip in one pass, recording first/last loan ID of every piece --------------
    $pieces  = @()                          # one record per piece: name, firstId, lastId, rows
    $i       = 1
    $n       = 0
    $total   = 0
    $lastid  = ""
    $firstid = ""
    $reader  = [System.IO.File]::OpenText($in)
    $path    = Join-Path $outDir ("{0}_{1:D2}.csv.gz" -f $q, $i)
    $writer  = New-GzipWriter $path

    while (($line = $reader.ReadLine()) -ne $null) {
        $f = $line.Split('|')
        if ($f.Count -lt 2) { $id = $lastid } else { $id = $f[1] }   # leading pipe: ID is [1]

        if ($n -ge $lines -and $id -ne $lastid) {
            $writer.Close()
            $pieces += [pscustomobject]@{ Name=$path; FirstId=$firstid; LastId=$lastid; Rows=$n }
            $i++
            $n       = 0
            $firstid = ""
            $path    = Join-Path $outDir ("{0}_{1:D2}.csv.gz" -f $q, $i)
            $writer  = New-GzipWriter $path
        }

        if ($n -eq 0) { $firstid = $id }
        $writer.WriteLine($line)
        $n++
        $total++
        $lastid = $id
    }

    $writer.Close()
    $reader.Close()
    $pieces += [pscustomobject]@{ Name=$path; FirstId=$firstid; LastId=$lastid; Rows=$n }

    # -- Test 1: loan boundaries - last ID of each piece must differ from first ID of the next -
    $boundaryOK = $true
    for ($p = 0; $p -lt $pieces.Count - 1; $p++) {
        if ($pieces[$p].LastId -eq $pieces[$p + 1].FirstId) {
            Write-Host ("FAIL: loan {0} spans {1} and {2}" -f $pieces[$p].LastId,
                (Split-Path $pieces[$p].Name -Leaf), (Split-Path $pieces[$p+1].Name -Leaf)) `
                -ForegroundColor Red
            $boundaryOK = $false
        }
    }

    # -- Test 2: row conservation - piece rows must sum to rows read --------------------------
    $sumRows = ($pieces | Measure-Object Rows -Sum).Sum
    $rowsOK  = ($sumRows -eq $total)

    # -- Test 3: every piece under the upload limit -------------------------------------------
    $sizeOK = $true
    foreach ($pc in $pieces) {
        $kb = [math]::Round((Get-Item $pc.Name).Length / 1KB)
        $flag = if ($kb -ge $kbLimit) { $sizeOK = $false; " OVER LIMIT" } else { "" }
        Write-Host ("  {0}  {1,10:N0} KB  {2,12:N0} rows  first={3}  last={4}{5}" -f
            (Split-Path $pc.Name -Leaf), $kb, $pc.Rows, $pc.FirstId, $pc.LastId, $flag)
    }

    if ($boundaryOK -and $rowsOK -and $sizeOK) {
        Write-Host ("  PASS: {0:N0} rows in, {1:N0} rows out across {2} pieces, boundaries clean, all under {3:N0} KB." -f
            $total, $sumRows, $pieces.Count, $kbLimit) -ForegroundColor Green
    }
    else {
        if (-not $rowsOK) { Write-Host ("  FAIL: rows in {0:N0} <> rows out {1:N0}" -f $total, $sumRows) -ForegroundColor Red }
        if (-not $sizeOK) { Write-Host "  FAIL: at least one piece is at or over the size margin - lower `$lines and re-run this quarter." -ForegroundColor Red }
        $allPass = $false
    }
}

Write-Host ""
if ($allPass) { Write-Host "All five quarters split, gzipped, and verified." -ForegroundColor Green }
else          { Write-Host "One or more quarters FAILED - review messages above before uploading." -ForegroundColor Red }