# Angelina County Parcel & Abstract Data Downloader (PowerShell)
# Run: powershell -ExecutionPolicy Bypass -File download_data.ps1

$baseUrl = "https://utility.arcgis.com/usrsvcs/servers/0d57665b0361492397b48cbd4ad88ad6/rest/services/AngelinaCADWebService/FeatureServer"
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$batchSize = 2000

function Download-Layer {
    param([int]$layerId, [string]$fields, [string]$name, [string]$outFile)

    Write-Host "`n========================================" -ForegroundColor Cyan
    Write-Host "Downloading $name..." -ForegroundColor Cyan
    Write-Host "========================================" -ForegroundColor Cyan

    # Get count
    $countUrl = "$baseUrl/$layerId/query?where=1%3D1&returnCountOnly=true&f=json"
    $countData = Invoke-RestMethod -Uri $countUrl -UseBasicParsing
    $total = $countData.count
    Write-Host "  Total features: $total"

    $allFeatures = @()
    $offset = 0
    $batch = 0

    while ($offset -lt $total) {
        $batch++
        $url = "$baseUrl/$layerId/query?where=1%3D1&outFields=$fields&outSR=4326&f=json&resultRecordCount=$batchSize&resultOffset=$offset"

        $retries = 0
        $success = $false
        while (-not $success -and $retries -lt 3) {
            try {
                $data = Invoke-RestMethod -Uri $url -UseBasicParsing -TimeoutSec 120
                $success = $true
            } catch {
                $retries++
                Write-Host "  Retry $retries/3..." -ForegroundColor Yellow
                Start-Sleep -Seconds 2
            }
        }

        if (-not $success) {
            Write-Host "  FAILED at offset $offset after 3 retries" -ForegroundColor Red
            return
        }

        if ($data.features) {
            $allFeatures += $data.features
        }

        $offset += $batchSize
        $pct = [math]::Min(100, [math]::Floor($allFeatures.Count / $total * 100))
        Write-Host "  Batch ${batch}: $($allFeatures.Count) / $total ($pct%)"
    }

    # Convert to GeoJSON
    Write-Host "  Converting to GeoJSON..."

    $geojsonFeatures = @()
    foreach ($f in $allFeatures) {
        $geom = $f.geometry
        $props = @{}
        foreach ($attr in $f.attributes.PSObject.Properties) {
            $props[$attr.Name] = $attr.Value
        }

        if ($geom.rings) {
            # Polygon - round coordinates to 5 decimals
            $roundedRings = @()
            foreach ($ring in $geom.rings) {
                $roundedRing = @()
                foreach ($pt in $ring) {
                    $roundedRing += ,@([math]::Round($pt[0], 5), [math]::Round($pt[1], 5))
                }
                $roundedRings += ,$roundedRing
            }
            $geojsonFeatures += @{
                type = "Feature"
                geometry = @{ type = "Polygon"; coordinates = $roundedRings }
                properties = $props
            }
        }
    }

    $geojson = @{
        type = "FeatureCollection"
        features = $geojsonFeatures
    }

    $outPath = Join-Path $scriptDir $outFile
    Write-Host "  Saving to $outPath..."
    $geojson | ConvertTo-Json -Depth 10 -Compress | Out-File -FilePath $outPath -Encoding UTF8
    $sizeMB = [math]::Round((Get-Item $outPath).Length / 1MB, 1)
    Write-Host "  Saved: $outFile ($sizeMB MB)" -ForegroundColor Green
}

Write-Host "Angelina County Data Downloader" -ForegroundColor White
Write-Host "Source: Angelina CAD ArcGIS FeatureServer"
Write-Host "Output: $scriptDir"

# Download abstracts
Download-Layer -layerId 1 -fields "CODE,DESC_,Block,Surv_Sect,Surv_Name" -name "Abstracts (1,013)" -outFile "angelina_abstracts.geojson"

# Download parcels
Download-Layer -layerId 0 -fields "prop_id,file_as_name,legal_acreage,legal_desc,abs_subdv_cd,land_val,market,situs_street,situs_city,geo_id,Deed_Date" -name "Parcels (60,763)" -outFile "angelina_parcels.geojson"

Write-Host "`n========================================" -ForegroundColor Green
Write-Host "DONE! All files saved." -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host "`nNext: run this command to start the web server:"
Write-Host "  powershell -Command `"& { `$listener = [System.Net.HttpListener]::new(); `$listener.Prefixes.Add('http://localhost:8080/'); `$listener.Start(); Write-Host 'Server running at http://localhost:8080'; while (`$true) { `$ctx = `$listener.GetContext(); `$file = Join-Path '$scriptDir' (`$ctx.Request.Url.LocalPath.TrimStart('/')); if (Test-Path `$file -PathType Leaf) { `$bytes = [IO.File]::ReadAllBytes(`$file); `$ctx.Response.ContentLength64 = `$bytes.Length; `$ctx.Response.OutputStream.Write(`$bytes,0,`$bytes.Length) } else { `$ctx.Response.StatusCode = 404 }; `$ctx.Response.Close() } }`"" -ForegroundColor Yellow
Write-Host "`nOr if you have Node.js: npx serve ."
Write-Host "Then open: http://localhost:8080/angelina_lease_tracker.html"

Read-Host "`nPress Enter to exit"
