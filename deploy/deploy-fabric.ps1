param(
    [string]$ConfigPath = "deploy/deploy.config.toml"
)

$ErrorActionPreference = "Stop"
$PSNativeCommandUseErrorActionPreference = $false

function Write-Step { param([string]$Message) Write-Host "[deploy-fabric] $Message" }

function Get-Config {
    param([string]$Path)
    if (-not (Test-Path $Path)) { throw "Config file not found: $Path" }
    $json = python -c "import json, pathlib, tomllib; p=pathlib.Path(r'$Path'); print(json.dumps(tomllib.loads(p.read_text(encoding='utf-8'))))"
    if ($LASTEXITCODE -ne 0) { throw "Failed to parse config file: $Path" }
    return $json | ConvertFrom-Json
}

function Select-Value {
    param([string]$Configured, [string]$Default)
    if ([string]::IsNullOrWhiteSpace($Configured)) { return $Default }
    return $Configured
}

function Get-FabricToken {
    return az account get-access-token --resource "https://api.fabric.microsoft.com" --query accessToken -o tsv
}

function Invoke-FabricApi {
    param([string]$Method, [string]$Uri, [object]$Body = $null)
    $token = Get-FabricToken
    $headers = @{ Authorization = "Bearer $token" }
    if ($null -eq $Body) {
        return Invoke-RestMethod -Method $Method -Uri $Uri -Headers $headers
    }
    $headers["Content-Type"] = "application/json"
    $jsonBody = $Body | ConvertTo-Json -Depth 50
    return Invoke-RestMethod -Method $Method -Uri $Uri -Headers $headers -Body $jsonBody
}

function Get-FabricItems {
    param([string]$WorkspaceId, [string]$Type = "")
    $base = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items"
    $uri = if ([string]::IsNullOrWhiteSpace($Type)) { $base } else { "$base`?type=$Type" }
    $response = Invoke-FabricApi -Method "GET" -Uri $uri
    if ($response -and $response.value) { return @($response.value) }
    return @()
}

function Ensure-FabricFolder {
    param([string]$WorkspaceId, [string]$FolderName, [string]$ParentFolderId = "")
    $existing = Get-FabricItems -WorkspaceId $WorkspaceId -Type "Folder" |
        Where-Object { $_.displayName -eq $FolderName } | Select-Object -First 1
    if ($existing) { return $existing.id }
    $body = @{ displayName = $FolderName; type = "Folder" }
    if (-not [string]::IsNullOrWhiteSpace($ParentFolderId)) { $body.folderId = $ParentFolderId }
    $created = Invoke-FabricApi -Method "POST" -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items" -Body $body
    Write-Step "  Created folder '$FolderName'."
    return $created.id
}

function Ensure-FabricLakehouse {
    param([string]$WorkspaceId, [string]$LakehouseId, [string]$LakehouseName, [string]$FolderId = "")
    if (-not [string]::IsNullOrWhiteSpace($LakehouseId)) { return $LakehouseId }
    $existing = Get-FabricItems -WorkspaceId $WorkspaceId -Type "Lakehouse" |
        Where-Object { $_.displayName -eq $LakehouseName } | Select-Object -First 1
    if ($existing) {
        Write-Step "  Lakehouse '$LakehouseName' exists: $($existing.id)"
        return $existing.id
    }
    $body = @{ displayName = $LakehouseName; type = "Lakehouse" }
    if (-not [string]::IsNullOrWhiteSpace($FolderId)) { $body.folderId = $FolderId }
    $created = Invoke-FabricApi -Method "POST" -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items" -Body $body
    Write-Step "  Created lakehouse '$LakehouseName': $($created.id)"
    return $created.id
}

function Ensure-FabricNotebook {
    param([string]$WorkspaceId, [string]$DisplayName, [string]$FolderId, [string]$SourceFilePath)
    if (-not (Test-Path $SourceFilePath)) { throw "Notebook source not found: $SourceFilePath" }
    $source = Get-Content -Path $SourceFilePath -Raw -Encoding UTF8
    $payloadBase64 = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($source))
    $definition = @{
        format = "fabricGitSource"
        parts = @( @{ path = "notebook-content.py"; payload = $payloadBase64; payloadType = "InlineBase64" } )
    }
    $existing = Get-FabricItems -WorkspaceId $WorkspaceId -Type "Notebook" |
        Where-Object { $_.displayName -eq $DisplayName } | Select-Object -First 1
    if (-not $existing) {
        $body = @{ displayName = $DisplayName; type = "Notebook"; folderId = $FolderId; definition = $definition }
        $created = Invoke-FabricApi -Method "POST" -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items" -Body $body
        Write-Step "  Created notebook '$DisplayName'."
        return $created.id
    }
    Invoke-FabricApi -Method "POST" `
      -Uri "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/items/$($existing.id)/updateDefinition?updateMetadata=true" `
      -Body @{ definition = $definition } | Out-Null
    Write-Step "  Updated notebook '$DisplayName'."
    return $existing.id
}

# ---------------------------------------------------------------------------
$config = Get-Config -Path $ConfigPath

$workspaceId = $config.fabric.workspace_id
if ([string]::IsNullOrWhiteSpace($workspaceId)) { throw "fabric.workspace_id is required." }

$pushNotebooks = [bool]$config.fabric.push_notebooks

# Ensure folders
Write-Step "Ensuring folder structure."
$notebooksFolder = Ensure-FabricFolder -WorkspaceId $workspaceId -FolderName "notebooks"
$mainFolder      = Ensure-FabricFolder -WorkspaceId $workspaceId -FolderName "main" -ParentFolderId $notebooksFolder
$modulesFolder   = Ensure-FabricFolder -WorkspaceId $workspaceId -FolderName "modules" -ParentFolderId $notebooksFolder

# Ensure lakehouses
Write-Step "Ensuring lakehouses."
$landingId = Ensure-FabricLakehouse -WorkspaceId $workspaceId -LakehouseId $config.lakehouses.landing_id -LakehouseName $config.lakehouses.landing_name
$bronzeId  = Ensure-FabricLakehouse -WorkspaceId $workspaceId -LakehouseId $config.lakehouses.bronze_id  -LakehouseName $config.lakehouses.bronze_name
$silverId  = Ensure-FabricLakehouse -WorkspaceId $workspaceId -LakehouseId $config.lakehouses.silver_id  -LakehouseName $config.lakehouses.silver_name
$goldId    = Ensure-FabricLakehouse -WorkspaceId $workspaceId -LakehouseId $config.lakehouses.gold_id    -LakehouseName $config.lakehouses.gold_name

# Push notebooks
if ($pushNotebooks) {
    Write-Step "Pushing notebooks."
    $nbBase = "deploy/assets/notebooks"

    foreach ($mod in @("config_module", "utils_module")) {
        Ensure-FabricNotebook -WorkspaceId $workspaceId -DisplayName $mod -FolderId $modulesFolder -SourceFilePath "$nbBase/modules/$mod.py"
    }
    foreach ($main in @("01_ingest_main", "02_transform_main", "03_enrich_main", "04_aggregate_main")) {
        Ensure-FabricNotebook -WorkspaceId $workspaceId -DisplayName $main -FolderId $mainFolder -SourceFilePath "$nbBase/main/$main.py"
    }
}

Write-Step "Fabric deployment complete."
Write-Output ""
Write-Output "Workspace:  $workspaceId"
Write-Output "Landing:    $landingId"
Write-Output "Bronze:     $bronzeId"
Write-Output "Silver:     $silverId"
Write-Output "Gold:       $goldId"
