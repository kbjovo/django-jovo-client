<#
=============================================================================
 install-service-windows.ps1
 Registers the Replication Edge Appliance as a Windows Service on a host that
 runs Docker/Podman on the WSL2 backend. Secondary to the VM appliance (the
 committed default) — for clients who insist on a bare Windows host.

 Prereqs (validated below): Windows 10/11 or Server 2022, WSL2, Docker or
 Podman, the Netbird Windows client, and NSSM (https://nssm.cc) on PATH.

 Run in an elevated PowerShell:
   .\install-service-windows.ps1 -EdgeDir "C:\replication-edge\edge-stack"
=============================================================================
#>
param(
  [string]$EdgeDir = "C:\replication-edge\edge-stack",
  [string]$ServiceName = "ReplicationEdge"
)

$ErrorActionPreference = "Stop"

function Assert-Command($name) {
  if (-not (Get-Command $name -ErrorAction SilentlyContinue)) {
    throw "Required command '$name' not found on PATH. Install it and retry."
  }
}

Write-Host "Checking prerequisites..." -ForegroundColor Cyan
Assert-Command "wsl"
Assert-Command "docker"
Assert-Command "netbird"
Assert-Command "nssm"

$envFile = Join-Path $EdgeDir ".env.edge"
if (-not (Test-Path $envFile)) {
  throw ".env.edge not found in $EdgeDir. Copy .env.edge.example, fill in enrollment values, retry."
}

# The service runs a small launcher that: joins the mesh, resolves the mesh IP,
# writes NETBIRD_IP into .env.edge, then `docker compose up`. We reuse the same
# logic as start-edge.sh via WSL so there is one source of truth.
$launcher = Join-Path $EdgeDir "start-edge.sh"
if (-not (Test-Path $launcher)) {
  throw "start-edge.sh not found in $EdgeDir."
}

# Translate the Windows path to a WSL path (C:\x -> /mnt/c/x)
$wslPath = & wsl wslpath ("'" + $launcher + "'")
$wslPath = $wslPath.Trim()

Write-Host "Registering Windows service '$ServiceName'..." -ForegroundColor Cyan
# Remove a stale service if re-running
if (Get-Service $ServiceName -ErrorAction SilentlyContinue) {
  & nssm stop $ServiceName confirm 2>$null
  & nssm remove $ServiceName confirm
}

# NSSM runs the launcher through WSL; restart on failure; autostart on boot.
& nssm install $ServiceName "wsl" "bash $wslPath"
& nssm set $ServiceName Start SERVICE_AUTO_START
& nssm set $ServiceName AppExit Default Restart
& nssm set $ServiceName AppRestartDelay 15000
& nssm set $ServiceName AppStdout (Join-Path $EdgeDir "logs\edge-service.out.log")
& nssm set $ServiceName AppStderr (Join-Path $EdgeDir "logs\edge-service.err.log")

Write-Host "Starting '$ServiceName'..." -ForegroundColor Cyan
& nssm start $ServiceName

Write-Host "Done. The appliance will appear in the central console once the agent registers." -ForegroundColor Green
