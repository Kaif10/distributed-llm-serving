# Run the pinned local CPU backend in the foreground. Stop with Ctrl+C.
$ErrorActionPreference = 'Stop'
$fleetRoot = Split-Path -Parent $PSScriptRoot
$fleetServer = Join-Path $fleetRoot '.tools\llama-b12110\llama-server.exe'
$fleetModel = Join-Path $fleetRoot '.tools\models\SmolLM2-135M-Instruct-Q4_K_M.gguf'
if (-not (Test-Path -LiteralPath $fleetServer) -or -not (Test-Path -LiteralPath $fleetModel)) {
    throw 'Run python scripts/setup_llama.py first.'
}
& $fleetServer -m $fleetModel --host 127.0.0.1 --port 18081 --alias smollm2-135m -c 2048 -np 1 -t 4 -ngl 0 --metrics --slots --no-ui --no-cache-prompt
exit $LASTEXITCODE
