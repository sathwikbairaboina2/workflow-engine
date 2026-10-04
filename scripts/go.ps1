# Runs the Go toolchain inside Docker so the host needs no Go install.
$root = (Resolve-Path "$PSScriptRoot\..").Path -replace '\', '/'
$name = "workflow-engine-go-$PID-$(Get-Random -Maximum 99999)"
docker run --rm --name $name --tmpfs /tmp:rw,exec,mode=1777,size=4g -v "${root}:/src" -v workflow-engine-gomod:/go/pkg/mod -v workflow-engine-gobuild:/root/.cache/go-build -w /src golang:1.26.8 go @args
exit $LASTEXITCODE
