# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Building
- `make erigon` - Build the main Erigon executable
- `make all` - Build Erigon and all additional commands
- `make dbg` - Build debug version with C stack traces
- `make db-tools` - Build MDBX database tools

### Testing
- `make test-short` - Run short tests with 10m timeout
- `make test-all` - Run all tests with 1h timeout
- `make test-all-race` - Run all tests with race detection
- `make test-hive` - Run Hive tests locally (requires act and GITHUB_TOKEN)

### Code Quality
- `make lint` - Run all linters (uses .golangci.yml configuration)
- `make lintci` - Run golangci-lint for CI

### Docker
- `make docker` - Build Docker image
- `make docker-compose` - Run full stack with docker-compose

## Architecture Overview

### Project Structure
- **cmd/** - Executable commands (erigon, rpcdaemon, downloader, etc.)
- **core/** - Core blockchain functionality
- **eth/** - Ethereum protocol implementation
- **p2p/** - Peer-to-peer networking
- **rpc/** - JSON-RPC API implementation
- **execution/** - Block execution and state management
- **erigon-lib/** - Shared library components
- **cl/** - Consensus layer (Caplin)
- **polygon/** - Polygon-specific code

### Key Components
- **Erigon** - Main Ethereum client executable
- **RPCDaemon** - JSON-RPC server (can run separately)
- **Downloader** - Snapshot and block downloading
- **Sentry** - P2P networking layer
- **Caplin** - Built-in consensus client
- **TxPool** - Transaction pool management

### Data Storage
- Uses MDBX database for efficient storage
- Supports modular architecture where components can run as separate processes
- Snapshot-based sync for faster initial synchronization
- Three pruning modes: archive, full (default), minimal

### Build System
- Go 1.24+ required
- Uses CGO with specific MDBX optimizations
- Support for multiple architectures and platforms
- Docker-based development environment available

## Chain Support
- Ethereum Mainnet (default)
- BSC (Binance Smart Chain) - this is a BSC-specific fork
- Polygon (`--chain=bor-mainnet`)
- Various testnets (Sepolia, Holesky, etc.)

## Development Notes
- This appears to be the BSC (Binance Smart Chain) fork of Erigon
- Uses erigon-lib as a shared library
- Extensive test suite with different timeout configurations
- Supports both embedded and external consensus layer clients
- Modular design allows running components separately for scaling