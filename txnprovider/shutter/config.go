// Copyright 2025 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package shutter

import (
	"crypto/ecdsa"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"

	"github.com/erigontech/erigon-lib/chain/networkname"
	"github.com/erigontech/erigon/cl/clparams"
)

type Config struct {
	P2pConfig
	Enabled                          bool
	InstanceId                       uint64
	BeaconChainGenesisTimestamp      uint64
	SecondsPerSlot                   uint64
	SequencerContractAddress         string
	ValidatorRegistryContractAddress string
	KeyBroadcastContractAddress      string
	KeyperSetManagerContractAddress  string
	MaxNumKeysPerMessage             uint64
	MaxRecentEons                    int
}

type P2pConfig struct {
	PrivateKey     *ecdsa.PrivateKey
	ListenPort     uint64
	BootstrapNodes []string
}

func (c P2pConfig) BootstrapNodesAddrInfo() ([]peer.AddrInfo, error) {
	addrInfos := make([]peer.AddrInfo, len(c.BootstrapNodes))
	for i, node := range c.BootstrapNodes {
		ma, err := multiaddr.NewMultiaddr(node)
		if err != nil {
			return nil, err
		}

		ai, err := peer.AddrInfoFromP2pAddr(ma)
		if err != nil {
			return nil, err
		}

		addrInfos[i] = *ai
	}

	return addrInfos, nil
}

func ConfigByChainName(chainName string) Config {
	switch chainName {
	case networkname.Chiado:
		return chiadoConfig
	case networkname.Gnosis:
		return gnosisConfig
	default:
		panic("missing shutter config for chain: " + chainName)
	}
}

var (
	chiadoConfig = Config{
		Enabled:                          true,
		InstanceId:                       102_000,
		BeaconChainGenesisTimestamp:      1665396300,
		SecondsPerSlot:                   clparams.BeaconConfigs[clparams.ChiadoNetwork].SecondsPerSlot,
		SequencerContractAddress:         "0x2aD8E2feB0ED5b2EC8e700edB725f120576994ed",
		ValidatorRegistryContractAddress: "0xa9289A3Dd14FEBe10611119bE81E5d35eAaC3084",
		KeyBroadcastContractAddress:      "0x9D31865BEffcE842FBd36CDA587aDDA8bef804B7",
		KeyperSetManagerContractAddress:  "0xC4DE9FAf4ec882b33dA0162CBE628B0D8205D0c0",
		MaxNumKeysPerMessage:             defaultMaxNumKeysPerMessage,
		MaxRecentEons:                    defaultMaxRecentEons,
		P2pConfig: P2pConfig{
			ListenPort: defaultP2PListenPort,
			BootstrapNodes: []string{
				"/ip4/167.99.177.227/tcp/23005/p2p/12D3KooWSdm5guPBdn8DSaBphVBzUUgPLg9sZLnazEUrcbtLy254",
				"/ip4/159.89.15.119/tcp/23005/p2p/12D3KooWPP6bp2PJQR8rUvG1SD4qNH4WFrKve6DMgWThyKxwNbbH",
			},
		},
	}

	gnosisConfig = Config{
		Enabled:                          true,
		InstanceId:                       1_000,
		BeaconChainGenesisTimestamp:      1638993340,
		SecondsPerSlot:                   clparams.BeaconConfigs[clparams.GnosisNetwork].SecondsPerSlot,
		SequencerContractAddress:         "0xc5C4b277277A1A8401E0F039dfC49151bA64DC2E",
		ValidatorRegistryContractAddress: "0xefCC23E71f6bA9B22C4D28F7588141d44496A6D6",
		KeyBroadcastContractAddress:      "0x626dB87f9a9aC47070016A50e802dd5974341301",
		KeyperSetManagerContractAddress:  "0x7C2337f9bFce19d8970661DA50dE8DD7d3D34abb",
		MaxNumKeysPerMessage:             defaultMaxNumKeysPerMessage,
		MaxRecentEons:                    defaultMaxRecentEons,
		P2pConfig: P2pConfig{
			ListenPort: defaultP2PListenPort,
			BootstrapNodes: []string{
				"/ip4/167.99.177.227/tcp/23003/p2p/12D3KooWD35AESYCttDEi3J5WnQdTFuM5JNtmuXEb1x4eQ28gb1s",
				"/ip4/159.89.15.119/tcp/23003/p2p/12D3KooWRzAhgPA16DiBQhiuYoasYzJaQSAbtc5i5FvgTi9ZDQtS",
			},
		},
	}
)

const (
	defaultP2PListenPort        = 23_102
	defaultMaxNumKeysPerMessage = 500
	defaultMaxRecentEons        = 100
)
