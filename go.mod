module github.com/erigontech/erigon

go 1.23.0

toolchain go1.23.6

replace github.com/erigontech/erigon-lib => ./erigon-lib

replace (
	github.com/anacrolix/torrent => github.com/erigontech/torrent v1.54.3-alpha-1
	github.com/cometbft/cometbft => github.com/bnb-chain/greenfield-cometbft v1.3.1
	github.com/crate-crypto/go-kzg-4844 => github.com/erigontech/go-kzg-4844 v0.0.0-20250130131058-ce13be60bc86
	github.com/erigontech/erigon-snapshot => github.com/node-real/bsc-erigon-snapshot v1.0.1-0.20250729063616-9fbd82c54b3d
	github.com/holiman/bloomfilter/v2 => github.com/AskAlexSharov/bloomfilter/v2 v2.0.9
	github.com/tendermint/tendermint => github.com/bnb-chain/tendermint v0.31.16
)

require (
	github.com/erigontech/erigonwatch v0.0.0-20240718131902-b6576bde1116
	github.com/erigontech/mdbx-go v0.39.8
	github.com/erigontech/secp256k1 v1.1.0
	github.com/erigontech/silkworm-go v0.18.0
)

require (
	gfx.cafe/util/go/generic v0.0.0-20230721185457-c559e86c829c
	github.com/99designs/gqlgen v0.17.63
	github.com/Giulio2002/bls v0.0.0-20250218151206-daa74641714d
	github.com/Masterminds/sprig/v3 v3.2.3
	github.com/RoaringBitmap/roaring/v2 v2.4.3
	github.com/alecthomas/kong v0.8.1
	github.com/anacrolix/sync v0.5.1
	github.com/anacrolix/torrent v1.52.6-0.20231201115409-7ea994b6bbd8
	github.com/bnb-chain/ics23 v0.1.0
	github.com/c2h5oh/datasize v0.0.0-20231215233829-aa82cc1e6500
	github.com/cenkalti/backoff/v4 v4.2.1
	github.com/cometbft/cometbft v0.0.0-00010101000000-000000000000
	github.com/consensys/gnark-crypto v0.13.0
	github.com/crate-crypto/go-kzg-4844 v1.1.0
	github.com/davecgh/go-spew v1.1.1
	github.com/deckarep/golang-set v1.8.0
	github.com/deckarep/golang-set/v2 v2.5.0
	github.com/dop251/goja v0.0.0-20230806174421-c933cf95e127
	github.com/edsrzf/mmap-go v1.1.0
	github.com/emicklei/dot v1.6.2
	github.com/erigontech/erigon-lib v0.0.0-00010101000000-000000000000
	github.com/fjl/gencodec v0.0.0-20230517082657-f9840df7b83e
	github.com/gballet/go-verkle v0.0.0-20221121182333-31427a1f2d35
	github.com/go-chi/chi/v5 v5.1.0
	github.com/go-chi/cors v1.2.1
	github.com/go-echarts/go-echarts/v2 v2.3.3
	github.com/go-test/deep v1.1.1
	github.com/goccy/go-json v0.10.2
	github.com/gofrs/flock v0.12.1
	github.com/golang-jwt/jwt/v4 v4.5.1
	github.com/golang/snappy v0.0.5-0.20220116011046-fa5810519dcb
	github.com/google/btree v1.1.3
	github.com/google/cel-go v0.18.2
	github.com/google/go-cmp v0.6.0
	github.com/google/gofuzz v1.2.0
	github.com/gorilla/websocket v1.5.3
	github.com/grpc-ecosystem/go-grpc-middleware v1.4.0
	github.com/hashicorp/golang-lru/arc/v2 v2.0.7
	github.com/hashicorp/golang-lru/v2 v2.0.7
	github.com/holiman/uint256 v1.3.2
	github.com/huandu/xstrings v1.5.0
	github.com/huin/goupnp v1.3.0
	github.com/jackpal/go-nat-pmp v1.0.2
	github.com/jedib0t/go-pretty/v6 v6.5.9
	github.com/json-iterator/go v1.1.12
	github.com/julienschmidt/httprouter v1.3.0
	github.com/klauspost/compress v1.18.0
	github.com/libp2p/go-libp2p v0.37.2
	github.com/libp2p/go-libp2p-mplex v0.9.0
	github.com/libp2p/go-libp2p-pubsub v0.11.0
	github.com/maticnetwork/crand v1.0.2
	github.com/multiformats/go-multiaddr v0.13.0
	github.com/nxadm/tail v1.4.11
	github.com/pelletier/go-toml v1.9.5
	github.com/pelletier/go-toml/v2 v2.2.3
	github.com/pion/randutil v0.1.0
	github.com/pion/stun v0.6.1
	github.com/pkg/errors v0.9.1
	github.com/protolambda/ztyp v0.2.2
	github.com/prysmaticlabs/go-bitfield v0.0.0-20240618144021-706c95b2dd15
	github.com/prysmaticlabs/gohashtree v0.0.4-beta
	github.com/quasilyte/go-ruleguard/dsl v0.3.22
	github.com/rs/cors v1.11.1
	github.com/spf13/afero v1.10.0
	github.com/spf13/cobra v1.8.1
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.10.0
	github.com/tendermint/go-amino v0.16.0
	github.com/tendermint/tendermint v0.0.0-00010101000000-000000000000
	github.com/thomaso-mirodin/intmath v0.0.0-20160323211736-5dc6d854e46e
	github.com/tidwall/btree v1.6.0
	github.com/ugorji/go/codec v1.2.12
	github.com/urfave/cli/v2 v2.27.5
	github.com/valyala/fastjson v1.6.4
	github.com/vektah/gqlparser/v2 v2.5.21
	github.com/willf/bitset v1.1.10
	github.com/xsleonard/go-merkle v1.1.0
	go.uber.org/mock v0.5.0
	go.uber.org/zap v1.27.0
	golang.org/x/crypto v0.32.0
	golang.org/x/exp v0.0.0-20250128182459-e0ece0dbea4c
	golang.org/x/net v0.34.0
	golang.org/x/sync v0.10.0
	golang.org/x/sys v0.31.0
	golang.org/x/time v0.9.0
	google.golang.org/grpc v1.69.4
	google.golang.org/grpc/cmd/protoc-gen-go-grpc v1.5.1
	google.golang.org/protobuf v1.36.4
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c
	gopkg.in/natefinch/lumberjack.v2 v2.2.1
	gopkg.in/yaml.v2 v2.4.0
	gopkg.in/yaml.v3 v3.0.1
	modernc.org/sqlite v1.34.5
	pgregory.net/rapid v1.1.0
	sigs.k8s.io/yaml v1.4.0
)

require (
	github.com/RoaringBitmap/roaring v1.9.4 // indirect
	github.com/alecthomas/atomic v0.1.0-alpha2 // indirect
	github.com/benesch/cgosymbolizer v0.0.0-20190515212042-bec6fe6e597b // indirect
	github.com/btcsuite/btcd/btcec/v2 v2.3.2 // indirect
	github.com/cosmos/gogoproto v1.4.1 // indirect
	github.com/crate-crypto/go-ipa v0.0.0-20221111143132-9aa5d42120bc // indirect
	github.com/elastic/go-freelru v0.13.0 // indirect
	github.com/erigontech/speedtest v0.0.2 // indirect
	github.com/go-kit/kit v0.12.0 // indirect
	github.com/go-kit/log v0.2.1 // indirect
	github.com/go-logfmt/logfmt v0.5.1 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/go-task/slim-sprig/v3 v3.0.0 // indirect
	github.com/go-viper/mapstructure/v2 v2.2.1 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/hashicorp/go-cleanhttp v0.5.2 // indirect
	github.com/hashicorp/go-retryablehttp v0.7.7 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/nyaosorg/go-windows-shortcut v0.0.0-20220529122037-8b0c89bca4c4 // indirect
	github.com/petermattis/goid v0.0.0-20180202154549-b0b1615b78e5 // indirect
	github.com/sasha-s/go-deadlock v0.3.1 // indirect
	github.com/tklauser/go-sysconf v0.3.14 // indirect
	github.com/tklauser/numcpus v0.8.0 // indirect
	github.com/wlynxg/anet v0.0.5 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	go.opentelemetry.io/otel/metric v1.31.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20241015192408-796eee8c2d53 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20241015192408-796eee8c2d53 // indirect
)

require (
	github.com/Masterminds/goutils v1.1.1 // indirect
	github.com/Masterminds/semver/v3 v3.2.0 // indirect
	github.com/agnivade/levenshtein v1.2.0 // indirect
	github.com/ajwerner/btree v0.0.0-20211221152037-f427b3e689c0 // indirect
	github.com/anacrolix/chansync v0.3.0 // indirect
	github.com/anacrolix/dht/v2 v2.21.1 // indirect
	github.com/anacrolix/envpprof v1.3.0 // indirect
	github.com/anacrolix/generics v0.0.2-0.20240227122613-f95486179cab // indirect
	github.com/anacrolix/go-libutp v1.3.2 // indirect
	github.com/anacrolix/log v0.15.2 // indirect
	github.com/anacrolix/missinggo v1.3.0 // indirect
	github.com/anacrolix/missinggo/perf v1.0.0 // indirect
	github.com/anacrolix/missinggo/v2 v2.7.3 // indirect
	github.com/anacrolix/mmsg v1.0.1 // indirect
	github.com/anacrolix/multiless v0.3.1-0.20221221005021-2d12701f83f7 // indirect
	github.com/anacrolix/stm v0.4.1-0.20221221005312-96d17df0e496 // indirect
	github.com/anacrolix/upnp v0.1.3-0.20220123035249-922794e51c96 // indirect
	github.com/anacrolix/utp v0.1.0 // indirect
	github.com/antlr4-go/antlr/v4 v4.13.0 // indirect
	github.com/bahlo/generic-list-go v0.2.0 // indirect
	github.com/benbjohnson/clock v1.3.5 // indirect
	github.com/benbjohnson/immutable v0.4.1-0.20221220213129-8932b999621d // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bits-and-blooms/bitset v1.12.0 // indirect
	github.com/bradfitz/iter v0.0.0-20191230175014-e8f45d346db8 // indirect
	github.com/cespare/xxhash v1.1.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cilium/ebpf v0.11.0 // indirect
	github.com/consensys/bavard v0.1.13 // indirect
	github.com/containerd/cgroups v1.1.0 // indirect
	github.com/containerd/cgroups/v3 v3.0.3 // indirect
	github.com/coreos/go-systemd/v22 v22.5.0 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.5 // indirect
	github.com/davidlazar/go-crypto v0.0.0-20200604182044-b73af7476f6c // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.3.0
	github.com/dlclark/regexp2 v1.7.0 // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/elastic/gosigar v0.14.3 // indirect
	github.com/erigontech/erigon-snapshot v1.3.1-0.20250502073210-6c1e7e3d5165 // indirect
	github.com/flynn/noise v1.1.0 // indirect
	github.com/francoispqt/gojay v1.2.13 // indirect
	github.com/fsnotify/fsnotify v1.6.0 // indirect
	github.com/garslo/gogen v0.0.0-20170307003452-d6ebae628c7c // indirect
	github.com/go-llsqlite/adapter v0.0.0-20230927005056-7f5ce7f0c916 // indirect
	github.com/go-llsqlite/crawshaw v0.5.2-0.20240425034140-f30eb7704568 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-sourcemap/sourcemap v2.1.3+incompatible // indirect
	github.com/go-stack/stack v1.8.1
	github.com/godbus/dbus/v5 v5.1.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/google/gopacket v1.1.19 // indirect
	github.com/google/pprof v0.0.0-20241017200806-017d972448fc // indirect
	github.com/holiman/bloomfilter/v2 v2.0.3 // indirect
	github.com/ianlancetaylor/cgosymbolizer v0.0.0-20241129212102-9c50ad6b591e // indirect
	github.com/imdario/mergo v0.3.11 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/ipfs/go-cid v0.4.1 // indirect
	github.com/ipfs/go-log/v2 v2.5.1 // indirect
	github.com/jbenet/go-temp-err-catcher v0.1.0 // indirect
	github.com/klauspost/cpuid/v2 v2.2.8 // indirect
	github.com/koron/go-ssdp v0.0.4 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/libp2p/go-buffer-pool v0.1.0 // indirect
	github.com/libp2p/go-flow-metrics v0.2.0 // indirect
	github.com/libp2p/go-libp2p-asn-util v0.4.1 // indirect
	github.com/libp2p/go-mplex v0.7.0 // indirect
	github.com/libp2p/go-msgio v0.3.0 // indirect
	github.com/libp2p/go-nat v0.2.0 // indirect
	github.com/libp2p/go-netroute v0.2.1 // indirect
	github.com/libp2p/go-reuseport v0.4.0 // indirect
	github.com/libp2p/go-yamux/v4 v4.0.1 // indirect
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/marten-seemann/tcp v0.0.0-20210406111302-dfbc87cc63fd // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mattn/go-runewidth v0.0.15 // indirect
	github.com/miekg/dns v1.1.62 // indirect
	github.com/mikioh/tcpinfo v0.0.0-20190314235526-30a79bb1804b // indirect
	github.com/mikioh/tcpopt v0.0.0-20190314235656-172688c1accc // indirect
	github.com/minio/sha256-simd v1.0.1 // indirect
	github.com/mitchellh/copystructure v1.0.0 // indirect
	github.com/mitchellh/reflectwalk v1.0.0 // indirect
	github.com/mmcloughlin/addchain v0.4.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/mr-tron/base58 v1.2.0 // indirect
	github.com/mschoch/smat v0.2.0 // indirect
	github.com/multiformats/go-base32 v0.1.0 // indirect
	github.com/multiformats/go-base36 v0.2.0 // indirect
	github.com/multiformats/go-multiaddr-dns v0.4.1 // indirect
	github.com/multiformats/go-multiaddr-fmt v0.1.0 // indirect
	github.com/multiformats/go-multibase v0.2.0 // indirect
	github.com/multiformats/go-multicodec v0.9.0 // indirect
	github.com/multiformats/go-multihash v0.2.3 // indirect
	github.com/multiformats/go-multistream v0.6.0 // indirect
	github.com/multiformats/go-varint v0.0.7 // indirect
	github.com/ncruces/go-strftime v0.1.9 // indirect
	github.com/onsi/ginkgo/v2 v2.20.2 // indirect
	github.com/opencontainers/runtime-spec v1.2.0 // indirect
	github.com/pbnjay/memory v0.0.0-20210728143218-7b4eea64cf58 // indirect
	github.com/pion/datachannel v1.5.9 // indirect
	github.com/pion/dtls/v2 v2.2.12 // indirect
	github.com/pion/ice/v2 v2.3.36 // indirect
	github.com/pion/interceptor v0.1.37 // indirect
	github.com/pion/logging v0.2.2 // indirect
	github.com/pion/mdns v0.0.12 // indirect
	github.com/pion/rtcp v1.2.14 // indirect
	github.com/pion/rtp v1.8.9 // indirect
	github.com/pion/sctp v1.8.33 // indirect
	github.com/pion/sdp/v3 v3.0.9 // indirect
	github.com/pion/srtp/v2 v2.0.20 // indirect
	github.com/pion/transport/v2 v2.2.10 // indirect
	github.com/pion/turn/v2 v2.1.6 // indirect
	github.com/pion/webrtc/v3 v3.3.4 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/power-devops/perfstat v0.0.0-20210106213030-5aafc221ea8c // indirect
	github.com/prometheus/client_golang v1.20.5
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.60.0 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/quic-go/qpack v0.5.1 // indirect
	github.com/quic-go/quic-go v0.48.2 // indirect
	github.com/quic-go/webtransport-go v0.8.1-0.20241018022711-4ac2c9250e66 // indirect
	github.com/raulk/go-watchdog v1.3.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/rogpeppe/go-internal v1.12.0 // indirect
	github.com/rs/dnscache v0.0.0-20211102005908-e0241e321417 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/shirou/gopsutil/v4 v4.24.8 // indirect
	github.com/shoenig/go-m1cpu v0.1.6 // indirect
	github.com/shopspring/decimal v1.2.0 // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	github.com/sosodev/duration v1.3.1 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/spf13/cast v1.5.0 // indirect
	github.com/stoewer/go-strcase v1.2.0 // indirect
	github.com/supranational/blst v0.3.14
	github.com/xrash/smetrics v0.0.0-20240521201337-686a1a2994c1 // indirect
	go.etcd.io/bbolt v1.3.9 // indirect
	go.opentelemetry.io/otel v1.31.0 // indirect
	go.opentelemetry.io/otel/trace v1.31.0 // indirect
	go.uber.org/dig v1.18.0 // indirect
	go.uber.org/fx v1.23.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/mod v0.22.0 // indirect
	golang.org/x/text v0.21.0 // indirect
	golang.org/x/tools v0.29.0
	gopkg.in/tomb.v1 v1.0.0-20141024135613-dd632973f1e7 // indirect
	lukechampine.com/blake3 v1.3.0 // indirect
	modernc.org/libc v1.55.3 // indirect
	modernc.org/mathutil v1.6.0 // indirect
	modernc.org/memory v1.8.0 // indirect
	rsc.io/tmplfunc v0.0.3 // indirect
	zombiezen.com/go/sqlite v0.13.1 // indirect
)
