package freezeblocks

import (
	"bytes"
	"encoding/json"
	"fmt"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutil"
	"github.com/ledgerwatch/erigon/core/types"
	"io/ioutil"
	"math/big"
	"net/http"
)

const (
	infuraURL = "https://bsc-testnet.nodereal.io/v1/2e9d633513f945d89ec95755ae183afd" // 替换为你的Infura项目ID
)

type RPCRequest struct {
	Jsonrpc string   `json:"jsonrpc"`
	Method  string   `json:"method"`
	Params  []string `json:"params"`
	ID      int      `json:"id"`
}

type BlockResponse struct {
	Jsonrpc string      `json:"jsonrpc"`
	Id      int         `json:"id"`
	Result  interface{} `json:"result"`
	Error   interface{} `json:"error"`
}
type BlobResponse struct {
	BlobTxSidecar types.BlobTxSidecar `json:"blobSidecar"`
	BlockNumber   *big.Int            `json:"blockNumber"`
	BlockHash     libcommon.Hash      `json:"blockHash"`
	TxIndex       uint64              `json:"txIndex"`
	TxHash        libcommon.Hash      `json:"txHash"`
}

func GetBlobSidecars(blockNumber uint64) types.BlobSidecars {
	blockNum := hexutil.EncodeUint64(blockNumber)
	request := RPCRequest{
		Jsonrpc: "2.0",
		Method:  "eth_getBlobSidecars",
		Params:  []string{blockNum},
		ID:      1,
	}

	body, err := json.Marshal(request)
	if err != nil {
		fmt.Println("Error marshalling request:", err)
		return nil
	}

	resp, err := http.Post(infuraURL, "application/json", bytes.NewBuffer(body))
	if err != nil {
		fmt.Println("Error making request:", err)
		return nil
	}
	defer resp.Body.Close()

	responseBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Error reading response body:", err)
		return nil
	}

	var blockResponse BlockResponse
	err = json.Unmarshal(responseBody, &blockResponse)
	if err != nil {
		fmt.Println("Error unmarshalling response:", err)
		return nil
	}

	if blockResponse.Error != nil {
		fmt.Println("Error from RPC:", blockResponse.Error)
		return nil
	}

	var blobResponse []BlobResponse
	err = json.Unmarshal(responseBody, &blobResponse)
	if err != nil {
		fmt.Println("Error unmarshalling response:", err)
		return nil
	}

	var blobSidecars types.BlobSidecars
	for _, blobSidecar := range blobResponse {
		blob := &types.BlobSidecar{
			BlobTxSidecar: blobSidecar.BlobTxSidecar,
			BlockNumber:   blobSidecar.BlockNumber,
			BlockHash:     blobSidecar.BlockHash,
			TxIndex:       blobSidecar.TxIndex,
			TxHash:        blobSidecar.TxHash,
		}
		blobSidecars = append(blobSidecars, blob)
	}
	return blobSidecars
}
