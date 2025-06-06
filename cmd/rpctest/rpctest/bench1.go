// Copyright 2024 The Erigon Authors
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

package rpctest

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	libcommon "github.com/erigontech/erigon-lib/common"

	"github.com/erigontech/erigon/core/state"
)

var routes map[string]string

// bench1 compares response of Erigon with Geth
// but also can be used for comparing RPCDaemon with Geth
// parameters:
// needCompare - if false - doesn't call Erigon and doesn't compare responses
//
//	use false value - to generate vegeta files, it's faster but we can generate vegeta files for Geth and Erigon
//
// fullTest - if false - then call only methods which RPCDaemon currently supports
func Bench1(erigonURL, gethURL string, needCompare bool, fullTest bool, blockFrom uint64, blockTo uint64, recordFileName string) error {
	setRoutes(erigonURL, gethURL)

	resultsCh := make(chan CallResult, 1000)
	defer close(resultsCh)
	go vegetaWrite(false, []string{"eth_getBlockByNumber", "debug_storageRangeAt"}, resultsCh)

	var res CallResult
	reqGen := &RequestGenerator{}

	var blockNumber EthBlockNumber
	res = reqGen.Erigon("eth_blockNumber", reqGen.blockNumber(), &blockNumber)
	resultsCh <- res
	if res.Err != nil {
		return fmt.Errorf("Could not get block number: %v\n", res.Err)
	}
	if blockNumber.Error != nil {
		return fmt.Errorf("Error getting block number: %d %s\n", blockNumber.Error.Code, blockNumber.Error.Message)
	}
	fmt.Printf("Last block: %d\n", blockNumber.Number)
	accounts := make(map[libcommon.Address]struct{})
	prevBn := blockFrom
	storageCounter := 0
	for bn := blockFrom; bn <= blockTo; bn++ {
		var b EthBlockByNumber
		res = reqGen.Erigon("eth_getBlockByNumber", reqGen.getBlockByNumber(bn, true /* withTxs */), &b)
		resultsCh <- res
		if res.Err != nil {
			return fmt.Errorf("Could not retrieve block (Erigon) %d: %v\n", bn, res.Err)
		}

		if b.Error != nil {
			fmt.Printf("Error retrieving block (Erigon): %d %s\n", b.Error.Code, b.Error.Message)
		}

		if needCompare {
			var bg EthBlockByNumber
			res = reqGen.Geth("eth_getBlockByNumber", reqGen.getBlockByNumber(bn, true /* withTxs */), &bg)
			if res.Err != nil {
				return fmt.Errorf("Could not retrieve block (geth) %d: %v\n", bn, res.Err)
			}
			if bg.Error != nil {
				return fmt.Errorf("Error retrieving block (geth): %d %s\n", bg.Error.Code, bg.Error.Message)
			}
			if !compareBlocks(&b, &bg) {
				return fmt.Errorf("Block difference for %d\n", bn)
			}
		}

		accounts[b.Result.Miner] = struct{}{}

		for i, txn := range b.Result.Transactions {
			accounts[txn.From] = struct{}{}
			if txn.To != nil {
				accounts[*txn.To] = struct{}{}
			}

			if txn.To != nil && txn.Gas.ToInt().Uint64() > 21000 {
				storageCounter++
				if storageCounter == 100 {
					storageCounter = 0
					nextKey := &libcommon.Hash{}
					nextKeyG := &libcommon.Hash{}
					sm := make(map[libcommon.Hash]storageEntry)
					smg := make(map[libcommon.Hash]storageEntry)
					for nextKey != nil {
						var sr DebugStorageRange
						res = reqGen.Erigon("debug_storageRangeAt", reqGen.storageRangeAt(b.Result.Hash, i, txn.To, *nextKey), &sr)
						resultsCh <- res
						if res.Err != nil {
							return fmt.Errorf("Could not get storageRange (Erigon): %s: %v\n", txn.Hash, res.Err)
						}
						if sr.Error != nil {
							return fmt.Errorf("Error getting storageRange: %d %s\n", sr.Error.Code, sr.Error.Message)
						}

						for k, v := range sr.Result.Storage {
							sm[k] = v
							if v.Key == nil {
								fmt.Printf("%x: %x", k, v)
							}
						}
						nextKey = sr.Result.NextKey

					}

					if needCompare {
						for nextKeyG != nil {
							var srGeth DebugStorageRange
							res = reqGen.Geth("debug_storageRangeAt", reqGen.storageRangeAt(b.Result.Hash, i, txn.To, *nextKeyG), &srGeth)
							resultsCh <- res
							if res.Err != nil {
								return fmt.Errorf("Could not get storageRange (geth): %s: %v\n", txn.Hash, res.Err)
							}
							if srGeth.Error != nil {
								fmt.Printf("Error getting storageRange (geth): %d %s\n", srGeth.Error.Code, srGeth.Error.Message)
								break
							} else {
								for k, v := range srGeth.Result.Storage {
									smg[k] = v
									if v.Key == nil {
										fmt.Printf("%x: %x", k, v)
									}
								}
								nextKeyG = srGeth.Result.NextKey
							}
						}
						if !compareStorageRanges(sm, smg) {
							fmt.Printf("len(sm) %d, len(smg) %d\n", len(sm), len(smg))
							fmt.Printf("================sm\n")
							printStorageRange(sm)
							fmt.Printf("================smg\n")
							printStorageRange(smg)
							return errors.New("Storage range different\n")
						}
					}
				}
			}

			if !fullTest {
				continue // TODO: remove me
			}

			var trace EthTxTrace
			res = reqGen.Erigon("debug_traceTransaction", reqGen.debugTraceTransaction(txn.Hash, ""), &trace)
			resultsCh <- res
			if res.Err != nil {
				fmt.Printf("Could not trace transaction (Erigon) %s: %v\n", txn.Hash, res.Err)
				print(client, routes[Erigon], reqGen.debugTraceTransaction(txn.Hash, ""))
			}

			if trace.Error != nil {
				fmt.Printf("Error tracing transaction (Erigon): %d %s\n", trace.Error.Code, trace.Error.Message)
			}

			if needCompare {
				var traceg EthTxTrace
				res = reqGen.Geth("debug_traceTransaction", reqGen.debugTraceTransaction(txn.Hash, ""), &traceg)
				resultsCh <- res
				if res.Err != nil {
					print(client, routes[Geth], reqGen.debugTraceTransaction(txn.Hash, ""))
					return fmt.Errorf("Could not trace transaction (geth) %s: %v\n", txn.Hash, res.Err)
				}
				if traceg.Error != nil {
					return fmt.Errorf("Error tracing transaction (geth): %d %s\n", traceg.Error.Code, traceg.Error.Message)
				}
				if res.Err == nil && trace.Error == nil {
					if !compareTraces(&trace, &traceg) {
						return fmt.Errorf("Different traces block %d, txn %s\n", bn, txn.Hash)
					}
				}
			}

			var receipt EthReceipt
			res = reqGen.Erigon("eth_getTransactionReceipt", reqGen.getTransactionReceipt(txn.Hash), &receipt)
			resultsCh <- res
			if res.Err != nil {
				print(client, routes[Erigon], reqGen.getTransactionReceipt(txn.Hash))
				return fmt.Errorf("Count not get receipt (Erigon): %s: %v\n", txn.Hash, res.Err)
			}
			if receipt.Error != nil {
				return fmt.Errorf("Error getting receipt (Erigon): %d %s\n", receipt.Error.Code, receipt.Error.Message)
			}
			if needCompare {
				var receiptg EthReceipt
				res = reqGen.Geth("eth_getTransactionReceipt", reqGen.getTransactionReceipt(txn.Hash), &receiptg)
				resultsCh <- res
				if res.Err != nil {
					print(client, routes[Geth], reqGen.getTransactionReceipt(txn.Hash))
					return fmt.Errorf("Count not get receipt (geth): %s: %v\n", txn.Hash, res.Err)
				}
				if receiptg.Error != nil {
					return fmt.Errorf("Error getting receipt (geth): %d %s\n", receiptg.Error.Code, receiptg.Error.Message)
				}
				if !compareReceipts(&receipt, &receiptg) {
					fmt.Printf("Different receipts block %d, txn %s\n", bn, txn.Hash)
					print(client, routes[Geth], reqGen.getTransactionReceipt(txn.Hash))
					print(client, routes[Erigon], reqGen.getTransactionReceipt(txn.Hash))
					return errors.New("Receipts are different\n")
				}
			}
		}
		if !fullTest {
			continue // TODO: remove me
		}

		var balance EthBalance
		res = reqGen.Erigon("eth_getBalance", reqGen.getBalance(b.Result.Miner, bn), &balance)
		resultsCh <- res
		if res.Err != nil {
			return fmt.Errorf("Could not get account balance (Erigon): %v\n", res.Err)
		}
		if balance.Error != nil {
			return fmt.Errorf("Error getting account balance (Erigon): %d %s", balance.Error.Code, balance.Error.Message)
		}
		if needCompare {
			var balanceg EthBalance
			res = reqGen.Geth("eth_getBalance", reqGen.getBalance(b.Result.Miner, bn), &balanceg)
			resultsCh <- res
			if res.Err != nil {
				return fmt.Errorf("Could not get account balance (geth): %v\n", res.Err)
			}
			if balanceg.Error != nil {
				return fmt.Errorf("Error getting account balance (geth): %d %s\n", balanceg.Error.Code, balanceg.Error.Message)
			}
			if !compareBalances(&balance, &balanceg) {
				return fmt.Errorf("Miner %x balance difference for block %d\n", b.Result.Miner, bn)
			}
		}

		if prevBn < bn && bn%100 == 0 {
			// Checking modified accounts

			var mag DebugModifiedAccounts
			res = reqGen.Erigon("debug_getModifiedAccountsByNumber", reqGen.getModifiedAccountsByNumber(prevBn, bn), &mag)
			resultsCh <- res
			if res.Err != nil {
				return fmt.Errorf("Could not get modified accounts (Erigon): %v\n", res.Err)
			}
			if mag.Error != nil {
				return fmt.Errorf("Error getting modified accounts (Erigon): %d %s\n", mag.Error.Code, mag.Error.Message)
			}
			fmt.Printf("Done blocks %d-%d, modified accounts: %d\n", prevBn, bn, len(mag.Result))

			page := libcommon.Hash{}.Bytes()
			pageGeth := libcommon.Hash{}.Bytes()

			var accRangeErigon map[libcommon.Address]state.DumpAccount
			var accRangeGeth map[libcommon.Address]state.DumpAccount

			for len(page) > 0 {
				accRangeErigon = make(map[libcommon.Address]state.DumpAccount)
				accRangeGeth = make(map[libcommon.Address]state.DumpAccount)
				var sr DebugAccountRange

				res = reqGen.Erigon("debug_accountRange", reqGen.accountRange(bn, page, 256), &sr)
				resultsCh <- res

				if res.Err != nil {
					return fmt.Errorf("Could not get accountRange (Erigon): %v\n", res.Err)
				}

				if sr.Error != nil {
					fmt.Printf("Error getting accountRange (Erigon): %d %s\n", sr.Error.Code, sr.Error.Message)
					break
				} else {
					page = sr.Result.Next
					for k, v := range sr.Result.Accounts {
						accRangeErigon[k] = v
					}
				}
				if needCompare {
					var srGeth DebugAccountRange
					res = reqGen.Geth("debug_accountRange", reqGen.accountRange(bn, pageGeth, 256), &srGeth)
					resultsCh <- res
					if res.Err != nil {
						return fmt.Errorf("Could not get accountRange geth: %v\n", res.Err)
					}
					if srGeth.Error != nil {
						fmt.Printf("Error getting accountRange geth: %d %s\n", srGeth.Error.Code, srGeth.Error.Message)
						break
					} else {
						pageGeth = srGeth.Result.Next
						for k, v := range srGeth.Result.Accounts {
							accRangeGeth[k] = v
						}
					}
					if !bytes.Equal(page, pageGeth) {
						fmt.Printf("Different next page keys: %x geth %x", page, pageGeth)
					}
					if !compareAccountRanges(accRangeErigon, accRangeGeth) {
						return errors.New("Different in account ranges tx\n")
					}
				}
			}
			prevBn = bn
		}
	}
	return nil
}

// vegetaWrite (to be run as a goroutine) writing results of server calls into several files:
// results to /$tmp$/erigon_stress_test/results_*.csv
// vegeta format going to files /$tmp$/erigon_stress_test/vegeta_*.txt
func vegetaWrite(enabled bool, methods []string, resultsCh chan CallResult) {
	var err error
	var files map[string]map[string]*os.File
	var vegetaFiles map[string]map[string]*os.File
	if enabled {
		files = map[string]map[string]*os.File{
			Geth:   make(map[string]*os.File),
			Erigon: make(map[string]*os.File),
		}
		vegetaFiles = map[string]map[string]*os.File{
			Geth:   make(map[string]*os.File),
			Erigon: make(map[string]*os.File),
		}
		tmpDir := os.TempDir()
		fmt.Printf("tmp dir is: %s\n", tmpDir)
		dir := filepath.Join(tmpDir, "erigon_stress_test")
		if err = os.MkdirAll(dir, 0770); err != nil {
			panic(err)
		}

		for _, route := range []string{Geth, Erigon} {
			for _, method := range methods {
				file := filepath.Join(dir, "results_"+route+"_"+method+".csv")
				files[route][method], err = os.OpenFile(file, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
				if err != nil {
					panic(err)
				}
			}
		}

		for _, route := range []string{Geth, Erigon} {
			for _, method := range methods {
				file := filepath.Join(dir, "vegeta_"+route+"_"+method+".txt")
				vegetaFiles[route][method], err = os.OpenFile(file, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
				if err != nil {
					panic(err)
				}
			}
		}
	}

	for res := range resultsCh {
		// If not enabled, simply keep draining the results channel
		if enabled {
			if res.Err != nil {
				fmt.Printf("error response. target: %s, err: %s\n", res.Target, res.Err)
			}
			// files with call stats
			if f, ok := files[res.Target][res.Method]; ok {
				row := fmt.Sprintf("%d, %s, %d\n", res.RequestID, res.Method, res.Took.Microseconds())
				if _, err := fmt.Fprint(f, row); err != nil {
					panic(err)
				}
			}

			// vegeta files, write into all target files
			// because if "needCompare" is false - then we don't have responses from Erigon
			// but we still have enough information to build vegeta file for Erigon
			for _, target := range []string{Geth, Erigon} {
				if f, ok := vegetaFiles[target][res.Method]; ok {
					template := `{"method": "POST", "url": "%s", "body": "%s", "header": {"Content-Type": ["application/json"]}}`
					row := fmt.Sprintf(template, routes[target], base64.StdEncoding.EncodeToString([]byte(res.RequestBody)))

					if _, err := fmt.Fprint(f, row+"\n"); err != nil {
						panic(err)
					}
				}
			}
		}
	}
}
