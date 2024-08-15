package surfstore

import (
	"io/ioutil"
	"log"
	"os"
)

func ClientSync(client RPCClient) {
	// get local index.db
	localMetaMap, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		log.Println(err)
	}
	// log.Println("local MetaMap:")
	// PrintMetaMap(localMetaMap)

	// Scan the base directory, and for each file, compute that file’s hash list
	// (1) there are now new files in the base directory that aren’t in the index file,
	// (2) files that are in the index file, but have changed since the last time the client was executed
	// (i.e., the hash list is different).
	// compare local files and local index.db
	files, err := ioutil.ReadDir(client.BaseDir)
	if err != nil {
		log.Println(err)
	}
	baseFilesMap := make(map[string][]string) // records new files in base dir that are not in index.db
	deletedFiles := make([]string, 0)
	for _, file := range files {
		// Todo: removed .DS_Store before submit
		if file.Name() != "index.db" && file.Name() != ".DS_Store" {
			content, err := ioutil.ReadFile(ConcatPath(client.BaseDir, file.Name()))
			if len(content) == 0 {
				continue
			}
			if err != nil {
				log.Fatal("Error reading file:", err)
				return
			}
			// fmt.Println(file.Name())
			hashList, _ := fileToHashesAndBlocks(content, client.BlockSize)

			// if file name exist in index.db
			if val, ok := localMetaMap[file.Name()]; ok {
				// if file content is changed
				if !compareStringSlice(val.BlockHashList, hashList) {
					baseFilesMap[file.Name()] = hashList
				}
			} else {
				baseFilesMap[file.Name()] = hashList
			}
			// hashByte := GetBlockHashBytes(content)
			// fmt.Println((content))
		}
	}
	// find file in index.db but not in base dir
	for fileName, metaData := range localMetaMap {
		if metaData.BlockHashList[0] != TOMBSTONE_HASHVALUE {
			_, error := os.Stat(ConcatPath(client.BaseDir, fileName))
			if os.IsNotExist(error) {
				deletedFiles = append(deletedFiles, fileName)
			}
		}
	}

	// Next, the client should connect to the server and download an updated FileInfoMap.
	var cloudMetaMap map[string]*FileMetaData
	err = client.GetFileInfoMap(&cloudMetaMap)
	if err != nil {
		log.Fatal(err)
	}
	// log.Println("SurfStore MetaBlock:")
	// PrintMetaMap(cloudMetaMap)

	// compare the local index (and any changes to local files not reflected in the local index) with the remote index

	// Case1: remote index refers to a file not present in the local index or in the base directory
	// client should download the blocks associated with that file, reconstitute that file in the base directory,
	// and then add the updated FileInfo information to the local index.
	// log.Println(baseFilesMap, deletedFiles)
	for fileName, metaData := range cloudMetaMap {
		// skip deleted file
		if metaData.BlockHashList[0] == TOMBSTONE_HASHVALUE && localMetaMap[fileName].BlockHashList[0] != TOMBSTONE_HASHVALUE {
			if _, error := os.Stat(ConcatPath(client.BaseDir, fileName)); !os.IsNotExist(error) {
				e := os.Remove(ConcatPath(client.BaseDir, fileName))
				if e != nil {
					log.Println(e)
				}
			}
			continue
		}
		// file not present in the local index
		if _, ok := localMetaMap[fileName]; !ok {
			// get mapping from block server address to block hashes
			var blockStoreMap map[string][]string
			client.GetBlockStoreMap(metaData.BlockHashList, &blockStoreMap)
			// download this file to local
			data := make([]byte, 0)
			for _, hash := range metaData.BlockHashList {
				for blockStoreAddr, blockHashes := range blockStoreMap {
					for _, blockHash := range blockHashes {
						if hash == blockHash {
							var block Block
							err := client.GetBlock(hash, blockStoreAddr, &block)
							if err != nil {
								log.Println("when download, GetBlock", err)
							}
							data = append(data, block.BlockData...)
						}
					}
				}
			}
			err := os.WriteFile(ConcatPath(client.BaseDir, fileName), data, 0644)
			if err != nil {
				log.Println("when WriteFile", err)
			}
			log.Println("finished downloading", fileName)

		} else {
			// file exist in index.db
			// compare version
			if metaData.Version > localMetaMap[fileName].Version {
				// get mapping from block server address to block hashes
				var blockStoreMap map[string][]string
				client.GetBlockStoreMap(metaData.BlockHashList, &blockStoreMap)
				// download this file to local
				data := make([]byte, 0)
				for _, hash := range metaData.BlockHashList {
					for blockStoreAddr, blockHashes := range blockStoreMap {
						for _, blockHash := range blockHashes {
							if hash == blockHash {
								var block Block
								err := client.GetBlock(hash, blockStoreAddr, &block)
								if err != nil {
									log.Fatal("when download, GetBlock", err)
								}
								data = append(data, block.BlockData...)
							}
						}
					}
				}
				err := os.WriteFile(ConcatPath(client.BaseDir, fileName), data, 0644)
				if err != nil {
					log.Println("when WriteFile", err)
				}
				log.Println("finished downloading", fileName)
			} else if metaData.Version == localMetaMap[fileName].Version {
				// upload if local file is modified
				if hashList, ok := baseFilesMap[fileName]; ok {
					// upload this file to cloud with Verision+1
					content, err := ioutil.ReadFile(ConcatPath(client.BaseDir, fileName))
					if err != nil {
						log.Println("Error reading file:", err)
						return
					}
					// write to MetaStore
					version := metaData.Version + 1
					metaData := FileMetaData{Filename: fileName, Version: version, BlockHashList: hashList}
					err = client.UpdateFile(&metaData, &version)
					if err != nil {
						log.Fatal("when UpdateFile", err)
					}

					// write to BlockStore
					hashList, blocks := fileToHashesAndBlocks(content, client.BlockSize)
					hashBlockMap := make(map[string]Block)
					for idx, hash := range hashList {
						hashBlockMap[hash] = blocks[idx]
					}
					// get mapping from block server address to block hashes
					var blockStoreMap map[string][]string
					client.GetBlockStoreMap(hashList, &blockStoreMap)
					log.Println("1", blockStoreMap)
					for blockStoreAddr, blockHashes := range blockStoreMap {
						for _, blockHash := range blockHashes {
							block := hashBlockMap[blockHash]
							var succ bool
							err := client.PutBlock(&block, blockStoreAddr, &succ)
							if err != nil {
								log.Fatal("when PutBlock", err)
							}
						}
					}
					log.Println("finished uploading", fileName)
				}
			}
		}
	}

	// Case2: new files in the local base directory that aren’t in the local index or in the remote index
	for fileName, hashList := range baseFilesMap {
		// upload the blocks corresponding to this file to the server, then update the server with the new FileInfo
		// if file not in cloud
		if _, ok := cloudMetaMap[fileName]; !ok {
			content, err := ioutil.ReadFile(ConcatPath(client.BaseDir, fileName))
			if err != nil {
				log.Println("Error reading file:", err)
				return
			}
			// write to MetaStore
			version1 := int32(1)
			metaData := FileMetaData{Filename: fileName, Version: version1, BlockHashList: hashList}
			err = client.UpdateFile(&metaData, &version1)
			if err != nil {
				log.Fatal("when UpdateFile", err)
			}
			// write to BlockStore
			hashList, blocks := fileToHashesAndBlocks(content, client.BlockSize)
			hashBlockMap := make(map[string]Block)
			for idx, hash := range hashList {
				hashBlockMap[hash] = blocks[idx]
			}
			// get mapping from block server address to block hashes
			var blockStoreMap map[string][]string
			client.GetBlockStoreMap(hashList, &blockStoreMap)
			log.Println("2", blockStoreMap)
			for blockStoreAddr, blockHashes := range blockStoreMap {
				for _, blockHash := range blockHashes {
					block := hashBlockMap[blockHash]
					var succ bool
					err := client.PutBlock(&block, blockStoreAddr, &succ)
					if err != nil {
						log.Fatal("when PutBlock", err)
					}
				}
			}
			log.Println("finished uploading", fileName)
		}
		// if file is recreated
		if _, ok := localMetaMap[fileName]; ok {
			if localMetaMap[fileName].BlockHashList[0] == TOMBSTONE_HASHVALUE {
				content, err := ioutil.ReadFile(ConcatPath(client.BaseDir, fileName))
				if err != nil {
					log.Println("Error reading file:", err)
					return
				}
				// write to MetaStore
				version1 := int32(1)
				metaData := FileMetaData{Filename: fileName, Version: version1, BlockHashList: hashList}
				err = client.UpdateFile(&metaData, &version1)
				if err != nil {
					log.Fatal("when UpdateFile", err)
				}
				// write to BlockStore
				hashList, blocks := fileToHashesAndBlocks(content, client.BlockSize)
				hashBlockMap := make(map[string]Block)
				for idx, hash := range hashList {
					hashBlockMap[hash] = blocks[idx]
				}
				// get mapping from block server address to block hashes
				var blockStoreMap map[string][]string
				client.GetBlockStoreMap(hashList, &blockStoreMap)
				log.Println("3", blockStoreMap)
				for blockStoreAddr, blockHashes := range blockStoreMap {
					for _, blockHash := range blockHashes {
						block := hashBlockMap[blockHash]
						var succ bool
						err := client.PutBlock(&block, blockStoreAddr, &succ)
						if err != nil {
							log.Fatal("when PutBlock", err)
						}
					}
				}
				log.Println("finished uploading", fileName)
			}
		}
	}

	for _, fileName := range deletedFiles {
		log.Println(fileName, " deleted")
		// write to MetaStore
		version := localMetaMap[fileName].Version + 1
		metaData := FileMetaData{Filename: fileName, Version: version, BlockHashList: []string{TOMBSTONE_HASHVALUE}}
		err = client.UpdateFile(&metaData, &version)
		if err != nil {
			log.Fatal("when UpdateFile", err)
		}
		log.Println("finished uploading", fileName)
	}

	err = client.GetFileInfoMap(&cloudMetaMap)
	if err != nil {
		log.Fatal(err)
	}
	// write back to index.db
	err = WriteMetaFile(cloudMetaMap, client.BaseDir)
	if err != nil {
		log.Println("when WriteMetaFile", err)
	}
}

func fileToHashesAndBlocks(content []byte, blockSize int) ([]string, []Block) {
	if len(content) == 0 {
		return []string{"-1"}, []Block{{BlockData: content, BlockSize: int32(blockSize)}}
	}
	idx := 0
	blocks := make([]Block, 0)
	hashList := make([]string, 0)
	if len(content) <= blockSize {
		block := Block{BlockData: content, BlockSize: int32(blockSize)}
		return append(hashList, GetBlockHashString(content)), append(blocks, block)
	}
	for idx < len(content) {
		if idx+blockSize >= len(content) {
			// log.Println("hi", string(content[idx:]), idx)
			block := Block{BlockData: content[idx:], BlockSize: int32(blockSize)}
			blocks = append(blocks, block)
			hashList = append(hashList, GetBlockHashString(content[idx:]))
			break
		}
		// log.Println(string(content[idx : idx+blockSize]), idx)
		block := Block{BlockData: content[idx : idx+blockSize], BlockSize: int32(blockSize)}
		blocks = append(blocks, block)
		hashList = append(hashList, GetBlockHashString(content[idx:idx+blockSize]))
		idx += blockSize
	}
	return hashList, blocks
}

func compareStringSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

// RPCClient{
// 	MetaStoreAddr: hostPort,
// 	BaseDir:       baseDir,
// 	BlockSize:     blockSize,
// }
