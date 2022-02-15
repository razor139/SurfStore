package surfstore

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"reflect"
)

func createFileHashList(path string, blockSize int) ([]string, map[string]*Block, error) {
	var hashList []string
	var hashBlocks = make(map[string]*Block)
	f, err := os.Open(path)
	if err != nil {
		fmt.Printf("Error in opening file:%v\n", err)
		return hashList, hashBlocks, err
	}
	defer f.Close()
	hash := make([]byte, blockSize)
	for {
		n, err := f.Read(hash)
		if err != nil {
			if err != io.EOF {
				return hashList, hashBlocks, err
			}
			break
		}
		hashString := GetBlockHashString(hash[:n])
		//fmt.Println("Hash :", hashString)
		block := &Block{BlockData: []byte(string(hash[:n])), BlockSize: int32(n)}
		hashBlocks[hashString] = block
		//fmt.Println(hashBlocks)
		hashList = append(hashList, hashString)
	}
	//fmt.Println(hashBlocks)

	// for key, value := range hashBlocks {
	// 	fmt.Println("HAsh:", key, "block data:", value.GetBlockData()[:value.BlockSize])
	// }
	return hashList, hashBlocks, nil
}

func diffLocal(localFileData map[string]*FileMetaData, BaseDir string) []string {
	localIndexMeta, _ := LoadMetaFromMetaFile(BaseDir)
	var localChanges []string
	for localFile, metaData := range localFileData {
		if _, ok := localIndexMeta[localFile]; ok {
			if !reflect.DeepEqual(metaData.GetBlockHashList(), localIndexMeta[localFile].GetBlockHashList()) {
				localChanges = append(localChanges, localFile)
			}
		} else {
			localChanges = append(localChanges, localFile)
		}
	}
	return localChanges
}

func diffIndex(indexA map[string]*FileMetaData, indexB map[string]*FileMetaData) []string {
	var changes []string
	for localFile := range indexA {
		if _, ok := indexB[localFile]; !ok {
			changes = append(changes, localFile)
		}
	}

	return changes
}

// Downloading file
func downloadFromRemote(client RPCClient, fileData *FileMetaData, blockStoreAddr string) error {

	remoteFileHash := fileData.GetBlockHashList()
	outputFilePath, _ := filepath.Abs(ConcatPath(client.BaseDir, fileData.Filename))

	if len(remoteFileHash) == 1 && remoteFileHash[0] == "0" {
		os.Remove(outputFilePath)
		return nil
	}

	f, err := os.OpenFile(outputFilePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		log.Println(err)
	}
	defer f.Close()

	block := Block{}
	for _, hash := range remoteFileHash {
		err := client.GetBlock(hash, blockStoreAddr, &block)
		if err != nil {
			fmt.Printf("Did not recieve block:%v\n", err)
			return err
		}

		_, err = f.Write(block.BlockData[0:block.BlockSize])
		if err != nil {
			fmt.Printf("Could not write into file\n")
		}
	}
	f.Sync()
	return nil
}

// Diff between Hash Lists
func diffHashList(listA, listB []string) []string {

	var diffList []string
	hashMap := make(map[string]bool)
	for _, hash := range listB {
		hashMap[hash] = true
	}

	for _, hash := range listA {
		if _, ok := hashMap[hash]; !ok {
			diffList = append(diffList, hash)
		}
	}

	return diffList
}

// Uploading to Server
func uploadToRemote(client RPCClient, indexfileData *FileMetaData, remotefileData *FileMetaData, hashBlocks map[string]*Block, blockStoreAddr string) error {

	indexHashList := indexfileData.BlockHashList
	remoteHashList := remotefileData.GetBlockHashList()

	diffList := diffHashList(indexHashList, remoteHashList)

	var remoteHashSubList []string
	err := client.HasBlocks(indexHashList, blockStoreAddr, &remoteHashSubList)
	if err != nil {
		fmt.Printf("Error while calling HasBlocks:%v", err)
		return err
	}

	hashToUpload := diffHashList(diffList, remoteHashSubList)

	for _, hash := range hashToUpload {
		var succ bool
		block := Block{BlockData: hashBlocks[hash].BlockData, BlockSize: hashBlocks[hash].BlockSize}
		//fmt.Println("Hash:", hash, "Block: ", string(hashBlocks[hash].BlockData))
		err := client.PutBlock(&block, blockStoreAddr, &succ)
		if err != nil {
			fmt.Printf("Could not put block properly:%v\n", err)
			return err
		}
	}
	return nil
}

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	//panic("todo")
	files, err := ioutil.ReadDir(client.BaseDir)
	if err != nil {
		fmt.Printf("Error in reading directory:%v\n", err)
	}

	localIndex, _ := LoadMetaFromMetaFile(client.BaseDir)
	//PrintMetaMap(localIndex)
	remoteIndex := make(map[string]*FileMetaData)
	err = client.GetFileInfoMap(&remoteIndex)
	if err != nil {
		fmt.Printf("Could not get remote index:%v\n", err)
		return // Revisit
	}

	//PrintMetaMap(remoteIndex)
	var blockStoreAddr string
	err = client.GetBlockStoreAddr(&blockStoreAddr)
	if err != nil {
		fmt.Printf("Could not get block store addr:%v\n", err)
		return // Revisit
	}

	localFileData := make(map[string]*FileMetaData)
	for _, file := range files {
		fileName := file.Name()
		//fmt.Println(fileName)
		if fileName == DEFAULT_META_FILENAME {
			continue
		}
		path, _ := filepath.Abs(ConcatPath(client.BaseDir, fileName))

		//PrintMetaMap(localFileData)
		localIndexMeta := localIndex[fileName]
		remoteIndexMeta := remoteIndex[fileName]

		if remoteIndexMeta.GetVersion() > localIndexMeta.GetVersion() {
			// Download and update local index map
			//fmt.Println("downloading Changes in file:", fileName)
			err := downloadFromRemote(client, remoteIndexMeta, blockStoreAddr)
			if err != nil {
				log.Printf("Could not download properly:%v\n", err)
			}

			localIndex[fileName] = remoteIndexMeta
			localFileData[fileName] = remoteIndexMeta
		} else {
			fileHashList, hashBlocks, _ := createFileHashList(path, client.BlockSize)
			localFileData[fileName] = &FileMetaData{Filename: fileName, BlockHashList: fileHashList}

			localIndexHashList := localIndexMeta.GetBlockHashList()

			if !reflect.DeepEqual(fileHashList, localIndexHashList) {
				// Upload data to server, if version error then download the content and update local index
				// Handle versions while uploading
				//fmt.Println(" Changes in file:", fileName)
				localFileData[fileName].Version = localIndex[fileName].GetVersion() + 1
				err := uploadToRemote(client, localFileData[fileName], remoteIndexMeta, hashBlocks, blockStoreAddr)
				if err != nil {
					fmt.Printf("Could not upload:%v\n", err)
					continue
				}

				newVersion := localFileData[fileName].GetVersion()
				err = client.UpdateFile(localFileData[fileName], &newVersion)
				if err != nil {
					fmt.Printf("Could not update file info:%v\n", err)
				}
				if newVersion == -1 {
					fmt.Println("Remote server has a newer version so download remote file")
					err = client.GetFileInfoMap(&remoteIndex)
					remoteIndexMeta := remoteIndex[fileName]

					err := downloadFromRemote(client, remoteIndexMeta, blockStoreAddr)
					if err != nil {
						fmt.Printf("Could not download properly:%v\n", err)
					}

					localIndex[fileName] = remoteIndexMeta

				} else {

					localIndex[fileName] = localFileData[fileName]

				}
			}
		}
	}

	// Handling locally deleted files
	localDeletedFiles := diffIndex(localIndex, localFileData)
	//fmt.Println("Locally deleted", localDeletedFiles)
	// For every locally deleted file upload to server with tombstone record.
	for _, deleteFile := range localDeletedFiles {
		localIndexMeta := localIndex[deleteFile]
		remoteIndexMeta := remoteIndex[deleteFile]

		if remoteIndexMeta.GetVersion() > localIndexMeta.GetVersion() {
			// Download and update local index map
			err := downloadFromRemote(client, remoteIndexMeta, blockStoreAddr)
			if err != nil {
				log.Printf("Could not download properly:%v\n", err)
			}

			localIndex[deleteFile] = remoteIndexMeta

		} else if !(len(localIndexMeta.BlockHashList) == 1 && localIndexMeta.BlockHashList[0] == "0") {
			newVersion := localIndexMeta.Version + 1
			deleteHashList := []string{"0"}
			deleteFileData := FileMetaData{Filename: deleteFile, Version: newVersion, BlockHashList: deleteHashList}
			err = client.UpdateFile(&deleteFileData, &newVersion)
			if err != nil {
				log.Printf("Could not update file info:%v\n", err)
			}
			if newVersion == -1 {
				log.Println("Remote server has a newer version so download remote file")
				err = client.GetFileInfoMap(&remoteIndex)
				remoteIndexMeta := remoteIndex[deleteFile]

				err := downloadFromRemote(client, remoteIndexMeta, blockStoreAddr)
				if err != nil {
					log.Printf("Could not download properly:%v\n", err)
				}

				localIndex[deleteFile] = remoteIndexMeta
			} else {

				localIndex[deleteFile] = &deleteFileData
			}
		}

	}

	// Check if remote index has new files.
	newRemoteFiles := diffIndex(remoteIndex, localIndex)
	//fmt.Println("New remote files:", newRemoteFiles)
	// For every New Remote file just download and update local index.
	for _, file := range newRemoteFiles {
		remoteIndexMeta := remoteIndex[file]
		err := downloadFromRemote(client, remoteIndexMeta, blockStoreAddr)
		if err != nil {
			fmt.Printf("Could not download properly:%v\n", err)
		}

		localIndex[file] = remoteIndexMeta
	}

	//PrintMetaMap(localIndex)
	WriteMetaFile(localIndex, client.BaseDir)
}
