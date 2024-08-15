package surfstore

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"

	_ "github.com/mattn/go-sqlite3"
)

/* Hash Related */
func GetBlockHashBytes(blockData []byte) []byte {
	h := sha256.New()
	h.Write(blockData)
	return h.Sum(nil)
}

func GetBlockHashString(blockData []byte) string {
	blockHash := GetBlockHashBytes(blockData)
	return hex.EncodeToString(blockHash)
}

/* File Path Related */
func ConcatPath(baseDir, fileDir string) string {
	return baseDir + "/" + fileDir
}

/*
	Writing Local Metadata File Related
*/

const createTable string = `CREATE TABLE IF NOT EXISTS indexes (
		fileName TEXT, 
		version INT,
		hashIndex INT,
		hashValue TEXT
	);`

const insertTuple string = `INSERT INTO indexes(fileName, version, hashIndex, hashValue) VALUES('aaa',1,0,a);`

func generateInsertTuple(fileMetas map[string]*FileMetaData) string {
	tupleString := "INSERT INTO indexes(fileName, version, hashIndex, hashValue) VALUES"
	for name, meta := range fileMetas {
		for idx, hash := range meta.BlockHashList {
			tuple := "('" + name + "'," + strconv.Itoa(int(meta.Version)) + "," + strconv.Itoa(idx) + "," + "'" + hash + "'),"
			tupleString += tuple
		}
	}
	tupleString = tupleString[:len(tupleString)-1]
	tupleString += ";"
	return tupleString
}

// WriteMetaFile writes the file meta map back to local metadata file index.db
func WriteMetaFile(fileMetas map[string]*FileMetaData, baseDir string) error {
	// remove index.db file if it exists
	outputMetaPath := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	log.Println("writing", outputMetaPath)
	if _, err := os.Stat(outputMetaPath); err == nil {
		e := os.Remove(outputMetaPath)
		if e != nil {
			log.Fatal("Error During Remove index.db", err)
		}
	}
	db, err := sql.Open("sqlite3", outputMetaPath)
	if err != nil {
		log.Fatal("Error During Open index.db", err)
	}
	statement, err := db.Prepare(createTable)
	if err != nil {
		log.Fatal("Error During Prepare index.db ", err)
	}
	statement.Exec()

	// if map is empty
	if len(fileMetas) == 0 {
		return nil
	}

	insertString := generateInsertTuple(fileMetas)
	// log.Println(insertString)
	_, err = db.Exec(insertString)
	if err != nil {
		log.Fatal("Error During Meta Write Back ", err)
	}
	return nil
}

/*
Reading Local Metadata File Related
*/
const getDistinctFileName string = ``

const getTuplesByFileName string = ``

// LoadMetaFromMetaFile loads the local metadata file into a file meta map.
// The key is the file's name and the value is the file's metadata.
// You can use this function to load the index.db file in this project.
func LoadMetaFromMetaFile(baseDir string) (fileMetaMap map[string]*FileMetaData, e error) {
	metaFilePath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))
	fileMetaMap = make(map[string]*FileMetaData)
	metaFileStats, e := os.Stat(metaFilePath)
	if e != nil || metaFileStats.IsDir() {
		return fileMetaMap, nil
	}
	db, err := sql.Open("sqlite3", metaFilePath)
	if err != nil {
		log.Fatal("Error When Opening Meta", err)
	}

	rows, err := db.Query("SELECT * FROM indexes")
	if err != nil {
		return fileMetaMap, nil
	}
	defer rows.Close()

	for rows.Next() {
		var filename string
		var version int32
		var hashIndex int
		var hashValue string
		err = rows.Scan(&filename, &version, &hashIndex, &hashValue)
		if err != nil {
			return fileMetaMap, err
		}
		// Todo: can we assume that the hashIndex is in order in the first place?
		if val, ok := fileMetaMap[filename]; ok {
			val.BlockHashList = append(val.BlockHashList, hashValue)
		} else {
			meta := FileMetaData{Filename: filename, Version: version, BlockHashList: []string{hashValue}}
			fileMetaMap[filename] = &meta
		}
	}
	return fileMetaMap, nil
}

/*
	Debugging Related
*/

// PrintMetaMap prints the contents of the metadata map.
// You might find this function useful for debugging.
func PrintMetaMap(metaMap map[string]*FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version)
		for _, blockHash := range filemeta.BlockHashList {
			fmt.Println("\t", blockHash)
		}
	}

	fmt.Println("---------END PRINT MAP--------")

}
