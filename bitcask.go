package bitcask

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

const (
	pendingMaxSize      = 50
	activeMaxSize       = 10000 // 10 kb per active File
	numOfEssentialFiles = 3
	TOMPSTONE           = "TOMPSTONE"
	hintFile            = "hintFile"
	lockFile            = "db.lck"
	activeFile          = "activeFile"
	activeFileId        = 0
	//defines the isTypeLocked value
	LockedForWriting = true
	OpenForRW        = false
	//defines the accessPermission option
	WritingPermession = true
	ReadOnly          = false
)

var ()

type Bitcask struct {
	directory  string
	keydir     keyDir
	dirOpts    ConfigOptions
	penWrites  pendingWrites
}

type keyDir map[string]record
type record struct {
	fileId    int
	valueSize int
	valuePos  int
	tStamp    int64
	isPending bool
}

type pendingWrites map[string]fileRecord
type fileRecord struct {
	tStamp int64
	keySz  int
	valSz  int
	key    string
	value  string
}

type ConfigOptions struct {
	accessPermission bool
	syncOption       bool
}

func Open(dirPath string, config ...ConfigOptions) (Bitcask, error) {
	bc := Bitcask{}
	if _, err := os.Stat(dirPath); errors.Is(err, os.ErrNotExist) {
		if len(config) > 0 && config[0].accessPermission == WritingPermession {
			os.Mkdir(dirPath, 0777)
			bc = createBitcask(dirPath, config)
			return bc, fmt.Errorf("New Directory was created in the path %s", dirPath)
		} else {
			return bc, fmt.Errorf("No such a file or directory %s\nCan't create directory in the path%s", dirPath, dirPath)
		}
	}
	if dirLocked(dirPath) {
		return bc, fmt.Errorf("The directory %s is type locked you can't read or write from it", dirPath)
	}
	bc = fetchBitcask(dirPath, config)
	return bc, nil
}

func (bc *Bitcask) Get(key string) (string, error) {
	val, exist := bc.keydir[key]
	if !exist {
		return "", fmt.Errorf("Key %s not found in the directory %s", key, bc.directory)
	}
	if val.isPending {
		value := bc.penWrites[key].value
		if value == TOMPSTONE {
			return "", fmt.Errorf("Key %s not found in the directory %s", key, bc.directory)
		}
		return value, nil
	}
	return bc.fetchValueFromFile(key)
}

func (bc *Bitcask) Put(key, val string) error {
	if err := bc.checkWritingPermission(); err != nil {
		return err
	}
	if len(bc.penWrites) >= pendingMaxSize {
		bc.Sync()
	}
	bc.penWrites[key] = fileRecord{
		tStamp: time.Now().Unix(),
		keySz:  len([]byte(key)),
		valSz:  len([]byte(val)),
		key:    key,
		value:  val,
	}

	bc.keydir[key] = record{
		tStamp:    time.Now().Unix(),
		valueSize: len([]byte(val)),
		isPending: true,
	}
	return nil
}

func (bc *Bitcask) Delate(key string) error {
	return bc.Put(key, TOMPSTONE)
}

func (bc *Bitcask) ListKeys() []string {
	keys := []string{}
	for key := range bc.keydir {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func (bc *Bitcask) Fold(foldFunc func(string, string, any) any, acc any) any{
	for key := range bc.keydir {
		val, _ := bc.Get(key)
		acc = foldFunc(key, val, acc)
	}
	return acc
}

func (bc *Bitcask) Merge() error {
	if err := bc.checkWritingPermission(); err != nil {
		return err
	}
	bc.Sync()
	oldFiles, _ := os.ReadDir(bc.directory)
	bc.buildMergedFiles()
	for _, file := range oldFiles {
		if file.Name() == hintFile || file.Name() == activeFile {
			filePath := filepath.Join(bc.directory, file.Name())
			os.Remove(filePath)
		}
	}
	os.Rename(filepath.Join(bc.directory, "m"), filepath.Join(bc.directory, "0"))
	bc.buildHintFile()
	return nil
}

func (bc *Bitcask) Sync() error {
	if err := bc.checkWritingPermission(); err != nil {
		return err
	}
	currentActiveFile := filepath.Join(bc.directory, activeFile)
	file, _ := os.OpenFile(currentActiveFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
	fi, _ := os.Stat(currentActiveFile)
	pos := int(fi.Size())
	temp, _ := os.ReadDir(bc.directory)
	numOfFiles := len(temp)
	for key, val := range bc.penWrites {
		if val.value == TOMPSTONE {
			delete(bc.keydir, key)
		}
		if state, _ := os.Stat(currentActiveFile); state.Size() >= activeMaxSize {
			file.Close()
			pos = 0
			os.Rename(currentActiveFile, filepath.Join(bc.directory, strconv.Itoa(numOfFiles)))
			file, _ = os.OpenFile(currentActiveFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
		}
		bc.keydir[key] = record{
			fileId:    activeFileId,
			tStamp:    val.tStamp,
			valuePos:  pos,
			valueSize: val.valSz,
			isPending: false,
		}
		rec := bc.buildActiveFileRecord(val)
		pos += len(rec)
		file.Write(rec)
	}
	bc.penWrites = make(pendingWrites)
	defer file.Close()
	return nil
}

func (bc *Bitcask) Close() {
	if err := bc.checkWritingPermission(); err == nil {
		bc.Sync()
		bc.buildHintFile()
		bc.unlockDir()
	}
}
