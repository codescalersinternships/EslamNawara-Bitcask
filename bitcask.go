package bitcask

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
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
	//defines the isTypeLocked value
	LockedForWriting = true
	OpenForRW        = false
	//defines the accessPermission option
	WritingPermession = true
	ReadOnly          = false
)

var (
	activeFile int
)

type Bitcask struct {
	activeFile string
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
		return "", fmt.Errorf("Key %s not found in the directory", key)
	}
	if val.isPending {
		value := bc.penWrites[key].value
		if value == TOMPSTONE {
			return "", fmt.Errorf("The value of the key %s  was deleted", key)
		}
		return value, nil
	}
	return bc.fetchValueFromFile(key), nil
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
	if _, exist := bc.keydir[key]; !exist {
		return fmt.Errorf("Key %s Not found in the directory %s", key, bc.directory)
	}
	return bc.Put(key, TOMPSTONE)
}

func (bc *Bitcask) ListKeys() []string {
	keys := []string{}
	for key := range bc.keydir {
		keys = append(keys, key)
	}
	return keys
}

func (bc *Bitcask) Fold(foldFunc func(string, int) int, acc int) int {
	for key := range bc.keydir {
		acc = foldFunc(key, acc)
	}
	return acc
}

func (bc *Bitcask) Merge() error {
	if err := bc.checkWritingPermission(); err != nil {
		return err
	}
	bc.Sync()
	oldFiles, _ := os.ReadDir(bc.directory)

	bc.buildHintFile()
	bc.buildMergedFiles()
	for _, file := range oldFiles {
		filePath := filepath.Join(bc.directory, file.Name())
		os.Remove(filePath)
	}
	os.Rename(filepath.Join(bc.directory, "m"), filepath.Join(bc.directory, "0"))
	return nil
}

func (bc *Bitcask) Sync() error {
	if err := bc.checkWritingPermission(); err != nil {
		return err
	}
	currentActiveFile := filepath.Join(bc.directory, strconv.Itoa(activeFile))
	file, _ := os.OpenFile(currentActiveFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
	fi, _ := os.Stat(currentActiveFile)
	pos := int(fi.Size())
	for key, val := range bc.penWrites {
		if val.value == TOMPSTONE {
			delete(bc.keydir, key)
		}
		if state, _ := os.Stat(currentActiveFile); state.Size() >= activeMaxSize {
			file.Close()
			pos = 0
			activeFile++
			currentActiveFile := filepath.Join(bc.directory, strconv.Itoa(activeFile))
			file, _ = os.OpenFile(currentActiveFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
		}
		bc.keydir[key] = record{
			fileId:    activeFile,
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

func (bc *Bitcask) Close() error {
	bc.Merge()
	bc.unlockDir()
	return nil
}
