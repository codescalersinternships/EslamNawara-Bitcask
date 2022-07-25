package bitcask

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// Helpers
func createBitcask(dirPath string, cfg []ConfigOptions) Bitcask {
	var config ConfigOptions
	if len(cfg) > 0 {
		config = cfg[0]
	} else {
		config = ConfigOptions{
			accessPermission: ReadOnly,
			syncOption:       false,
		}
	}
	bc := Bitcask{
		keydir:       make(keyDir),
		directory:    dirPath,
		penWrites:    make(pendingWrites),
		activeFileId: int(time.Now().UnixNano()),
		dirOpts: ConfigOptions{
			accessPermission: config.accessPermission,
			syncOption:       config.syncOption,
		},
	}
	if config.accessPermission == WritingPermession {
		bc.lockDir()
	}
	return bc
}

func fetchBitcask(dirPath string, config []ConfigOptions) Bitcask {
	bc := createBitcask(dirPath, config)
	//  read the hint file line by line to get every key and record and add them to the bitcask and return it
	file, _ := os.OpenFile(filepath.Join(bc.directory, hintFile), os.O_CREATE|os.O_RDONLY, 0777)
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lineBytes := scanner.Bytes()
		if len(lineBytes) > 20 {
			key := string(lineBytes[20:])
			keydirField := record{
				fileId:    int(binary.BigEndian.Uint32(lineBytes[16:20])),
				valueSize: int(binary.BigEndian.Uint32(lineBytes[4:8])),
				valuePos:  int(binary.BigEndian.Uint32(lineBytes[8:12])),
				tStamp:    int(binary.BigEndian.Uint32(lineBytes[12:16])),
			}
			bc.keydir[key] = keydirField
		}
	}
	return bc
}

func (bc *Bitcask) lockDir() {
	//create a lockfile like pacman
	lockFilePath := filepath.Join(bc.directory, lockFile)
	os.Create(lockFilePath)
}

func dirLocked(dirPath string) bool {
	// return true if db.lck exist
	lockFilePath := filepath.Join(dirPath, lockFile)
	if _, err := os.Stat(lockFilePath); errors.Is(err, os.ErrNotExist) {
		return false
	}
	return true
}

func (bc *Bitcask) unlockDir() {
	//remove the lockfile
	if bc.checkWritingPermission() == nil {
		lockFilePath := filepath.Join(bc.directory, lockFile)
		os.Remove(lockFilePath)
	}
}

func (bc *Bitcask) checkWritingPermission() error {
	if bc.dirOpts.accessPermission == ReadOnly {
		return fmt.Errorf("Writing permession denied in directory %s", bc.directory)
	}
	return nil
}

func (bc *Bitcask) buildHintFileRecord(key string, rec record) []byte {
	elem := make([]byte, 20, 20+len(key+"\n"))
	binary.BigEndian.PutUint32(elem[:], uint32(len(key)))
	binary.BigEndian.PutUint32(elem[4:], uint32(rec.valueSize))
	binary.BigEndian.PutUint32(elem[8:], uint32(rec.valuePos))
	binary.BigEndian.PutUint32(elem[12:], uint32(rec.tStamp))
	binary.BigEndian.PutUint32(elem[16:], uint32(rec.fileId))
	elem = append(elem, key+"\n"...)
	return elem
}

func (bc *Bitcask) buildHintFile() {
	hintFile := filepath.Join(bc.directory, hintFile)
	os.Remove(hintFile)
	file, _ := os.OpenFile(hintFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0777)
	//have to sync before
	bc.Sync()
	for key, val := range bc.keydir {
		elem := bc.buildHintFileRecord(key, val)
		file.Write(elem)
	}
	defer file.Close()
}

func (bc *Bitcask) fetchValueFromFile(key string) (string, error) {
	recForKey, _ := bc.keydir[key]

	currentActiveFile := strconv.Itoa(recForKey.fileId)
	if recForKey.fileId == bc.activeFileId {
		currentActiveFile = activeFile
	}
	file, _ := os.Open(filepath.Join(bc.directory, currentActiveFile))
	file.Seek(int64(recForKey.valuePos+16+len(key)), 0)
	value := make([]byte, recForKey.valueSize)
	file.Read(value)
	return string(value), nil
}

func (bc *Bitcask) buildActiveFileRecord(rec fileRecord) []byte {
	elem := make([]byte, 16, 16+rec.keySz+rec.valSz)
	binary.BigEndian.PutUint32(elem[4:], uint32(rec.tStamp))
	binary.BigEndian.PutUint32(elem[8:], uint32(rec.keySz))
	binary.BigEndian.PutUint32(elem[12:], uint32(rec.valSz))
	elem = append(elem, rec.key...)
	elem = append(elem, rec.value...)
	crc := crc32.ChecksumIEEE(elem[4:])
	binary.BigEndian.PutUint32(elem[:4], crc)
	return elem
}

func (bc *Bitcask) buildMergedFileRecord(key string, rec record) []byte {
	val, _ := bc.fetchValueFromFile(key)
	rc := fileRecord{
		tStamp: rec.tStamp,
		keySz:  len([]byte(key)),
		valSz:  rec.valueSize,
		key:    key,
		value:  val,
	}
	return bc.buildActiveFileRecord(rc)
}

func (bc *Bitcask) buildMergedFiles() {
	fId := int(time.Now().UnixNano())
	mergedFile := filepath.Join(bc.directory, "m"+strconv.Itoa(fId))
	file, _ := os.OpenFile(mergedFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
	defer file.Close()
	pos := 0
	for key, val := range bc.keydir {
		if val.fileId != bc.activeFileId {
			elem := bc.buildMergedFileRecord(key, val)
			bc.keydir[key] = record{
				fileId:    fId,
				tStamp:    val.tStamp,
				valueSize: val.valueSize,
				isPending: false,
				valuePos:  pos,
			}
			file.Write(elem)
			pos += len(elem)
		}
	}
}

func  addReader(dir string) {
	path := filepath.Join(dir, readerLock)
	data, err := os.ReadFile(path)
	if err != nil {
		os.WriteFile(path, []byte("1"), 0777)
	}
	readers, err := strconv.Atoi(string(data))
	os.WriteFile(path, []byte(strconv.Itoa(readers+1)), 0777)
}

func removeReader(dir string) {
	path := filepath.Join(dir, readerLock)
	data, _ := os.ReadFile(path)
	readers, _ := strconv.Atoi(string(data))
	if readers == 1 {
		os.Remove(path)
	} else {
		os.WriteFile(path, []byte(strconv.Itoa(readers+1)), 0777)
	}
}

func readerExist(dir string) bool {
	path := filepath.Join(dir, readerLock)
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return false
	}
	return true
}
