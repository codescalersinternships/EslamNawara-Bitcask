package bitcask

import (
	"bufio"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Helpers
func createBitcask(dirPath string, cfg []ConfigOptions) Bitcask {
	var config ConfigOptions
	if len(cfg) > 0 {
		config = cfg[0]
	} else {
		config = ConfigOptions{
			AccessPermission: ReadOnly,
			SyncOption:       false,
		}
	}
	bc := Bitcask{
		keydir:       make(keyDir),
		directory:    dirPath,
		penWrites:    make(pendingWrites),
		activeFileId: time.Now().UnixNano(),
		dirOpts: ConfigOptions{
			AccessPermission: config.AccessPermission,
			SyncOption:       config.SyncOption,
		},
	}
	if config.AccessPermission == WritingPermession {
		bc.lockDir()
	}
	return bc
}

func fetchBitcask(dirPath string, config []ConfigOptions) Bitcask {
	bc := createBitcask(dirPath, config)
	//  read the hint file line by line to get every key and record and add them to the bitcask and return it
	file, _ := os.ReadFile(filepath.Join(bc.directory, hintFile))
	scanner := bufio.NewScanner(strings.NewReader(string(file)))
	for scanner.Scan() {
		lineBytes := scanner.Bytes()
		ts, _ := strconv.ParseInt(string(lineBytes[0:20]), 10, 64)
		fId, _ := strconv.ParseInt(string(lineBytes[21:41]), 10, 64)
		vSz, _ := strconv.Atoi(string(lineBytes[42:52]))
		vPos, _ := strconv.Atoi(string(lineBytes[53:63]))
		key := string(lineBytes[64:])
		bc.keydir[key] = record{
			fileId:    fId,
			valueSize: vSz,
			valuePos:  vPos,
			tStamp:    ts,
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
	if bc.dirOpts.AccessPermission == ReadOnly {
		return fmt.Errorf("Writing permession denied in directory %s", bc.directory)
	}
	return nil
}

func (bc *Bitcask) buildHintFileRecord(key string, rec record) []byte {
	vSz := padWithZero(rec.valueSize)
	vPos := padWithZero(rec.valuePos)
	ts := padInt64WithZero(rec.tStamp)
	fid := padInt64WithZero(rec.fileId)
	return []byte(ts + " " + fid + " " + vSz + " " + vPos + " " + key + "\n")
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
	fileName := strconv.FormatInt(recForKey.fileId, 10)
	if recForKey.fileId == bc.activeFileId {
		fileName = activeFile
	}
	file, _ := os.Open(filepath.Join(bc.directory, fileName))
	file.Seek(int64(recForKey.valuePos+50+len(key)), 0)
	value := make([]byte, recForKey.valueSize)
	file.Read(value)
	return string(value), nil
}

func (bc *Bitcask) buildActiveFileRecord(rec fileRecord) []byte {
	kSz := padWithZero(rec.keySz)
	ts := padInt64WithZero(rec.tStamp)
	vSz := padWithZero(rec.valSz)
	elem := []byte(ts + kSz + vSz + rec.key + rec.value)
	crc := crc32.ChecksumIEEE(elem)
	crcS := padWithZero(int(crc))
	return []byte(crcS + ts + kSz + vSz + rec.key + rec.value)
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
	fId := time.Now().UnixNano()
	mergedFile := filepath.Join(bc.directory, strconv.FormatInt(fId, 10))
	file, _ := os.OpenFile(mergedFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
	defer file.Close()
	newKeyDir := make(keyDir)
	pos := 0
	for key, val := range bc.keydir {
		if val.fileId != bc.activeFileId {
			elem := bc.buildMergedFileRecord(key, val)
			newKeyDir[key] = record{
				fileId:    fId,
				tStamp:    val.tStamp,
				valueSize: val.valueSize,
				isPending: false,
				valuePos:  pos,
			}
			file.Write(elem)
			pos += len(elem)
		} else {
			newKeyDir[key] = bc.keydir[key]
		}
	}
	bc.keydir = newKeyDir
}

func addReader(dir string) {
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

func padWithZero(val int) string {
	return fmt.Sprintf("%010d", val)
}
func padInt64WithZero(val int64) string {
	return fmt.Sprintf("%020d", val)
}
