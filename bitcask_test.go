package bitcask

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
)

func TestOpen(t *testing.T) {
	t.Run("Non existing directory with writing permession", func(t *testing.T) {
		path := filepath.Join(filepath.Join("testing", "testOpen"), "NonEsisting")
		bc, err := Open(path, ConfigOptions{accessPermission: WritingPermession, syncOption: false})
		want := fmt.Sprintf("New Directory was created in the path %s", path)
		got := err.Error()
		bc.unlockDir()
		os.Remove(path)
		if got != want {
			t.Errorf("expected %v but got %v", want, got)
		}
	})

	t.Run("Non existing directory with no writing permession", func(t *testing.T) {
		path := filepath.Join(filepath.Join("testing", "testOpen"), "NonEsisting")
		_, err := Open(path)
		want := fmt.Sprintf("No such a file or directory %s\nCan't create directory in the path%s", path, path)
		got := err.Error()
		if got != want {
			t.Errorf("expected %v but got %v", want, got)
		}
	})

	t.Run("read from locked dir", func(t *testing.T) {
		path := filepath.Join(filepath.Join("testing", "testOpen"), "lockedDir")
		Open(path, ConfigOptions{WritingPermession, false})
		bc, err := Open(path, ConfigOptions{ReadOnly, false})
		want := fmt.Sprintf("The directory %s is type locked you can't read or write from it", path)
		got := err.Error()
		bc.Close()
		if got != want {
			t.Errorf("expected %v but got %v", want, got)
		}
	})
}

func TestPut(t *testing.T) {
	t.Run("Put with permession", func(t *testing.T) {
		path := filepath.Join("testing", "testPut")
		bc, _ := Open(path, ConfigOptions{WritingPermession, false})
		bc.Put("Name", "Eslam")
		got := len(bc.keydir)
		want := 1
		bc.Close()
		if got != want {
			t.Errorf("expected %v but got %v", want, got)
		}
	})

	t.Run("Put with no permession", func(t *testing.T) {
		path := filepath.Join("testing", "testPut")
		bc, _ := Open(path)
		got := bc.Put("Name", "Eslam").Error()
		want := fmt.Sprintf("Writing permession denied in directory %s", path)
		bc.Close()
		if got != want {
			t.Errorf("expected %v but got %v", want, got)

		}
	})
}

func TestDelete(t *testing.T) {
	t.Run("Delate with permession", func(t *testing.T) {
		path := filepath.Join("testing", "testDelete")
		bc, _ := Open(path, ConfigOptions{WritingPermession, false})
		key := "Name"
		bc.Put(key, "Eslam")
		bc.Delate(key)
		_, err := bc.Get(key)
		want := fmt.Sprintf("Key %s not found in the directory %s", key, path)
		got := err.Error()
		bc.Close()
		if got != want {
			t.Errorf("expected %v but got %v", want, got)
		}
	})

	t.Run("key not found", func(t *testing.T) {
		path := filepath.Join("testing", "testDelete")
		bc, _ := Open(path)
		got := bc.Delate("Age").Error()
		want := fmt.Sprintf("Writing permession denied in directory %s", path)
		bc.Close()
		if got != want {
			t.Errorf("expected %v but got %v", want, got)
		}
	})
}

func TestListKeys(t *testing.T) {
	path := filepath.Join("testing", "testListKeys")
	bc, _ := Open(path, ConfigOptions{WritingPermession, false})
	bc.Put("Name", "Eslam")
	bc.Put("Age", "22")
	want := []string{"Age", "Name"}
	bc.Close()
	got := bc.ListKeys()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("expected %v but got %v", want, got)
	}
}

func TestGet(t *testing.T) {
	t.Run("Get pending value", func(t *testing.T) {
		path := filepath.Join("testing", "testGet", "validValue")
		config := ConfigOptions{WritingPermession, false}
		bc, _ := Open(path, config)
		bc.Put("Name", "Eslam")
		got, _ := bc.Get("Name")
		want := "Eslam"
		bc.Close()
		if got != want {
			t.Errorf("expected %v but got %v", want, got)
		}
	})

	t.Run("Get not existing value", func(t *testing.T) {
		path := filepath.Join("testing", "testGet", "invalidValue")
		config := ConfigOptions{WritingPermession, false}
		bc, _ := Open(path, config)
		key := "Age"
		want := fmt.Sprintf("Key %s not found in the directory %s", key, path)
		_, err := bc.Get(key)
		got := err.Error()
		bc.Close()
		if got != want {
			t.Errorf("expected %v but got %v", want, got)
		}
	})

	t.Run("Get synced value", func(t *testing.T) {
		path := filepath.Join("testing", "testGet", "syncedDir")
		config := ConfigOptions{WritingPermession, true}
		bc, _ := Open(path, config)
		bc.Put("Name", "Eslam")
		bc.Sync()
		got, _ := bc.Get("Name")
		want := "Eslam"
		bc.Close()
		if got != want {
			t.Errorf("expected %v but got %v", want, got)
		}
	})
	t.Run("Get synced invalid value", func(t *testing.T) {
		path := filepath.Join("testing", "testGet", "syncedDirInvalid")
		config := ConfigOptions{WritingPermession, true}
		bc, _ := Open(path, config)
		bc.Put("Name", "Eslam")
		bc.Sync()
		key := "Age"
		_, err := bc.Get(key)
		want := fmt.Sprintf("Key %s not found in the directory %s", key, path)
		got := err.Error()
		bc.Close()
		if got != want {
			t.Errorf("expected %v but got %v", want, got)
		}
	})
	t.Run("Get Merged value", func(t *testing.T) {
		path := filepath.Join("testing", "testGet", "mergedDir")
		config := ConfigOptions{WritingPermession, false}
		bc, _ := Open(path, config)
		bc.Put("Name", "Eslam")
		bc.Put("Age", "22")
		bc.Put("Uni", "MU")
		bc.Sync()
		bc.Merge()
		want := "MU"
		got, _ := bc.Get("Uni")
		bc.Close()
		if got != want {
			t.Errorf("expected %v but got %v", want, got)
		}
	})
}

func TestFold(t *testing.T) {
	path := filepath.Join("testing", "testFold")
	bc, _ := Open(path, ConfigOptions{WritingPermession, false})
	for i := 0; i < 10; i++ {
		bc.Put(fmt.Sprint(i+1), fmt.Sprint(i+1))
	}
	foldFunc := func(s1, s2 string, a any) any {
		acc, _ := a.(int)
		k, _ := strconv.Atoi(s1)
		v, _ := strconv.Atoi(s2)
		return acc + k + v
	}
	want := 110
	got := bc.Fold(foldFunc, 0)
	bc.Close()
	if got != want {
		t.Errorf("got:%d, want:%d", got, want)
	}
}

func TestMerge(t *testing.T) {
	t.Run("Merge 200000 values", func(t *testing.T) {
		path := filepath.Join("testing", "testMerge", "largeFile")
		config := ConfigOptions{WritingPermession, false}
		bc, _ := Open(path, config)
		for i := 0; i < 200000; i++ {
			bc.Put("Id"+strconv.Itoa(i), "20202020")
		}
		want := 200000
		got := len(bc.keydir)
		bc.Close()
		if got != want {
			t.Errorf("expected %v but got %v", want, got)
		}
	})
	t.Run("Merge with no Writing permession", func(t *testing.T) {
		path := filepath.Join("testing", "testMerge", "noPerm")
		bc, _ := Open(path)
		err := bc.Merge()
		want := fmt.Sprintf("Writing permession denied in directory %s", bc.directory)
		got := err.Error()
		if got != want {
			t.Errorf("expected %v but got %v", want, got)
		}
	})
}

func TestSync(t *testing.T) {
	t.Run("Merge with Writing permession", func(t *testing.T) {
		path := filepath.Join("testing", "testSync")
		bc, _ := Open(path, ConfigOptions{WritingPermession, false})
		bc.Close()
		bc, _ = Open(path)
		err := bc.Sync()
		want := fmt.Sprintf("Writing permession denied in directory %s", path)
		got := err.Error()
		if got != want {
			t.Errorf("expected %v but got %v", want, got)
		}
	})
}
