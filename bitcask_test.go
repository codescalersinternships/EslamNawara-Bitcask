package bitcask

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestOpen(t *testing.T) {
	t.Run("Non existing directory with writing permession", func(t *testing.T) {
		path := filepath.Join(filepath.Join("testing", "testOpen"), "NonEsisting")
		bc, err := Open(path, ConfigOptions{accessPermission: WritingPermession, syncOption: false})
		want := fmt.Sprintf("New Directory was created in the path %s", path)
		got := err.Error()
		bc.unlockDir() //change to close after finishing
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
		os.Remove(path)
		if got != want {
			t.Errorf("expected %v but got %v", want, got)
		}
	})
	t.Run("Existing directory", func(t *testing.T) {
		path := filepath.Join(filepath.Join("testing", "testOpen"), "openForReadingDir")
		bc, _ := Open(path)
		//the hint file had one line so the keydir contains one record after parsing the hint file
		want := 1
		got := len(bc.keydir)
		if got != want {
			t.Errorf("expected %v but got %v", want, got)
		}
	})

	t.Run("Open for write", func(t *testing.T) {
		path := filepath.Join(filepath.Join("testing", "testOpen"), "openForWritingDir")
		config := ConfigOptions{WritingPermession, true}
		bc, _ := Open(path, config)
		//the hint file had one line so the keydir contains one record after parsing the hint file
		want := 1
		got := len(bc.keydir)
		bc.unlockDir() //change to close after finishing
		if got != want {
			t.Errorf("expected %v but got %v", want, got)
		}
	})

	t.Run("read from locked dir", func(t *testing.T) {
		path := filepath.Join(filepath.Join("testing", "testOpen"), "lockedDir")
		Open(path, ConfigOptions{WritingPermession, true})
		bc, err := Open(path, ConfigOptions{ReadOnly, false})
		want := fmt.Sprintf("The directory %s is type locked you can't read or write from it", path)
		got := err.Error()
		bc.unlockDir()
		if got != want {
			t.Errorf("expected %v but got %v", want, got)
		}
	})
}

func TestPut(t *testing.T) {
	t.Run("Put with permession", func(t *testing.T) {
		path := filepath.Join("testing", "testPut")
		bc, _ := Open(path, ConfigOptions{WritingPermession, true})
		bc.Put("Name", "Eslam")
		got := len(bc.keydir)
		want := 1
		bc.unlockDir() //change to close after finishing
		if got != want {
			t.Errorf("expected %v but got %v", want, got)
		}
	})

	t.Run("Put with no permession", func(t *testing.T) {
		path := filepath.Join("testing", "testPut")
		bc, _ := Open(path)
		got := bc.Put("Name", "Eslam").Error()
		want := fmt.Sprintf("Writing permession denied in directory %s", path)
		bc.unlockDir() //change to close after finishing
		if got != want {
			t.Errorf("expected %v but got %v", want, got)

		}
	})
}

func TestGet(t *testing.T) {
	t.Run("Get pending value", func(t *testing.T) {
		path := filepath.Join("testing", "testGet", "validValue")
		config := ConfigOptions{WritingPermession, true}
		bc, _ := Open(path, config)
		bc.Put("Name", "Eslam")
		got, _ := bc.Get("Name")
		want := "Eslam"
		bc.unlockDir() //change to close after finishing
		if got != want {
			t.Errorf("expected %v but got %v", want, got)
		}
	})
	t.Run("Get not existing value", func(t *testing.T) {
		path := filepath.Join("testing", "testGet", "invalidValue")
		config := ConfigOptions{WritingPermession, true}
		bc, _ := Open(path, config)
		key := "Age"
		want := fmt.Sprintf("Key %s not found in the directory", key)
		_, err := bc.Get(key)
		got := err.Error()
		bc.unlockDir() //change to close after finishing
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
		bc.unlockDir() //change to close after finishing
		if got != want {
			t.Errorf("expected %v but got %v", want, got)
		}
	})
}

