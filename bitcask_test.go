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
		path := "testOpen"
		createTestingDir(path)
		path = filepath.Join(path, "NonEsisting")
		bc, err := Open(path, ConfigOptions{accessPermission: WritingPermession, syncOption: false})
		want := fmt.Sprintf("New Directory was created in the path %s", path)
		got := err.Error()
		bc.Close()
		os.Remove(path)
		if got != want {
			t.Errorf("expected %v but got %v", want, got)
		}
		cleanTestingDir("testOpen")
	})

	t.Run("Non existing directory with no writing permession", func(t *testing.T) {
		path := "testOpen"
		createTestingDir(path)
		path = filepath.Join(path, "NonEsisting")
		_, err := Open(path)
		want := fmt.Sprintf("No such a file or directory %s\nCan't create directory in the path%s", path, path)
		got := err.Error()
		if got != want {
			t.Errorf("expected %v but got %v", want, got)
		}
		cleanTestingDir(path)
	})

	t.Run("read from locked dir", func(t *testing.T) {
		path := "testOpen"
		createTestingDir(path)
		bc, _ := Open(path, ConfigOptions{WritingPermession, false})
		_, err := Open(path)
		want := fmt.Sprintf("The directory %s is type locked you can't read or write from it", path)
		got := err.Error()
		bc.Close()
		if got != want {
			t.Errorf("expected %v but got %v", want, got)
		}
		cleanTestingDir(path)
	})
}

func TestPut(t *testing.T) {
	t.Run("Put with permession", func(t *testing.T) {
		path := "testPut"
		createTestingDir(path)
		bc, _ := Open(path, ConfigOptions{WritingPermession, false})
		bc.Put("Name", "Eslam")
		got := len(bc.keydir)
		want := 1
		bc.Close()
		if got != want {
			t.Errorf("expected %v but got %v", want, got)
		}
		cleanTestingDir(path)
	})

	t.Run("Put with no permession", func(t *testing.T) {
		path := "testPut"
		createTestingDir(path)
		bc, _ := Open(path)
		got := bc.Put("Name", "Eslam").Error()
		want := fmt.Sprintf("Writing permession denied in directory %s", path)
		bc.Close()
		if got != want {
			t.Errorf("expected %v but got %v", want, got)
		}
		cleanTestingDir(path)
	})
}

func TestDelete(t *testing.T) {
	t.Run("Delate with permession", func(t *testing.T) {
		path := "testPut"
		createTestingDir(path)
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
		cleanTestingDir(path)
	})

	t.Run("key not found", func(t *testing.T) {
		path := "testPut"
		createTestingDir(path)
		bc, _ := Open(path)
		got := bc.Delate("Age").Error()
		want := fmt.Sprintf("Writing permession denied in directory %s", path)
		bc.Close()
		if got != want {
			t.Errorf("expected %v but got %v", want, got)
		}
		cleanTestingDir(path)
	})
}

func TestListKeys(t *testing.T) {
	path := "testPut"
	createTestingDir(path)
	bc, _ := Open(path, ConfigOptions{WritingPermession, false})
	bc.Put("Name", "Eslam")
	bc.Put("Age", "22")
	want := []string{"Age", "Name"}
	bc.Close()
	got := bc.ListKeys()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("expected %v but got %v", want, got)
	}
	cleanTestingDir(path)
}

func TestGet(t *testing.T) {
	t.Run("Get pending value", func(t *testing.T) {
		path := "testGet"
		createTestingDir(path)
		config := ConfigOptions{WritingPermession, false}
		bc, _ := Open(path, config)
		bc.Put("Name", "Eslam")
		got, _ := bc.Get("Name")
		want := "Eslam"
		bc.Close()
		if got != want {
			t.Errorf("expected %v but got %v", want, got)
		}
		cleanTestingDir(path)
	})

	t.Run("Get not existing value", func(t *testing.T) {
		path := "testGet"
		createTestingDir(path)
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
		cleanTestingDir(path)
	})

	t.Run("Get synced value", func(t *testing.T) {
		path := "testGet"
		createTestingDir(path)
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
		cleanTestingDir(path)
	})

	t.Run("Get synced invalid value", func(t *testing.T) {
		path := "testGet"
		createTestingDir(path)
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
		cleanTestingDir(path)
	})

	t.Run("Get Merged value", func(t *testing.T) {
		path := "testGet"
		createTestingDir(path)
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
		cleanTestingDir(path)
	})

	t.Run("ask for write while other process is reading", func(t *testing.T) {
		path := "testGet"
		createTestingDir(path)
		bc, _ := Open(path)
		_, err := Open(path, ConfigOptions{WritingPermession, false})
		got := err.Error()
		want := fmt.Sprintf("The directory %s cant open a bitcask for writing", path)
		bc.Close()
		if got != want {
			t.Errorf("expected %v but got %v", want, got)
		}
		cleanTestingDir(path)
	})

	t.Run("Open multiple readers", func(t *testing.T) {
		path := "testGet"
		createTestingDir(path)
		bc, _ := Open(path)
		_, err := Open(path)
		got := err
		bc.Close()
		bc.Close()
		if got != nil {
			t.Errorf("expected %v but got %v", nil, got)
		}
		cleanTestingDir(path)
	})

	t.Run("test parsing hint file", func(t *testing.T) {
		path := "testGet"
		createTestingDir(path)
		bc, _ := Open(path, ConfigOptions{WritingPermession, false})
		bc.Put("Name", "Eslam")
		bc.Put("uni", "MU")
		bc.Close()
		bc2, _ := Open(path)
		got, _ := bc2.Get("Name")
		want := "Eslam"
		bc2.Close()
		if got != want {
			t.Errorf("expected %v but got %v", want, got)
		}
		cleanTestingDir(path)
	})
}

func TestFold(t *testing.T) {
	path := "testGet"
	createTestingDir(path)
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
	cleanTestingDir(path)
}

func TestMerge(t *testing.T) {
	t.Run("Merge 200000 values", func(t *testing.T) {
		path := "testGet"
		createTestingDir(path)
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
		cleanTestingDir(path)
	})
	t.Run("Merge with no Writing permession", func(t *testing.T) {
		path := "testGet"
		createTestingDir(path)
		bc, _ := Open(path)
		err := bc.Merge()
		want := fmt.Sprintf("Writing permession denied in directory %s", bc.directory)
		got := err.Error()
		if got != want {
			t.Errorf("expected %v but got %v", want, got)
		}
		cleanTestingDir(path)
	})
}

func TestSync(t *testing.T) {
	t.Run("Merge with Writing permession", func(t *testing.T) {
		path := "testGet"
		createTestingDir(path)
		bc, _ := Open(path, ConfigOptions{WritingPermession, false})
		bc.Close()
		bc, _ = Open(path)
		err := bc.Sync()
		want := fmt.Sprintf("Writing permession denied in directory %s", path)
		got := err.Error()
		if got != want {
			t.Errorf("expected %v but got %v", want, got)
		}
		cleanTestingDir(path)
	})

}

func createTestingDir(dir string) {
	os.Mkdir(dir, os.ModePerm)
}

func cleanTestingDir(dir string) {
	os.RemoveAll(dir)
}
