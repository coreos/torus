package metadata

import (
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/pborman/uuid"
)

func MakeOrGetUUID(datadir string) (string, error) {
	if datadir == "" {
		return uuid.NewUUID().String(), nil
	}

	filename := filepath.Join(datadir, "metadata", "uuid")
	if _, err := os.Stat(filename); err == os.ErrNotExist {
		id := uuid.NewUUID()
		fnew, err := os.Create(filename)
		if err != nil {
			return "", err
		}
		defer fnew.Close()
		_, err = fnew.WriteString(id.String())
		return id.String(), err
	}
	bytes, err := ioutil.ReadFile(filename)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}
