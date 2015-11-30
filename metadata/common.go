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
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		id := uuid.NewUUID()
		fnew, ferr := os.Create(filename)
		if ferr != nil {
			return "", ferr
		}
		defer fnew.Close()
		_, werr := fnew.WriteString(id.String())
		return id.String(), werr
	}
	bytes, err := ioutil.ReadFile(filename)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}
