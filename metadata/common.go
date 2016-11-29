// metadata is the metapackage for the implementations of the metadata
// interface, for each potential backend.
package metadata

import (
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/pborman/uuid"
)

func MakeUUID() string {
	return uuid.NewUUID().String()
}

// TODO(barakmich): Make into a JSON file?
// This all should be moved to storage/ because that's where it's really owned.
func GetUUID(datadir string) (string, error) {
	if datadir == "" {
		return "", errors.New("given a empty datadir and asked to get it's UUID")
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
