package utils

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

func IsLinkExist(path string) (bool, error) {
	_, err := os.Lstat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func CreateSoftLink(srcPath, dstPath string) error {
	dstDir := filepath.Dir(dstPath)
	if err := os.MkdirAll(dstDir, 0666); err != nil {
		return err
	}

	isExist, err := IsLinkExist(dstPath)
	if err != nil {
		return err
	}

	if isExist {
		if err := os.Remove(dstPath); err != nil {
			return err
		}
	}
	if err := os.Symlink(srcPath, dstPath); err != nil {
		return err
	}
	return nil
}

func GetValueFromAnnotation(path, key string) (string, error) {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return "", err
	}

	lines := strings.Split(string(b), "\n")
	for _, line := range lines {
		kv := strings.Split(line, "=")
		if len(kv) != 2 {
			continue
		}

		if kv[0] == key {
			return kv[1], nil
		}
	}

	return "", nil
}
