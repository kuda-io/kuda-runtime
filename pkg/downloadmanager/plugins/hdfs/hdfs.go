package hdfs

import (
	"context"
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/colinmarc/hdfs"
	"k8s.io/klog/v2"

	"github.com/kuda-io/kuda-runtime/pkg/api/v1alpha1"
	"github.com/kuda-io/kuda-runtime/pkg/downloadmanager/downloader"
	kudaApi "github.com/kuda-io/kuda/pkg/api/data/v1alpha1"
)

const (
	Name = "hdfs"
)

type Hdfs struct {
	downloadRootDir string
}

func New(downloadRootDir string) downloader.Interface {
	return &Hdfs{
		downloadRootDir: downloadRootDir,
	}
}

func (h *Hdfs) Download(ctx context.Context, subTask *v1alpha1.SubTask) error {
	remotePath := filepath.Clean(subTask.RemotePath)
	downloadDir := fmt.Sprintf("%s/%s/%s/%s", h.downloadRootDir, subTask.Namespace, subTask.Name, subTask.Version)

	if err := os.MkdirAll(downloadDir, 0666); err != nil {
		klog.Errorf("Failed to stat downloadDir %s, err: %s", downloadDir, err)
		return err
	}

	dataSource, ok := subTask.DataSource.(*kudaApi.HdfsDataSource)
	if !ok {
		return fmt.Errorf("invalid hdfs dataSource %+v", subTask.DataSource)
	}

	client, err := h.newClient(dataSource)
	if err != nil {
		return err
	}
	defer client.Close()

	if err := h.copyToLocal(client, remotePath, downloadDir); err != nil {
		if err := os.RemoveAll(downloadDir); err != nil {
			klog.Errorf("Failed to delete downloadPath %s, err: %s", downloadDir, err)
		}
		return err
	}
	klog.Infof("Succeed to download %s:%s to %s", Name, remotePath, downloadDir)
	return nil
}

func (h *Hdfs) newClient(dataSouce *kudaApi.HdfsDataSource) (*hdfs.Client, error) {
	client, err := hdfs.NewClient(hdfs.ClientOptions{
		Addresses: dataSouce.Addresses,
		User:      dataSouce.UserName,
	})
	if err != nil {
		return nil, err
	}

	return client, nil
}

func (h *Hdfs) copyToLocal(client *hdfs.Client, remotePath, downloadDir string) error {
	fileInfo, err := client.Stat(remotePath)
	if err != nil {
		klog.Errorf("Failed to stat remotePath %s, err: %s", remotePath, err)
		return err
	}

	downloadPath := fmt.Sprintf("%s/%s", downloadDir, fileInfo.Name())
	if !fileInfo.IsDir() {
		return h.copyFileToLocal(client, remotePath, downloadPath, fileInfo.Mode())
	}

	if err := os.MkdirAll(downloadPath, fileInfo.Mode()); err != nil {
		return err
	}

	fileInfoList, err := client.ReadDir(remotePath)
	if err != nil {
		klog.Errorf("Failed to readDir remotePath %s, err: %s", remotePath, err)
		return err
	}

	for _, fileInfo := range fileInfoList {
		src := fmt.Sprintf("%s/%s", remotePath, fileInfo.Name())
		if err := h.copyToLocal(client, src, downloadPath); err != nil {
			return err
		}
	}
	return nil
}

func (h *Hdfs) copyFileToLocal(client *hdfs.Client, remotePath, downloadPath string, perm fs.FileMode) error {
	data, err := client.ReadFile(remotePath)
	if err != nil {
		klog.Errorf("Failed to readFile remotePath %s, err: %s", remotePath, err)
		return err
	}

	if err := ioutil.WriteFile(downloadPath, data, perm); err != nil {
		klog.Errorf("Failed to writeFile downloadPath %s, err: %s", downloadPath, err)
		return err
	}

	return nil
}
