package alluxio

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	alluxioOpt "github.com/Alluxio/alluxio-go/option"
	alluxio "github.com/alluxio/alluxio-go"
	"k8s.io/klog/v2"

	"github.com/kuda-io/kuda-runtime/pkg/api/v1alpha1"
	"github.com/kuda-io/kuda-runtime/pkg/downloadmanager/downloader"
	kudaApi "github.com/kuda-io/kuda/pkg/api/data/v1alpha1"
)

const (
	Name = "alluxio"
)

type Alluxio struct {
	downloadRootDir string
}

func New(downloadRootDir string) downloader.Interface {
	return &Alluxio{
		downloadRootDir: downloadRootDir,
	}
}

/*
	remotePath: download data from this place
    downloadPath: download data to this place
	localPath:  app use data in this place, sort link from downloadPath
*/
func (a *Alluxio) Download(ctx context.Context, subTask *v1alpha1.SubTask) error {
	// handle non-standard path
	remotePath := filepath.Clean(subTask.RemotePath)
	downloadDir := fmt.Sprintf("%s/%s/%s/%s", a.downloadRootDir, subTask.Namespace, subTask.Name, subTask.Version)

	dataSource, ok := subTask.DataSource.(*kudaApi.AlluxioDataSource)
	if !ok {
		return fmt.Errorf("invalid alluxio dataSource %+v", subTask.DataSource)
	}

	client := a.newClient(dataSource)
	if err := a.copyToLocal(client, remotePath, downloadDir); err != nil {
		if err := os.RemoveAll(downloadDir); err != nil {
			klog.Errorf("Failed to delete downloadPath %s", downloadDir)
		}
		return err
	}

	klog.Infof("Succeed to download %s:%s to %s", Name, remotePath, downloadDir)
	return nil
}

func (a *Alluxio) newClient(dataSouce *kudaApi.AlluxioDataSource) *alluxio.Client {
	return alluxio.NewClient(dataSouce.Host, dataSouce.Port, time.Duration(dataSouce.Timeout)*time.Second)
}

func (a *Alluxio) copyToLocal(fs *alluxio.Client, remotePath, downloadDir string) error {
	fileInfo, err := fs.GetStatus(remotePath, &alluxioOpt.GetStatus{})
	if err != nil {
		return err
	}

	if !fileInfo.Folder {
		return a.copyFileToLocal(fs, remotePath, fileInfo.Name, downloadDir)
	}

	path := fmt.Sprintf("%s/%s", downloadDir, fileInfo.Name)
	if err := os.MkdirAll(path, os.FileMode(fileInfo.Mode)); err != nil {
		klog.Errorf("Failed to mkdir %s, err: %s", path, err)
		return err
	}

	subFileInfoList, err := fs.ListStatus(remotePath, &alluxioOpt.ListStatus{})
	if err != nil {
		klog.Errorf("Failed to list dir %s, err: %s", remotePath, err)
		return err
	}

	for _, subFileInfo := range subFileInfoList {
		if err := a.copyToLocal(fs, subFileInfo.Path, path); err != nil {
			return err
		}
	}
	return nil
}

func (a *Alluxio) copyFileToLocal(fs *alluxio.Client, remotePath, remoteFileName, downloadDir string) error {
	dstFilePath := fmt.Sprintf("%s/%s", downloadDir, remoteFileName)
	if err := os.MkdirAll(downloadDir, 0666); err != nil {
		klog.Errorf("Failed to mkdir downloadPath %s, err: %s", downloadDir, err)
		return err
	}

	dstFile, err := os.Create(dstFilePath)
	if err != nil {
		klog.Errorf("Failed to mkdir file %s, err: %s", dstFilePath, err)
		return err
	}
	defer dstFile.Close()

	id, err := fs.OpenFile(remotePath, &alluxioOpt.OpenFile{})
	if err != nil {
		klog.Errorf("Failed to open alluxio remotePath %s, err: %s", remotePath, err)
		return err
	}
	defer fs.Close(id)
	srcFile, err := fs.Read(id)
	if err != nil {
		klog.Errorf("Failed to read alluxio remotePath %s, err: %s", remotePath, err)
		return err
	}
	defer srcFile.Close()

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		klog.Errorf("Failed to copy from %s to %s, err: %s", remotePath, dstFilePath, err)
		return err
	}

	return nil
}
