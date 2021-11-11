package downloader

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"k8s.io/klog/v2"

	"github.com/kuda-io/kuda-runtime/pkg/api/v1alpha1"
	"github.com/kuda-io/kuda-runtime/pkg/utils"
	kudaApi "github.com/kuda-io/kuda/pkg/api/data/v1alpha1"
	kuda "github.com/kuda-io/kuda/pkg/generated/clientset/versioned/typed/data/v1alpha1"
)

type Downloader struct {
	dataNs          string
	dataName        string
	podName         string
	downloadRootDir string
	localRootDir    string
	containerID     string
	taskCh          chan *v1alpha1.Task
	noticeCh        chan *v1alpha1.Task
	registry        Registry
	compClient      *kuda.DataV1alpha1Client
}

func New(dataNs, dataName, podName, downloadRootDir, localRootDir string, taskCh, noticeCh chan *v1alpha1.Task, registry Registry) (*Downloader, error) {
	containerID := os.Getenv(kudaApi.KudaRuntimeEnvMainContainerName)
	if podName == "" {
		return nil, errors.New("podName can not empty")
	}
	if containerID == "" {
		return nil, errors.New("containerID can not empty")
	}

	compClient, err := utils.GetCompClient()
	if err != nil {
		return nil, err
	}

	return &Downloader{
		dataNs:          dataNs,
		dataName:        dataName,
		taskCh:          taskCh,
		noticeCh:        noticeCh,
		registry:        registry,
		downloadRootDir: downloadRootDir,
		localRootDir:    localRootDir,
		podName:         podName,
		containerID:     containerID,
		compClient:      compClient,
	}, nil
}

func (d *Downloader) Run(ctx context.Context) {
	go func() {
		for {
			select {
			case task, ok := <-d.taskCh:
				if ok {
					if err := d.execTask(ctx, task); err != nil {
						klog.Errorf("Failed to exec task, err: %s", err)
						os.Exit(1)
					}
				}
			}
		}
	}()
}

func (d *Downloader) execTask(ctx context.Context, task *v1alpha1.Task) error {
	if err := d.execHook(task.Lifecycle.PreDownload); err != nil {
		return err
	}

	for _, subTask := range task.Items {
		if err := d.execHook(subTask.Lifecycle.PreDownload); err != nil {
			return err
		}

		if err := d.execSubTask(ctx, &subTask); err != nil {
			return err
		}

		if err := d.execHook(subTask.Lifecycle.PostDownload); err != nil {
			return err
		}
	}

	if err := d.execHook(task.Lifecycle.PostDownload); err != nil {
		return err
	}

	if err := d.cleanUnusedSubTask(ctx, task.Items); err != nil {
		return err
	}

	d.noticeCh <- task

	return nil
}

func (d *Downloader) execSubTask(ctx context.Context, subTask *v1alpha1.SubTask) error {
	pluginFactory, err := d.registry.Get(subTask.DataSourceType)
	if err != nil {
		return err
	}

	plugin := pluginFactory(d.downloadRootDir)

	if err := d.updateSubTaskStatus(ctx, subTask, kudaApi.DataDownloading, ""); err != nil {
		return err
	}

	if err := plugin.Download(ctx, subTask); err != nil {
		if err := d.updateSubTaskStatus(ctx, subTask, kudaApi.DataFailed, err.Error()); err != nil {
			return err
		}
		return err
	}

	if err := d.createSoftLink(subTask); err != nil {
		return err
	}

	if err := d.updateSubTaskStatus(ctx, subTask, kudaApi.DataSuccess, ""); err != nil {
		return err
	}

	return nil
}

func (d *Downloader) createSoftLink(subTask *v1alpha1.SubTask) error {
	srcPath := fmt.Sprintf("%s/%s/%s/%s/%s",
		d.downloadRootDir,
		subTask.Namespace,
		subTask.Name,
		subTask.Version,
		filepath.Clean(filepath.Base(subTask.RemotePath)),
	)
	localPath := filepath.Clean(fmt.Sprintf("%s%s", d.localRootDir, subTask.LocalPath))
	if err := utils.CreateSoftLink(srcPath, localPath); err != nil {
		return err
	}
	klog.Infof("Succeed to create softLink from %s to %s", srcPath, localPath)
	return nil
}
