package downloadmanager

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/kuda-io/kuda-runtime/pkg/api/v1alpha1"
	"github.com/kuda-io/kuda-runtime/pkg/downloadmanager/downloader"
	"github.com/kuda-io/kuda-runtime/pkg/downloadmanager/listener"
	"github.com/kuda-io/kuda-runtime/pkg/downloadmanager/notifier"
	kudaApi "github.com/kuda-io/kuda/pkg/api/data/v1alpha1"
)

type DownloadManager struct {
	listener   *listener.Listener
	downloader *downloader.Downloader
	notifier   *notifier.Notifier
}

func New(opts *Options, registry downloader.Registry) (*DownloadManager, error) {
	taskCh := make(chan *v1alpha1.Task, v1alpha1.QueueSize)
	noticeCh := make(chan *v1alpha1.Task, v1alpha1.QueueSize)
	dataNs := os.Getenv(kudaApi.KudaRuntimeEnvDataSetNamespace)
	datasetName := os.Getenv(kudaApi.KudaRuntimeEnvDataSetName)
	podName := os.Getenv(kudaApi.KudaRuntimeEnvPodName)
	dataName := getDataName(datasetName, podName)
	if dataNs == "" || datasetName == "" || podName == "" {
		return nil, errors.New("dataNs | datasetName | podName can not empty")
	}

	listener, err := listener.New(dataNs, dataName, taskCh)
	if err != nil {
		return nil, err
	}

	downloader, err := downloader.New(
		dataNs,
		dataName,
		podName,
		opts.DownloadRootDir,
		opts.LocalRootDir,
		taskCh,
		noticeCh,
		registry)
	if err != nil {
		return nil, err
	}
	return &DownloadManager{
		listener:   listener,
		downloader: downloader,
		notifier:   notifier.New(opts.NoticeServerPort, noticeCh),
	}, nil
}

func (d *DownloadManager) Run(ctx context.Context) {
	d.listener.Run(ctx)
	d.downloader.Run(ctx)
	d.notifier.Run()
}

func getDataName(datasetName, podName string) string {
	res := strings.Split(podName, "-")
	if len(res) < 3 {
		return ""
	}
	return fmt.Sprintf("%s-%s", datasetName, strings.Join(res[len(res)-2:], "-"))
}
