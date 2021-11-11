package downloader

import (
	"context"

	"github.com/kuda-io/kuda-runtime/pkg/api/v1alpha1"
)

type PluginFactory = func(downloadRootDir string) Interface

type Interface interface {
	Download(ctx context.Context, subTask *v1alpha1.SubTask) error
}
