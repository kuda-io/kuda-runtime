package downloader

import (
	"fmt"
	"net/http"
	"strings"

	"k8s.io/klog/v2"

	"github.com/kuda-io/kuda-runtime/pkg/utils"
	kudaApi "github.com/kuda-io/kuda/pkg/api/data/v1alpha1"
)

func (d *Downloader) execHook(handler *kudaApi.LifecycleHandler) error {
	if handler != nil {
		return execHook(d.dataNs, d.podName, d.containerID, handler)
	}
	return nil
}

func execHook(podNs, podName, containerID string, handler *kudaApi.LifecycleHandler) error {
	switch {
	case handler.Exec != nil && len(handler.Exec.Command) > 0:
		_, err := utils.ExecInPod(podNs, podName, containerID, handler.Exec.Command)
		if err != nil {
			return err
		}
		klog.Infof("Succeed to exec command %+v", handler.Exec.Command)
	case handler.HTTPGet != nil:
		url := fmt.Sprintf("%s://%s:%d%s",
			strings.ToLower(string(handler.HTTPGet.Scheme)),
			handler.HTTPGet.Host, handler.HTTPGet.Port.IntVal,
			handler.HTTPGet.Path)
		resp, err := http.Get(url)
		if err != nil {
			return err
		}
		if resp.StatusCode != 200 {
			return fmt.Errorf("resp statusCode not 200, statusCode: %d", resp.StatusCode)
		}
		klog.Info("Succeed to exec http.Get %s", url)
	}
	return nil
}
