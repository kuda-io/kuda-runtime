package downloader

import (
	"context"
	"encoding/json"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/kuda-io/kuda-runtime/pkg/api/v1alpha1"
	kudaApi "github.com/kuda-io/kuda/pkg/api/data/v1alpha1"
)

const (
	subResourceStatus = "status"
)

func (d *Downloader) updateSubTaskStatus(ctx context.Context, subTask *v1alpha1.SubTask, phase kudaApi.DataPhase, message string) error {
	data, err := d.compClient.Datas(d.dataNs).Get(ctx, d.dataName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	var isExistFlag bool
	for i, s := range data.Status.DataItemsStatus {
		if s.Namespace == subTask.Namespace && s.Name == subTask.Name {
			isExistFlag = true
			data.Status.DataItemsStatus[i] = *subTask.GetStatus(phase, message)
			break
		}
	}
	if !isExistFlag {
		data.Status.DataItemsStatus = append(data.Status.DataItemsStatus, *subTask.GetStatus(phase, message))
	}
	patchData, _ := json.Marshal(map[string]interface{}{
		"status": map[string]interface{}{
			"dataItemsStatus": data.Status.DataItemsStatus,
		},
	})
	if _, err := d.compClient.Datas(d.dataNs).Patch(ctx, d.dataName, types.MergePatchType, patchData, metav1.PatchOptions{}, subResourceStatus); err != nil {
		return err
	}
	return nil
}

func (d *Downloader) cleanUnusedSubTask(ctx context.Context, subTaskList []v1alpha1.SubTask) error {
	dataInUse := make(map[string]bool)
	for _, subTask := range subTaskList {
		key := fmt.Sprintf("%s.%s.%s", subTask.Namespace, subTask.Name, subTask.Version)
		dataInUse[key] = true
	}

	data, err := d.compClient.Datas(d.dataNs).Get(ctx, d.dataName, metav1.GetOptions{})
	if err != nil {
		return err
	}

	newDataItemsStatus := make(kudaApi.DataItemsStatus, 0)
	for _, s := range data.Status.DataItemsStatus {
		key := fmt.Sprintf("%s.%s.%s", s.Namespace, s.Name, s.Version)
		if dataInUse[key] {
			newDataItemsStatus = append(newDataItemsStatus, s)
		}
	}

	patchData, _ := json.Marshal(map[string]interface{}{
		"status": map[string]interface{}{
			"dataItemsStatus": newDataItemsStatus,
		},
	})
	if _, err := d.compClient.Datas(d.dataNs).Patch(ctx, d.dataName, types.MergePatchType, patchData, metav1.PatchOptions{}, subResourceStatus); err != nil {
		return err
	}
	return nil
}
