package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kudaApi "github.com/kuda-io/kuda/pkg/api/data/v1alpha1"
)

const (
	QueueSize = 512
)

type DataPhase string

type Task struct {
	Items     []SubTask
	Lifecycle *kudaApi.Lifecycle `json:"lifecycle,omitempty"`
}

type SubTask struct {
	Name           string            `json:"name"`
	Namespace      string            `json:"namespace"`
	RemotePath     string            `json:"remotePath"`
	LocalPath      string            `json:"localPath"`
	Version        string            `json:"version,omitempty"`
	DataSource     interface{}       `json:"dataSource"`
	DataSourceType string            `json:"dataSourceType"`
	Lifecycle      *kudaApi.Lifecycle `json:"lifecycle,omitempty"`
}

type NoticeResp struct {
	Data []NoticeDataItem `json:"data"`
}

type NoticeDataItem struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	Version   string `json:"version"`
	LocalPath string `json:"localPath"`
}

func (t *SubTask) GetStatus(phase kudaApi.DataPhase, message string) *kudaApi.DataItemStatus {
	return &kudaApi.DataItemStatus{
		Name:      t.Name,
		Namespace: t.Namespace,
		Version:   t.Version,
		Phase:     phase,
		Message:   message,
		StartTime: metav1.Now(),
	}
}
