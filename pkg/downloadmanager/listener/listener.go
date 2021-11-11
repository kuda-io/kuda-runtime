package listener

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kuda-io/kuda-runtime/pkg/api/v1alpha1"
	"github.com/kuda-io/kuda-runtime/pkg/utils"
	kudaApi "github.com/kuda-io/kuda/pkg/api/data/v1alpha1"
	kuda "github.com/kuda-io/kuda/pkg/generated/clientset/versioned/typed/data/v1alpha1"
)

const (
	dataDigesFile = "/etc/podinfo/annotations"
	listInterval  = 15 * 60
)

type Listener struct {
	dataNs     string
	dataName   string
	taskCh     chan *v1alpha1.Task
	digested   string
	fsWatcher  *fsnotify.Watcher
	compClient *kuda.DataV1alpha1Client
}

func New(dataNs, dataName string, taskCh chan *v1alpha1.Task) (*Listener, error) {
	fsWatcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("failed to new fsnotify watcher, err: %s", err)
	}

	compClient, err := utils.GetCompClient()
	if err != nil {
		return nil, err
	}

	return &Listener{
		taskCh:     taskCh,
		dataNs:     dataNs,
		dataName:   dataName,
		fsWatcher:  fsWatcher,
		compClient: compClient,
	}, nil
}

func (l *Listener) Run(ctx context.Context) {
	go func() {
		l.newTask(ctx)

		ticker := time.NewTicker(listInterval * time.Second)
		defer ticker.Stop()

		for {
			if err := l.fsWatcher.Add(dataDigesFile); err != nil {
				l.fsWatcher.Close()
				time.Sleep(10 * time.Second)
				continue
			}

			select {
			case <-ticker.C:
				l.newTask(ctx)
			case event := <-l.fsWatcher.Events:
				if event.Op == fsnotify.Remove {
					l.newTask(ctx)
				}
			case <-l.fsWatcher.Errors:
				break
			}
		}
	}()
}

func (l *Listener) newTask(ctx context.Context) {
	signature, err := utils.GetValueFromAnnotation(dataDigesFile, kudaApi.KudaKeyDigest)
	if err != nil {
		return
	}

	if l.digested == signature {
		return
	}

	data, err := l.compClient.Datas(l.dataNs).Get(ctx, l.dataName, metav1.GetOptions{})
	if err != nil {
		return
	}

	task := &v1alpha1.Task{
		Items:     make([]v1alpha1.SubTask, 0),
		Lifecycle: getLifecycle(data.Spec.Lifecycle),
	}

	dataSourceMap := getDataSourceMap(*data.Spec.DataSources)
	for _, dataItem := range data.Spec.DataItems {
		dataSource, ok := dataSourceMap[dataItem.DataSourceType]
		if !ok {
			continue
		}
		subTask := v1alpha1.SubTask{
			Namespace:      dataItem.Namespace,
			Name:           dataItem.Name,
			Version:        dataItem.Version,
			RemotePath:     dataItem.RemotePath,
			LocalPath:      dataItem.LocalPath,
			DataSource:     dataSource,
			DataSourceType: dataItem.DataSourceType,
			Lifecycle:      getLifecycle(dataItem.Lifecycle),
		}
		task.Items = append(task.Items, subTask)
	}

	l.taskCh <- task
	l.digested = signature
}

func getDataSourceMap(sources kudaApi.DataSources) map[string]interface{} {
	dataSourceMap := make(map[string]interface{})
	t := reflect.TypeOf(sources)
	v := reflect.ValueOf(sources)
	for i := 0; i < t.NumField(); i++ {
		dataSourceMap[strings.ToLower(t.Field(i).Name)] = v.Field(i).Interface()
	}
	return dataSourceMap
}

func getLifecycle(lifecycle *kudaApi.Lifecycle) *kudaApi.Lifecycle {
	if lifecycle != nil {
		return lifecycle
	}
	return &kudaApi.Lifecycle{}
}
