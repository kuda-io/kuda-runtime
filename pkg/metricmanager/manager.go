package metricmanager

import (
	"context"
)

type MetricManager struct{}

func New() *MetricManager {
	return &MetricManager{}
}

/*
	todo
	Collect and report distribution of data between nodes and usage between applications
*/
func (m *MetricManager) Run(ctx context.Context) {

}
