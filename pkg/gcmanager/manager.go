package gcmanager

import (
	"context"
)

type GcManager struct{}

func New() *GcManager {
	return &GcManager{}
}

/*
	todo
	Clean unused data
*/
func (m *GcManager) Run(ctx context.Context) error {
	return nil
}
