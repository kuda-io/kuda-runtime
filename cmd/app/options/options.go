package options

import (
	flag "github.com/spf13/pflag"
	"k8s.io/klog/v2"

	"github.com/kuda-io/kuda-runtime/pkg/downloadmanager"
)

type AgentOptions struct {
	DownloadManagerOpt *downloadmanager.Options
}

func NewAgentOptions() *AgentOptions {
	return &AgentOptions{
		DownloadManagerOpt: downloadmanager.NewOptions(),
	}
}

func (o *AgentOptions) AddFlags(s *flag.FlagSet) {
	o.DownloadManagerOpt.AddFlags(s)

	flag.Parse()
	s.VisitAll(func(f *flag.Flag) {
		klog.Infof("FLAG: --%s=%q", f.Name, f.Value)
	})
}
