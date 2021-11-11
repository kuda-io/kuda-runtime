package app

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	flag "github.com/spf13/pflag"
	"k8s.io/klog/v2"

	"github.com/kuda-io/kuda-runtime/cmd/app/options"
	"github.com/kuda-io/kuda-runtime/pkg/downloadmanager"
	"github.com/kuda-io/kuda-runtime/pkg/downloadmanager/downloader"
	"github.com/kuda-io/kuda-runtime/pkg/gcmanager"
	"github.com/kuda-io/kuda-runtime/pkg/metricmanager"
)

type Option func(registry downloader.Registry)

func NewAgentCommand(registryOptions ...Option) *cobra.Command {
	opts := options.NewAgentOptions()
	opts.AddFlags(flag.CommandLine)

	cmd := &cobra.Command{
		Use: "Kuda-runtime",
		Long: `The Kuda-runtime is Kuda data plane, 
provides data download, garbage data recycling, data distribution and usage collection`,
		Run: func(cmd *cobra.Command, args []string) {
			// init downloader registry
			registry := make(map[string]downloader.PluginFactory)
			for _, opt := range registryOptions {
				opt(registry)
			}

			// run agent
			ctx := context.Background()
			if err := run(ctx, opts, registry); err != nil {
				return
			}

			// gracefully exit
			procExit := make(chan os.Signal)
			signal.Notify(procExit, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGHUP)
			<-procExit
		},
	}
	return cmd
}

func WithPlugin(name string, factory downloader.PluginFactory) Option {
	return func(registry downloader.Registry) {
		registry.Register(name, factory)
	}
}

func run(ctx context.Context, opts *options.AgentOptions, registry downloader.Registry) error {
	downloadManager, err := downloadmanager.New(opts.DownloadManagerOpt, registry)
	if err != nil {
		klog.Errorf("Failed to new downloadManager, err: %s", err)
		return err
	}
	downloadManager.Run(ctx)

	// todo
	metricManager := metricmanager.New()
	metricManager.Run(ctx)

	// todo
	gcManager := gcmanager.New()
	gcManager.Run(ctx)

	return nil
}
