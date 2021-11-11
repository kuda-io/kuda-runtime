package main

import (
	"fmt"
	"os"

	"k8s.io/component-base/logs"

	"github.com/kuda-io/kuda-runtime/cmd/app"
	"github.com/kuda-io/kuda-runtime/pkg/downloadmanager/plugins/alluxio"
	"github.com/kuda-io/kuda-runtime/pkg/downloadmanager/plugins/hdfs"
)

func main() {
	defer logs.FlushLogs()

	command := app.NewAgentCommand(
		app.WithPlugin(alluxio.Name, alluxio.New),
		app.WithPlugin(hdfs.Name, hdfs.New),
	)

	if err := command.Execute(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}
