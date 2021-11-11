package downloadmanager

import (
	flag "github.com/spf13/pflag"
)

type Options struct {
	DownloadRootDir  string
	LocalRootDir     string
	NoticeServerPort int
}

func NewOptions() *Options {
	return &Options{
		DownloadRootDir:  "/data/assets",
		LocalRootDir:     "/data",
		NoticeServerPort: 18888,
	}
}

func (o *Options) AddFlags(s *flag.FlagSet) {
	s.StringVar(&o.DownloadRootDir, "download-root-dir", o.DownloadRootDir, "the directory where downloading data.")
	s.StringVar(&o.LocalRootDir, "local-root-dir", o.LocalRootDir, "the directory where app use data,  data sort link from downloadRootDir.")
	s.IntVar(&o.NoticeServerPort, "notice-server-port", o.NoticeServerPort, "notifier http server port.")
}
