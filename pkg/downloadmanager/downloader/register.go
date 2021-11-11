package downloader

import (
	"fmt"
)

type Registry map[string]PluginFactory

func (r Registry) Register(name string, factory PluginFactory) {
	r[name] = factory
}

// Unregister removes an existing plugins from the registry. If no plugins with
// the provided name exists, it returns an error.
func (r Registry) Unregister(name string) error {
	if _, ok := r[name]; !ok {
		return fmt.Errorf("no plugins named %v exists", name)
	}
	delete(r, name)
	return nil
}

func (r Registry) Get(name string) (PluginFactory, error) {
	pluginFactory, ok := r[name]
	if !ok {
		return nil, fmt.Errorf("a plugins named %s not exists", name)
	}
	return pluginFactory, nil
}
