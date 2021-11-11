package utils

import (
	"bytes"
	"os"
	"sync"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"

	kuda "github.com/kuda-io/kuda/pkg/generated/clientset/versioned/typed/data/v1alpha1"
)

const (
	MyKubeConfig = "MY_KUBE_CONFIG"
)

var (
	kubeClient *kubernetes.Clientset
	compClient *kuda.DataV1alpha1Client
	kubeLock   sync.Mutex
	compLock   sync.Mutex
)

func GetKubeClient() (*kubernetes.Clientset, error) {
	kubeLock.Lock()
	defer kubeLock.Unlock()

	if kubeClient != nil {
		return kubeClient, nil
	}

	config, err := GetConfig()
	if err != nil {
		return nil, err
	}

	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	kubeClient = client
	return kubeClient, nil
}

func GetCompClient() (*kuda.DataV1alpha1Client, error) {
	compLock.Lock()
	defer compLock.Unlock()

	if compClient != nil {
		return compClient, nil
	}

	config, err := GetConfig()
	if err != nil {
		return nil, err
	}

	client, err := kuda.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	compClient = client
	return compClient, nil
}

func GetConfig() (*rest.Config, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		kubeConfig := os.Getenv(MyKubeConfig)
		if kubeConfig != "" {
			config, err = clientcmd.BuildConfigFromFlags("", kubeConfig)
			if err == nil {
				return config, nil
			}
		}
		return nil, err
	}
	return config, nil
}

func ExecInPod(podNs, podName, containerID string, cmd []string) ([]byte, error) {
	client, err := GetKubeClient()
	if err != nil {
		return nil, err
	}

	req := client.CoreV1().
		RESTClient().
		Post().
		Namespace(podNs).
		Name(podName).
		Resource("pods").
		SubResource("exec").
		Param("container", containerID)

	req.VersionedParams(&v1.PodExecOptions{
		Command: cmd,
		Stdin:   false,
		Stdout:  true,
		Stderr:  true,
		TTY:     false,
	}, scheme.ParameterCodec)

	config, err := GetConfig()
	if err != nil {
		return nil, err
	}

	var stdout, stderr bytes.Buffer
	exec, err := remotecommand.NewSPDYExecutor(config, "POST", req.URL())
	if err != nil {
		return nil, err
	}
	err = exec.Stream(remotecommand.StreamOptions{
		Stdin:  nil,
		Stdout: &stdout,
		Stderr: &stderr,
	})
	if err != nil {
		return nil, err
	}
	return append(stdout.Bytes(), stderr.Bytes()...), err
}
