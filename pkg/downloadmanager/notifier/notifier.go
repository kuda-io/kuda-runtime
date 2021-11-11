package notifier

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/kuda-io/kuda-runtime/pkg/api/v1alpha1"
	"github.com/julienschmidt/httprouter"
)

type Notifier struct {
	noticeCh         chan *v1alpha1.Task
	noticeServerPort int
	resp             *v1alpha1.NoticeResp
}

func New(noticeServerPort int, noticeCh chan *v1alpha1.Task) *Notifier {
	return &Notifier{
		noticeServerPort: noticeServerPort,
		noticeCh:         noticeCh,
		resp:             &v1alpha1.NoticeResp{},
	}
}

func (n *Notifier) Run() {
	go func() {
		for {
			select {
			case task, ok := <-n.noticeCh:
				if ok {
					data := make([]v1alpha1.NoticeDataItem, 0)
					for _, item := range task.Items {
						data = append(data, v1alpha1.NoticeDataItem{
							Name:      item.Name,
							Namespace: item.Namespace,
							Version:   item.Version,
							LocalPath: item.LocalPath,
						})
					}
					n.resp.Data = data
				}
			}
		}
	}()

	go func() {
		router := httprouter.New()
		router.GET("/api/v1/data", func(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
			json.NewEncoder(w).Encode(n.resp)
		})

		addr := fmt.Sprintf(":%d", n.noticeServerPort)
		http.ListenAndServe(addr, router)
	}()
}
