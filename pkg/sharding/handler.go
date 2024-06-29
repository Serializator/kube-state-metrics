package sharding

import (
	"context"
	"errors"
	"fmt"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	ksmtypes "k8s.io/kube-state-metrics/v2/pkg/builder/types"
	"k8s.io/kube-state-metrics/v2/pkg/metricshandler"
	"k8s.io/kube-state-metrics/v2/pkg/options"
	"strconv"
	"strings"
	"sync"
)

type Handler struct {
	opts           *options.Options
	kubeClient     kubernetes.Interface
	storeBuilder   ksmtypes.BuilderInterface
	metricsHandler *metricshandler.MetricsHandler

	cancel func()

	// mtx protects curShard, and curTotalShards
	shardMtx       *sync.RWMutex
	curShard       int32
	curTotalShards int
}

func NewHandler(
	opts *options.Options,
	kubeClient kubernetes.Interface,
	storeBuilder ksmtypes.BuilderInterface,
	metricsHandler *metricshandler.MetricsHandler,
) *Handler {
	return &Handler{
		opts:           opts,
		kubeClient:     kubeClient,
		storeBuilder:   storeBuilder,
		metricsHandler: metricsHandler,

		shardMtx: &sync.RWMutex{},
	}
}

// ConfigureSharding (re-)configures sharding. Re-configuration can be done
// concurrently.
func (h *Handler) ConfigureSharding(ctx context.Context, shard int32, totalShards int) {
	h.shardMtx.Lock()
	defer h.shardMtx.Unlock()

	if h.cancel != nil {
		h.cancel()
	}
	if totalShards != 1 {
		klog.InfoS("Configuring sharding of this instance to be shard index (zero-indexed) out of total shards", "shard", shard, "totalShards", totalShards)
	}
	ctx, h.cancel = context.WithCancel(ctx)
	h.storeBuilder.WithSharding(shard, totalShards)
	h.storeBuilder.WithContext(ctx)
	h.metricsHandler.Refresh(h.storeBuilder)
	h.curShard = shard
	h.curTotalShards = totalShards
}

func (h *Handler) Run(ctx context.Context) error {
	autoSharding := len(h.opts.Pod) > 0 && len(h.opts.Namespace) > 0

	if !autoSharding {
		klog.InfoS("Autosharding disabled")
		h.ConfigureSharding(ctx, h.opts.Shard, h.opts.TotalShards)
		// Wait for context to be done, metrics will be served until then.
		<-ctx.Done()
		return ctx.Err()
	}

	klog.InfoS("Autosharding enabled with pod", "pod", klog.KRef(h.opts.Namespace, h.opts.Pod))
	klog.InfoS("Auto detecting sharding settings")
	ss, err := detectStatefulSet(h.kubeClient, h.opts.Pod, h.opts.Namespace)
	if err != nil {
		return fmt.Errorf("detect StatefulSet: %w", err)
	}
	statefulSetName := ss.Name

	fieldSelectorOptions := func(o *metav1.ListOptions) {
		o.FieldSelector = fields.OneTermEqualSelector("metadata.name", statefulSetName).String()
	}

	i := cache.NewSharedIndexInformer(
		cache.NewFilteredListWatchFromClient(h.kubeClient.AppsV1().RESTClient(), "statefulsets", h.opts.Namespace, fieldSelectorOptions),
		&appsv1.StatefulSet{}, 0, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
	)
	i.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(o interface{}) {
			ss := o.(*appsv1.StatefulSet)
			if ss.Name != statefulSetName {
				return
			}

			shard, totalShards, err := shardingSettingsFromStatefulSet(ss, h.opts.Pod)
			if err != nil {
				klog.ErrorS(err, "Detected sharding settings from StatefulSet")
				return
			}

			h.shardMtx.RLock()
			shardingUnchanged := h.curShard == shard && h.curTotalShards == totalShards
			h.shardMtx.RUnlock()

			if shardingUnchanged {
				return
			}

			h.ConfigureSharding(ctx, shard, totalShards)
		},
		UpdateFunc: func(oldo, curo interface{}) {
			old := oldo.(*appsv1.StatefulSet)
			cur := curo.(*appsv1.StatefulSet)
			if cur.Name != statefulSetName {
				return
			}

			if old.ResourceVersion == cur.ResourceVersion {
				return
			}

			shard, totalShards, err := shardingSettingsFromStatefulSet(cur, h.opts.Pod)
			if err != nil {
				klog.ErrorS(err, "Detected sharding settings from StatefulSet")
				return
			}

			h.shardMtx.RLock()
			shardingUnchanged := h.curShard == shard && h.curTotalShards == totalShards
			h.shardMtx.RUnlock()

			if shardingUnchanged {
				return
			}

			h.ConfigureSharding(ctx, shard, totalShards)
		},
	})
	go i.Run(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), i.HasSynced) {
		return errors.New("waiting for informer cache to sync failed")
	}
	<-ctx.Done()
	return ctx.Err()
}

func shardingSettingsFromStatefulSet(ss *appsv1.StatefulSet, podName string) (nominal int32, totalReplicas int, err error) {
	nominal, err = detectNominalFromPod(ss.Name, podName)
	if err != nil {
		return 0, 0, fmt.Errorf("detecting Pod nominal: %w", err)
	}

	totalReplicas = 1
	replicas := ss.Spec.Replicas
	if replicas != nil {
		totalReplicas = int(*replicas)
	}

	return nominal, totalReplicas, nil
}

func detectNominalFromPod(statefulSetName, podName string) (int32, error) {
	nominalString := strings.TrimPrefix(podName, statefulSetName+"-")
	nominal, err := strconv.ParseInt(nominalString, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("failed to detect shard index for Pod %s of StatefulSet %s, parsed %s: %w", podName, statefulSetName, nominalString, err)
	}

	return int32(nominal), nil
}

func detectStatefulSet(kubeClient kubernetes.Interface, podName, namespaceName string) (*appsv1.StatefulSet, error) {
	p, err := kubeClient.CoreV1().Pods(namespaceName).Get(context.TODO(), podName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("retrieve pod %s for sharding: %w", podName, err)
	}

	owners := p.GetOwnerReferences()
	for _, o := range owners {
		if o.APIVersion != "apps/v1" || o.Kind != "StatefulSet" || o.Controller == nil || !*o.Controller {
			continue
		}

		ss, err := kubeClient.AppsV1().StatefulSets(namespaceName).Get(context.TODO(), o.Name, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("retrieve shard's StatefulSet: %s/%s: %w", namespaceName, o.Name, err)
		}

		return ss, nil
	}

	return nil, fmt.Errorf("no suitable statefulset found for auto detecting sharding for Pod %s/%s", namespaceName, podName)
}
