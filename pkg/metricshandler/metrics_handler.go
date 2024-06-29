package metricshandler

import (
	"compress/gzip"
	"github.com/prometheus/common/expfmt"
	"io"
	"k8s.io/klog/v2"
	ksmtypes "k8s.io/kube-state-metrics/v2/pkg/builder/types"
	metricsstore "k8s.io/kube-state-metrics/v2/pkg/metrics_store"
	"net/http"
	"strings"
	"sync"
)

type MetricsHandler struct {
	readWriteMtx       *sync.RWMutex
	metricsWriters     metricsstore.MetricsWriterList
	enableGZIPEncoding bool
}

func New(enableGZIPEncoding bool) *MetricsHandler {
	return &MetricsHandler{
		readWriteMtx:       &sync.RWMutex{},
		enableGZIPEncoding: enableGZIPEncoding,
	}
}

// Refresh refreshes the metrics served by the MetricsHandler HTTP server.
func (m *MetricsHandler) Refresh(storeBuilder ksmtypes.BuilderInterface) {
	m.readWriteMtx.Lock()
	defer m.readWriteMtx.Unlock()
	m.metricsWriters = storeBuilder.Build()
}

func (m *MetricsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.readWriteMtx.RLock()
	defer m.readWriteMtx.RUnlock()
	resHeader := w.Header()
	var writer io.Writer = w

	contentType := expfmt.NegotiateIncludingOpenMetrics(r.Header)

	// We do not support protobuf at the moment. Fall back to FmtText if the negotiated exposition format is not FmtOpenMetrics See: https://github.com/kubernetes/kube-state-metrics/issues/2022.

	if contentType.FormatType() != expfmt.TypeOpenMetrics {
		contentType = expfmt.NewFormat(expfmt.TypeTextPlain)
	}
	resHeader.Set("Content-Type", string(contentType))

	if m.enableGZIPEncoding {
		// Gzip response if requested. Taken from
		// github.com/prometheus/client_golang/prometheus/promhttp.decorateWriter.
		reqHeader := r.Header.Get("Accept-Encoding")
		parts := strings.Split(reqHeader, ",")
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part == "gzip" || strings.HasPrefix(part, "gzip;") {
				writer = gzip.NewWriter(writer)
				resHeader.Set("Content-Encoding", "gzip")
			}
		}
	}

	m.metricsWriters = metricsstore.SanitizeHeaders(string(contentType), m.metricsWriters)
	for _, w := range m.metricsWriters {
		err := w.WriteAll(writer)
		if err != nil {
			klog.ErrorS(err, "Failed to write metrics")
		}
	}

	// OpenMetrics spec requires that we end with an EOF directive.
	if contentType.FormatType() == expfmt.TypeOpenMetrics {
		_, err := writer.Write([]byte("# EOF\n"))
		if err != nil {
			klog.ErrorS(err, "Failed to write EOF directive")
		}
	}

	// In case we gzipped the response, we have to close the writer.
	if closer, ok := writer.(io.Closer); ok {
		err := closer.Close()
		if err != nil {
			klog.ErrorS(err, "Failed to close the writer")
		}
	}
}
