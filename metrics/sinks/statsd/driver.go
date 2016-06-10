package statsd

import (
	"k8s.io/heapster/metrics/core"
	"github.com/golang/glog"
	"net/url"
	"fmt"
	"strings"
	dogstatsd "github.com/DataDog/datadog-go/statsd"
)

type StatsdSink struct {
	uri  *url.URL
}

func (this *StatsdSink) Name() string {
	return "Statsd Sink"
}

func (this *StatsdSink) Stop() {
	// Do nothing.
}

func kubeLabelsToTags(s string) []string{
	var tags []string

	if s == "" {
		return tags
	}

	// prefix each kube label to avoid collision with other labels from heapster
	for _, label := range strings.Split(s, ",") {
		l := strings.SplitN(label, ":", 2)
		if len(l) == 2{
			tags = append(tags, "kube_" + l[0] + ":" + l[1])
		} else {
			tags = append(tags, "kube_"+ l[0])
		}
	}
	glog.Info(fmt.Sprintf("This is the tags: %s", tags))
	return tags
}

func labelsToTags(m map[string]string) []string{

	var tags []string

	for key, value := range m {
		if key == "labels" {
			tags = append(tags, kubeLabelsToTags(value)...)
		} else {
			tags = append(tags, fmt.Sprintf("%s:%s", key, value))
		}
	}
	return tags
}


func (this *StatsdSink) ExportData(batch *core.DataBatch) {
	c, err := dogstatsd.New("statsd:8125")
	if err != nil {
		glog.Fatal(err)
	}
	// prefix every metric with the app name
	c.Namespace = "heapster."
	c.Tags = append(c.Tags, "dangot")
	for _, metricSet := range batch.MetricSets{
		tags := labelsToTags(metricSet.Labels)
		for metricName, metricValue := range metricSet.MetricValues{
			metric_name := fmt.Sprintf("%s.%s", metricSet.Labels["type"], strings.Replace(metricName, "/", ".", -1))
			if core.ValueInt64 == metricValue.ValueType{
				c.Gauge(metric_name, float64(metricValue.IntValue), tags, 1)
			} else {
				c.Gauge(metric_name, float64(metricValue.FloatValue), tags, 1)
			}
		}
	}
}

func NewStatsdSink(u *url.URL) *StatsdSink {
	//FIXME options can be passed in the uri but we don't do anything with it yet
	//statds url, port etc...
	sink := &StatsdSink{
		uri:       u,
	}
	return sink
}

