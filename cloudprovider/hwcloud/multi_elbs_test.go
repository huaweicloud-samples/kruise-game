package hwcloud

import (
	"context"
	"encoding/json"
	"testing"

	gamekruiseiov1alpha1 "github.com/openkruise/kruise-game/apis/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestMultiElbsAllocateRefreshesTargetPortsOnConfigChange(t *testing.T) {
	plugin := &MultiElbsPlugin{
		minPort: 6000,
		maxPort: 6005,
		cache: [][]bool{
			{true, false, false, false, false, false},
		},
		podAllocate: map[string]*lbsPorts{
			"default/test-pod-0": {
				index:      0,
				lbIds:      []string{"elb-1"},
				ports:      []int32{6000},
				targetPort: []int{80},
				protocols:  []corev1.Protocol{corev1.ProtocolTCP},
			},
		},
	}

	conf := &multiELBsConfig{
		lbNames:               map[string]string{"elb-1": "pool-a"},
		idList:                [][]string{{"elb-1"}},
		targetPorts:           []int{81},
		protocols:             []corev1.Protocol{corev1.ProtocolTCP},
		allocatePolicy:        "default",
		externalTrafficPolicy: corev1.ServiceExternalTrafficPolicyTypeCluster,
	}

	allocated, err := plugin.allocate(conf, "default/test-pod-0")
	if err != nil {
		t.Fatalf("allocate returned error: %v", err)
	}

	if allocated != plugin.podAllocate["default/test-pod-0"] {
		t.Fatalf("expected existing allocation to be reused")
	}
	if len(allocated.targetPort) != 1 || allocated.targetPort[0] != 81 {
		t.Fatalf("expected target ports to refresh to 81, got %v", allocated.targetPort)
	}
	if len(allocated.protocols) != 1 || allocated.protocols[0] != corev1.ProtocolTCP {
		t.Fatalf("expected protocols to refresh, got %v", allocated.protocols)
	}
	if len(allocated.ports) != 1 || allocated.ports[0] != 6000 {
		t.Fatalf("expected allocated external port to stay 6000, got %v", allocated.ports)
	}
}

func TestMultiElbsConsSvcUsesUpdatedTargetPortAndHealthCheckConfig(t *testing.T) {
	plugin := &MultiElbsPlugin{}
	podLbsPorts := &lbsPorts{
		index:      0,
		lbIds:      []string{"elb-1"},
		ports:      []int32{6000},
		targetPort: []int{81},
		protocols:  []corev1.Protocol{corev1.ProtocolTCP},
	}
	conf := &multiELBsConfig{
		lbNames:               map[string]string{"elb-1": "pool-a"},
		idList:                [][]string{{"elb-1"}},
		targetPorts:           []int{81},
		protocols:             []corev1.Protocol{corev1.ProtocolTCP},
		allocatePolicy:        "default",
		elbClass:              "performance",
		lbHealthCheckFlag:     "on",
		lbHealthCheckConfig:   `[{"protocol":"tcp","pod_target_port":"TCP:81","monitor_port":"8080"}]`,
		externalTrafficPolicy: corev1.ServiceExternalTrafficPolicyTypeCluster,
	}
	pod := &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Pod",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod-0",
			Namespace: "default",
			UID:       "pod-uid-1",
		},
	}

	svc, err := plugin.consSvc(podLbsPorts, conf, pod, "pool-a", nil, context.Background())
	if err != nil {
		t.Fatalf("consSvc returned error: %v", err)
	}

	if len(svc.Spec.Ports) != 1 {
		t.Fatalf("expected 1 service port, got %d", len(svc.Spec.Ports))
	}
	if svc.Spec.Ports[0].TargetPort.IntValue() != 81 {
		t.Fatalf("expected service targetPort 81, got %d", svc.Spec.Ports[0].TargetPort.IntValue())
	}

	got := svc.Annotations[ElbHealthCheckOptionsAnnotationKey]
	var options []HealthCheckOption
	if err := json.Unmarshal([]byte(got), &options); err != nil {
		t.Fatalf("failed to unmarshal health check annotation %q: %v", got, err)
	}
	if len(options) != 1 {
		t.Fatalf("expected 1 health check option, got %d", len(options))
	}
	if options[0].Protocol != "tcp" {
		t.Fatalf("expected protocol tcp, got %s", options[0].Protocol)
	}
	if options[0].TargetServicePort != "TCP:6000" {
		t.Fatalf("expected target_service_port TCP:6000, got %s", options[0].TargetServicePort)
	}
	if options[0].MonitorPort != "8080" {
		t.Fatalf("expected monitor_port 8080, got %s", options[0].MonitorPort)
	}
}

func TestParseMultiElbsConfigReadsHealthCheckOptions(t *testing.T) {
	conf, err := parseMultiELBsConfig([]gamekruiseiov1alpha1.NetworkConfParams{
		{Name: ElbIdNamesConfigName, Value: "elb-1/pool-a"},
		{Name: PortProtocolsConfigName, Value: "81/TCP"},
		{Name: ElbHealthCheckFlagConfigName, Value: "on"},
		{Name: ElbHealthCheckOptionsConfigName, Value: `[{"protocol":"tcp","pod_target_port":"TCP:81"}]`},
	})
	if err != nil {
		t.Fatalf("parseMultiELBsConfig returned error: %v", err)
	}

	if conf.lbHealthCheckConfig != `[{"protocol":"tcp","pod_target_port":"TCP:81"}]` {
		t.Fatalf("expected lbHealthCheckConfig to be preserved, got %s", conf.lbHealthCheckConfig)
	}
}
