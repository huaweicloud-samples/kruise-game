package hwcloud

import (
	"context"
	"encoding/json"
	"testing"

	gamekruiseiov1alpha1 "github.com/openkruise/kruise-game/apis/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
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

func TestInitMultiLBCacheSkipsOutOfRangeServicePorts(t *testing.T) {
	svcList := []corev1.Service{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-pod-0-pool-a",
				Namespace: "default",
				Annotations: map[string]string{
					LBIDBelongIndexKey: "0",
					ElbIdAnnotationKey: "elb-1",
				},
			},
			Spec: corev1.ServiceSpec{
				Selector: map[string]string{
					SvcSelectorKey: "test-pod-0",
				},
				Ports: []corev1.ServicePort{
					{
						Port:       6001,
						Protocol:   corev1.ProtocolTCP,
						TargetPort: intstr.FromInt(80),
					},
					{
						Port:       7001,
						Protocol:   corev1.ProtocolUDP,
						TargetPort: intstr.FromInt(81),
					},
				},
			},
		},
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("initMultiLBCache should skip out-of-range service ports, but panicked: %v", r)
		}
	}()

	podAllocate, cache := initMultiLBCache(svcList, 7000, 14000, nil)
	allocated := podAllocate["default/test-pod-0"]
	if allocated == nil {
		t.Fatalf("expected pod allocation for default/test-pod-0")
	}
	if len(allocated.ports) != 1 || allocated.ports[0] != 7001 {
		t.Fatalf("expected only valid port 7001 to be allocated, got %v", allocated.ports)
	}
	if len(allocated.protocols) != 1 || allocated.protocols[0] != corev1.ProtocolUDP {
		t.Fatalf("expected only valid protocol UDP to be preserved, got %v", allocated.protocols)
	}
	if len(allocated.targetPort) != 1 || allocated.targetPort[0] != 81 {
		t.Fatalf("expected only valid target port 81 to be preserved, got %v", allocated.targetPort)
	}
	if len(cache) != 1 {
		t.Fatalf("expected one cache bucket, got %d", len(cache))
	}
	if !cache[0][1] {
		t.Fatalf("expected port 7001 to be marked as allocated")
	}
}

func TestInitMultiLBCacheMergesTCPUDPServicePorts(t *testing.T) {
	svcList := []corev1.Service{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-pod-0-pool-a",
				Namespace: "default",
				Annotations: map[string]string{
					LBIDBelongIndexKey: "0",
					ElbIdAnnotationKey: "elb-1",
				},
			},
			Spec: corev1.ServiceSpec{
				Selector: map[string]string{
					SvcSelectorKey: "test-pod-0",
				},
				Ports: []corev1.ServicePort{
					{
						Port:       6076,
						Protocol:   corev1.ProtocolTCP,
						TargetPort: intstr.FromInt(8601),
					},
					{
						Port:       6076,
						Protocol:   corev1.ProtocolUDP,
						TargetPort: intstr.FromInt(8601),
					},
					{
						Port:       6077,
						Protocol:   corev1.ProtocolTCP,
						TargetPort: intstr.FromInt(8661),
					},
					{
						Port:       6077,
						Protocol:   corev1.ProtocolUDP,
						TargetPort: intstr.FromInt(8661),
					},
				},
			},
		},
	}

	podAllocate, cache := initMultiLBCache(svcList, 6000, 7000, nil)
	allocated := podAllocate["default/test-pod-0"]
	if allocated == nil {
		t.Fatalf("expected pod allocation for default/test-pod-0")
	}
	if len(allocated.ports) != 2 || allocated.ports[0] != 6076 || allocated.ports[1] != 6077 {
		t.Fatalf("expected TCPUDP service ports to merge into [6076 6077], got %v", allocated.ports)
	}
	if len(allocated.protocols) != 2 || allocated.protocols[0] != ProtocolTCPUDP || allocated.protocols[1] != ProtocolTCPUDP {
		t.Fatalf("expected merged protocols to be [TCPUDP TCPUDP], got %v", allocated.protocols)
	}
	if len(allocated.targetPort) != 2 || allocated.targetPort[0] != 8601 || allocated.targetPort[1] != 8661 {
		t.Fatalf("expected merged target ports to be [8601 8661], got %v", allocated.targetPort)
	}
	if !cache[0][76] {
		t.Fatalf("expected service port 6076 to be marked as allocated")
	}
	if !cache[0][77] {
		t.Fatalf("expected service port 6077 to be marked as allocated")
	}
}

func TestInitMultiLBCacheSkipsOutOfRangeBlockPorts(t *testing.T) {
	svcList := []corev1.Service{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-pod-0-pool-a",
				Namespace: "default",
				Annotations: map[string]string{
					LBIDBelongIndexKey: "0",
					ElbIdAnnotationKey: "elb-1",
				},
			},
			Spec: corev1.ServiceSpec{
				Selector: map[string]string{
					SvcSelectorKey: "test-pod-0",
				},
				Ports: []corev1.ServicePort{
					{
						Port:       7003,
						Protocol:   corev1.ProtocolTCP,
						TargetPort: intstr.FromInt(80),
					},
				},
			},
		},
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("initMultiLBCache should skip out-of-range block ports, but panicked: %v", r)
		}
	}()

	podAllocate, cache := initMultiLBCache(svcList, 7000, 14000, []int32{6999, 7001, 14001})
	allocated := podAllocate["default/test-pod-0"]
	if allocated == nil {
		t.Fatalf("expected pod allocation for default/test-pod-0")
	}
	if len(allocated.ports) != 1 || allocated.ports[0] != 7003 {
		t.Fatalf("expected valid service port 7003 to remain allocated, got %v", allocated.ports)
	}
	if len(cache) != 1 {
		t.Fatalf("expected one cache bucket, got %d", len(cache))
	}
	if !cache[0][1] {
		t.Fatalf("expected in-range block port 7001 to be marked as blocked")
	}
	if !cache[0][3] {
		t.Fatalf("expected service port 7003 to be marked as allocated")
	}
}

func TestInitMultiLBCacheUsesMinMaxParameterOrder(t *testing.T) {
	svcList := []corev1.Service{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-pod-0-pool-a",
				Namespace: "default",
				Annotations: map[string]string{
					LBIDBelongIndexKey: "0",
					ElbIdAnnotationKey: "elb-1",
				},
			},
			Spec: corev1.ServiceSpec{
				Selector: map[string]string{
					SvcSelectorKey: "test-pod-0",
				},
				Ports: []corev1.ServicePort{
					{
						Port:       7002,
						Protocol:   corev1.ProtocolTCP,
						TargetPort: intstr.FromInt(80),
					},
				},
			},
		},
	}

	podAllocate, cache := initMultiLBCache(svcList, 7000, 14000, []int32{7001})
	allocated := podAllocate["default/test-pod-0"]
	if allocated == nil {
		t.Fatalf("expected pod allocation for default/test-pod-0")
	}
	if len(allocated.ports) != 1 || allocated.ports[0] != 7002 {
		t.Fatalf("expected valid service port 7002 to remain allocated, got %v", allocated.ports)
	}
	if len(cache) != 1 {
		t.Fatalf("expected one cache bucket, got %d", len(cache))
	}
	if !cache[0][1] {
		t.Fatalf("expected block port 7001 to be marked as blocked")
	}
	if !cache[0][2] {
		t.Fatalf("expected service port 7002 to be marked as allocated")
	}
}

// Regression: when ElbIdNames shrinks (an ELB group is removed), conf.idList
// becomes shorter than the index some already-running pods still sit on. A
// fresh allocate (new pod) truncates m.cache to len(conf.idList) first; the
// stale high-index pod must then reallocate without panicking on
// m.cache[existingLbs.index] being out of range.
func TestMultiElbsAllocateShrunkIdListDoesNotPanic(t *testing.T) {
	level := func() []bool { return make([]bool, 1000) } // ports [6000,6999]
	plugin := &MultiElbsPlugin{
		minPort: 6000,
		maxPort: 6999,
		cache: [][]bool{
			level(), // index 0
			level(), // index 1
			level(), // index 2 (removed from config but pod still here)
		},
		podAllocate: map[string]*lbsPorts{
			"default/test-pod-b": {
				index:      2,
				lbIds:      []string{"elb-x"},
				ports:      []int32{6076},
				targetPort: []int{80},
				protocols:  []corev1.Protocol{corev1.ProtocolTCP},
			},
		},
	}

	// conf shrank to a single ELB group
	conf := &multiELBsConfig{
		lbNames:        map[string]string{"elb-1": "pool-a"},
		idList:         [][]string{{"elb-1"}},
		targetPorts:    []int{80},
		protocols:      []corev1.Protocol{corev1.ProtocolTCP},
		allocatePolicy: "default",
	}

	// 1) new pod reconciles first -> fresh allocate truncates cache 3 -> 1
	if _, err := plugin.allocate(conf, "default/test-pod-c"); err != nil {
		t.Fatalf("allocate for new pod c returned error: %v", err)
	}
	if len(plugin.cache) != 1 {
		t.Fatalf("expected cache truncated to 1 level, got %d", len(plugin.cache))
	}

	// 2) stale high-index pod reconciles -> must not panic, must reallocate
	var allocated *lbsPorts
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("allocate panicked on shrunk-config pod b: %v", r)
			}
		}()
		var err error
		allocated, err = plugin.allocate(conf, "default/test-pod-b")
		if err != nil {
			t.Fatalf("allocate for stale pod b returned error: %v", err)
		}
	}()

	if allocated.index != 0 {
		t.Fatalf("expected stale pod reallocated to index 0, got %d", allocated.index)
	}
	if len(allocated.ports) != 1 {
		t.Fatalf("expected 1 port reallocated, got %v", allocated.ports)
	}
	if plugin.podAllocate["default/test-pod-b"] == nil || plugin.podAllocate["default/test-pod-b"].index != 0 {
		t.Fatalf("expected stale pod migrated to index 0 in podAllocate")
	}
}
