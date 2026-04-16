/*
Copyright 2024 The Kruise Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package hwcloud

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"testing"

	gamekruiseiov1alpha1 "github.com/openkruise/kruise-game/apis/v1alpha1"
	"github.com/openkruise/kruise-game/pkg/util"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestParseMultiELBsConfig(t *testing.T) {
	tests := []struct {
		conf    []gamekruiseiov1alpha1.NetworkConfParams
		want    *multiELBsConfig
		wantErr bool
	}{
		{
			conf: []gamekruiseiov1alpha1.NetworkConfParams{
				{
					Name:  ElbIdNamesConfigName,
					Value: "id-xx-A/name-a, id-xx-B/name-b,id-xx-C/name-a,id-xx-D/name-b",
				},
				{
					Name:  PortProtocolsConfigName,
					Value: "8601/TCPUDP, 8661/TCPUDP",
				},
				{
					Name:  AllocatePolicyConfigName,
					Value: " balanced ",
				},
				{
					Name:  ElbHealthCheckFlagConfigName,
					Value: "off",
				},
				{
					Name:  AllocateLoadBalancerNodePortsConfigName,
					Value: "false",
				},
			},
			want: &multiELBsConfig{
				lbNames: map[string]string{
					"id-xx-A": "name-a",
					"id-xx-B": "name-b",
					"id-xx-C": "name-a",
					"id-xx-D": "name-b",
				},
				idList: [][]string{
					{"id-xx-A", "id-xx-B"},
					{"id-xx-C", "id-xx-D"},
				},
				targetPorts:                   []int{8601, 8661},
				protocols:                     []corev1.Protocol{ProtocolTCPUDP, ProtocolTCPUDP},
				isFixed:                       false,
				externalTrafficPolicy:         corev1.ServiceExternalTrafficPolicyTypeCluster,
				allocatePolicy:                "balanced",
				elbClass:                      "performance",
				lbHealthCheckFlag:             "off",
				lbHealthCheckConfig:           "",
				userDefine:                    "",
				idNums:                        4,
				allocateLoadBalancerNodePorts: false,
			},
		},
	}

	for i, tt := range tests {
		got, err := parseMultiELBsConfig(tt.conf)
		if (err != nil) != tt.wantErr {
			t.Fatalf("case %d: error = %v, wantErr %v", i, err, tt.wantErr)
		}
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("case %d: parseMultiELBsConfig actual: %#v, expect: %#v", i, got, tt.want)
		}
	}
}

func TestInitMultiLBCache(t *testing.T) {
	tests := []struct {
		svcList     []corev1.Service
		maxPort     int32
		minPort     int32
		blockPorts  []int32
		podAllocate map[string]*lbsPorts
		cache       [][]bool
	}{
		{
			svcList: []corev1.Service{
				{
					ObjectMeta: metav1.ObjectMeta{
						Annotations: map[string]string{
							LBIDBelongIndexKey: "0",
							ElbIdAnnotationKey: "elb-id-A",
						},
						Labels: map[string]string{
							ServiceBelongNetworkTypeKey: MultiElbsNetwork,
						},
						Namespace: "ns-0",
						Name:      "name-0",
					},
					Spec: corev1.ServiceSpec{
						Type: corev1.ServiceTypeLoadBalancer,
						Selector: map[string]string{
							SvcSelectorKey: "pod-A",
						},
						Ports: []corev1.ServicePort{
							{
								TargetPort: intstr.FromInt(80),
								Port:       8001,
								Protocol:   corev1.ProtocolTCP,
							},
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Annotations: map[string]string{
							LBIDBelongIndexKey: "0",
							ElbIdAnnotationKey: "elb-id-B",
						},
						Labels: map[string]string{
							ServiceBelongNetworkTypeKey: MultiElbsNetwork,
						},
						Namespace: "ns-0",
						Name:      "name-1",
					},
					Spec: corev1.ServiceSpec{
						Type: corev1.ServiceTypeLoadBalancer,
						Selector: map[string]string{
							SvcSelectorKey: "pod-A",
						},
						Ports: []corev1.ServicePort{
							{
								TargetPort: intstr.FromInt(80),
								Port:       8001,
								Protocol:   corev1.ProtocolTCP,
							},
						},
					},
				},
			},
			maxPort:    int32(8002),
			minPort:    int32(8000),
			blockPorts: []int32{},
			podAllocate: map[string]*lbsPorts{
				"ns-0/pod-A": {
					index:      0,
					lbIds:      []string{"elb-id-A", "elb-id-B"},
					ports:      []int32{8001},
					targetPort: []int{80},
					protocols:  []corev1.Protocol{corev1.ProtocolTCP},
				},
			},
			cache: [][]bool{{false, true, false}},
		},
		{
			svcList: []corev1.Service{
				{
					ObjectMeta: metav1.ObjectMeta{
						Annotations: map[string]string{
							LBIDBelongIndexKey: "0",
							ElbIdAnnotationKey: "elb-id-A",
						},
						Labels: map[string]string{
							ServiceBelongNetworkTypeKey: MultiElbsNetwork,
						},
						Namespace: "ns-0",
						Name:      "name-0",
					},
					Spec: corev1.ServiceSpec{
						Type: corev1.ServiceTypeLoadBalancer,
						Selector: map[string]string{
							SvcSelectorKey: "pod-A",
						},
						Ports: []corev1.ServicePort{
							{
								TargetPort: intstr.FromInt(80),
								Port:       8001,
								Protocol:   corev1.ProtocolTCP,
							},
						},
					},
				},
			},
			maxPort:    int32(8002),
			minPort:    int32(8000),
			blockPorts: []int32{7999, 8002},
			podAllocate: map[string]*lbsPorts{
				"ns-0/pod-A": {
					index:      0,
					lbIds:      []string{"elb-id-A"},
					ports:      []int32{8001},
					targetPort: []int{80},
					protocols:  []corev1.Protocol{corev1.ProtocolTCP},
				},
			},
			cache: [][]bool{{false, true, true}},
		},
	}

	for i, tt := range tests {
		podAllocate, cache := initMultiLBCache(tt.svcList, tt.maxPort, tt.minPort, tt.blockPorts)
		if !reflect.DeepEqual(podAllocate, tt.podAllocate) {
			t.Errorf("case %d: podAllocate actual: %v, expect: %v", i, podAllocate, tt.podAllocate)
		}
		if !reflect.DeepEqual(cache, tt.cache) {
			t.Errorf("case %d: cache actual: %v, expect: %v", i, cache, tt.cache)
		}
	}
}

func TestMultiElbsPlugin_allocate(t *testing.T) {
	tests := []struct {
		plugin           *MultiElbsPlugin
		conf             *multiELBsConfig
		nsName           string
		want             *lbsPorts
		cacheAfter       [][]bool
		podAllocateAfter map[string]*lbsPorts
	}{
		{
			plugin: &MultiElbsPlugin{
				maxPort:     int32(8001),
				minPort:     int32(8000),
				blockPorts:  []int32{},
				mutex:       sync.RWMutex{},
				cache:       [][]bool{{true, false}, {false, false}},
				podAllocate: map[string]*lbsPorts{},
			},
			conf: &multiELBsConfig{
				idList:         [][]string{{"id-xx-A", "id-xx-B"}, {"id-xx-C", "id-xx-D"}},
				targetPorts:    []int{80},
				protocols:      []corev1.Protocol{corev1.ProtocolTCP},
				allocatePolicy: "balanced",
			},
			nsName: "default/test-0",
			want: &lbsPorts{
				index:      1,
				lbIds:      []string{"id-xx-C", "id-xx-D"},
				ports:      []int32{8000},
				targetPort: []int{80},
				protocols:  []corev1.Protocol{corev1.ProtocolTCP},
			},
			cacheAfter: [][]bool{{true, false}, {true, false}},
			podAllocateAfter: map[string]*lbsPorts{
				"default/test-0": {
					index:      1,
					lbIds:      []string{"id-xx-C", "id-xx-D"},
					ports:      []int32{8000},
					targetPort: []int{80},
					protocols:  []corev1.Protocol{corev1.ProtocolTCP},
				},
			},
		},
		{
			plugin: &MultiElbsPlugin{
				maxPort:    int32(8001),
				minPort:    int32(8000),
				blockPorts: []int32{},
				mutex:      sync.RWMutex{},
				cache:      [][]bool{{true, false}},
				podAllocate: map[string]*lbsPorts{
					"default/test-0": {
						index:      0,
						lbIds:      []string{"id-xx-A", "id-xx-B"},
						ports:      []int32{8000},
						targetPort: []int{80},
						protocols:  []corev1.Protocol{corev1.ProtocolTCP},
					},
				},
			},
			conf: &multiELBsConfig{
				idList:         [][]string{{"id-xx-A", "id-xx-B"}},
				targetPorts:    []int{80},
				protocols:      []corev1.Protocol{corev1.ProtocolTCP},
				allocatePolicy: "default",
			},
			nsName: "default/test-0",
			want: &lbsPorts{
				index:      0,
				lbIds:      []string{"id-xx-A", "id-xx-B"},
				ports:      []int32{8000},
				targetPort: []int{80},
				protocols:  []corev1.Protocol{corev1.ProtocolTCP},
			},
			cacheAfter: [][]bool{{true, false}},
			podAllocateAfter: map[string]*lbsPorts{
				"default/test-0": {
					index:      0,
					lbIds:      []string{"id-xx-A", "id-xx-B"},
					ports:      []int32{8000},
					targetPort: []int{80},
					protocols:  []corev1.Protocol{corev1.ProtocolTCP},
				},
			},
		},
	}

	for i, tt := range tests {
		got, err := tt.plugin.allocate(tt.conf, tt.nsName)
		if err != nil {
			t.Fatalf("case %d: allocate error: %v", i, err)
		}
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("case %d: allocate actual: %v, expect: %v", i, got, tt.want)
		}
		if !reflect.DeepEqual(tt.plugin.cache, tt.cacheAfter) {
			t.Errorf("case %d: cache actual: %v, expect: %v", i, tt.plugin.cache, tt.cacheAfter)
		}
		if !reflect.DeepEqual(tt.plugin.podAllocate, tt.podAllocateAfter) {
			t.Errorf("case %d: podAllocate actual: %v, expect: %v", i, tt.plugin.podAllocate, tt.podAllocateAfter)
		}
	}
}

func TestDeAllocate_OutOfRangeNoPanic(t *testing.T) {
	plugin := &MultiElbsPlugin{
		maxPort:    int32(8001),
		minPort:    int32(8000),
		blockPorts: []int32{},
		mutex:      sync.RWMutex{},
		cache:      [][]bool{{true, false}},
		podAllocate: map[string]*lbsPorts{
			"default/test-0": {
				index:      0,
				lbIds:      []string{"id-xx-A", "id-xx-B"},
				ports:      []int32{8000, 7999},
				targetPort: []int{80, 80},
				protocols:  []corev1.Protocol{corev1.ProtocolTCP, corev1.ProtocolTCP},
			},
		},
	}

	plugin.deAllocate("default/test-0")

	wantCache := [][]bool{{false, false}}
	if !reflect.DeepEqual(plugin.cache, wantCache) {
		t.Errorf("cache actual: %v, expect: %v", plugin.cache, wantCache)
	}
	if len(plugin.podAllocate) != 0 {
		t.Errorf("podAllocate actual: %v, expect empty", plugin.podAllocate)
	}
}

func TestApplyServiceUpdateControlledKeys(t *testing.T) {
	current := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				"keep":                      "1",
				ServiceBelongNetworkTypeKey: "old",
			},
			Annotations: map[string]string{
				ElbIdAnnotationKey:                 "old-elb",
				ElbConfigHashKey:                   "oldhash",
				ElbHealthCheckFlagAnnotationKey:    "on",
				ElbHealthCheckOptionsAnnotationKey: `{"a":1}`,
				LBIDBelongIndexKey:                 "0",
				ElbMappingPoolAnnotationKey:        "oldpool",
				ElbClassAnnotationKey:              "oldclass",
				ElbPortMappingResultCount:          "2",
				"custom/keep":                      "x",
			},
			OwnerReferences: []metav1.OwnerReference{{Kind: "Pod", Name: "old", UID: "111"}},
		},
		Spec: corev1.ServiceSpec{
			Type:                          corev1.ServiceTypeClusterIP,
			ExternalTrafficPolicy:         corev1.ServiceExternalTrafficPolicyTypeCluster,
			AllocateLoadBalancerNodePorts: ptr.To(true),
			Selector:                      map[string]string{"a": "b"},
			Ports:                         []corev1.ServicePort{{Name: "p", Port: 1}},
		},
	}

	desired := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				ServiceBelongNetworkTypeKey: MultiElbsNetwork,
			},
			Annotations: map[string]string{
				ElbIdAnnotationKey:              "new-elb",
				ElbConfigHashKey:                "newhash",
				ElbHealthCheckFlagAnnotationKey: "off",
				LBIDBelongIndexKey:              "1",
				ElbMappingPoolAnnotationKey:     "newpool",
				ElbClassAnnotationKey:           "performance",
				ElbPortMappingResultCount:       "4",
				"user/added":                    "y",
			},
			OwnerReferences: []metav1.OwnerReference{{Kind: "Pod", Name: "new", UID: "222"}},
		},
		Spec: corev1.ServiceSpec{
			Type:                          corev1.ServiceTypeLoadBalancer,
			ExternalTrafficPolicy:         corev1.ServiceExternalTrafficPolicyTypeLocal,
			AllocateLoadBalancerNodePorts: ptr.To(false),
			Selector:                      map[string]string{SvcSelectorKey: "pod"},
			Ports:                         []corev1.ServicePort{{Name: "new", Port: 2}},
		},
	}

	applyMultiElbsServiceUpdate(current, desired)

	if current.Labels["keep"] != "1" {
		t.Errorf("keep label should be preserved, got %v", current.Labels["keep"])
	}
	if current.Labels[ServiceBelongNetworkTypeKey] != MultiElbsNetwork {
		t.Errorf("network-type label actual: %v, expect: %v", current.Labels[ServiceBelongNetworkTypeKey], MultiElbsNetwork)
	}
	if current.Annotations["custom/keep"] != "x" {
		t.Errorf("custom/keep annotation should be preserved, got %v", current.Annotations["custom/keep"])
	}
	if current.Annotations[ElbIdAnnotationKey] != "new-elb" {
		t.Errorf("elb id annotation actual: %v, expect: %v", current.Annotations[ElbIdAnnotationKey], "new-elb")
	}
	if _, ok := current.Annotations[ElbHealthCheckOptionsAnnotationKey]; ok {
		t.Errorf("health check options annotation should be deleted when desired does not contain it")
	}
	if current.Annotations["user/added"] != "y" {
		t.Errorf("user/added annotation actual: %v, expect: %v", current.Annotations["user/added"], "y")
	}
	if !reflect.DeepEqual(current.OwnerReferences, desired.OwnerReferences) {
		t.Errorf("ownerReferences actual: %v, expect: %v", current.OwnerReferences, desired.OwnerReferences)
	}
	if !reflect.DeepEqual(current.Spec.Ports, desired.Spec.Ports) {
		t.Errorf("ports actual: %v, expect: %v", current.Spec.Ports, desired.Spec.Ports)
	}
	if !reflect.DeepEqual(current.Spec.Selector, desired.Spec.Selector) {
		t.Errorf("selector actual: %v, expect: %v", current.Spec.Selector, desired.Spec.Selector)
	}
	if current.Spec.Type != desired.Spec.Type {
		t.Errorf("type actual: %v, expect: %v", current.Spec.Type, desired.Spec.Type)
	}
	if current.Spec.ExternalTrafficPolicy != desired.Spec.ExternalTrafficPolicy {
		t.Errorf("externalTrafficPolicy actual: %v, expect: %v", current.Spec.ExternalTrafficPolicy, desired.Spec.ExternalTrafficPolicy)
	}
	if !reflect.DeepEqual(current.Spec.AllocateLoadBalancerNodePorts, desired.Spec.AllocateLoadBalancerNodePorts) {
		t.Errorf("allocateLoadBalancerNodePorts actual: %v, expect: %v", current.Spec.AllocateLoadBalancerNodePorts, desired.Spec.AllocateLoadBalancerNodePorts)
	}
}

func TestConsSvc_UserDefineCannotOverrideControlledKeys(t *testing.T) {
	plugin := &MultiElbsPlugin{}
	pod := &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Pod",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "pod-0",
			UID:       "uid-0",
		},
	}
	podLbsPorts := &lbsPorts{
		index:      0,
		lbIds:      []string{"elb-1"},
		ports:      []int32{8000},
		targetPort: []int{80},
		protocols:  []corev1.Protocol{corev1.ProtocolTCP},
	}

	conf := &multiELBsConfig{
		lbNames: map[string]string{"elb-1": "pool-a"},
		idList:  [][]string{{"elb-1"}},

		targetPorts:    []int{80},
		protocols:      []corev1.Protocol{corev1.ProtocolTCP},
		isFixed:        false,
		allocatePolicy: "default",
		elbClass:       "performance",

		lbHealthCheckFlag:   "on",
		lbHealthCheckConfig: "",

		userDefine: `{
			"kubernetes.io/elb.id":"evil-elb",
			"game.kruise.io/network-config-hash":"evil-hash",
			"kubernetes.io/elb.health-check-flag":"off",
			"custom/ok":"1"
		}`,
		allocateLoadBalancerNodePorts: true,
	}

	svc, err := plugin.consSvc(podLbsPorts, conf, pod, "pool-a", nil, context.TODO())
	if err != nil {
		t.Fatalf("consSvc error: %v", err)
	}

	if svc.Annotations[ElbIdAnnotationKey] != "elb-1" {
		t.Errorf("elb id annotation actual: %v, expect: %v", svc.Annotations[ElbIdAnnotationKey], "elb-1")
	}
	if svc.Annotations[ElbConfigHashKey] != util.GetHash(conf) {
		t.Errorf("config hash actual: %v, expect: %v", svc.Annotations[ElbConfigHashKey], util.GetHash(conf))
	}
	if svc.Annotations[ElbHealthCheckFlagAnnotationKey] != "on" {
		t.Errorf("health check flag actual: %v, expect: %v", svc.Annotations[ElbHealthCheckFlagAnnotationKey], "on")
	}
	if svc.Annotations["custom/ok"] != "1" {
		t.Errorf("custom annotation actual: %v, expect: %v", svc.Annotations["custom/ok"], "1")
	}
}

func TestProcessHealthCheckOptions_PartialInvalidDoesNotAbort(t *testing.T) {
	podLbsPorts := &lbsPorts{
		ports:      []int32{8001},
		targetPort: []int{80},
		protocols:  []corev1.Protocol{corev1.ProtocolTCP},
	}
	healthCheckConfig := `[
		{"pod_target_port":" tcp : 80 ","monitor_port":"1"},
		{"pod_target_port":"bad"},
		{"pod_target_port":"TCP:notnum"},
		{"monitor_port":"2"}
	]`

	out, err := processHealthCheckOptions(healthCheckConfig, podLbsPorts)
	if err != nil {
		t.Fatalf("processHealthCheckOptions error: %v", err)
	}
	if out == "" {
		t.Fatalf("expect non-empty processed config")
	}

	var got []HealthCheckOption
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("unmarshal processed config error: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expect 1 processed option, got %d: %+v", len(got), got)
	}
	if got[0].PodTargetPort != "" {
		t.Errorf("PodTargetPort should be cleared, got %q", got[0].PodTargetPort)
	}
	if got[0].TargetServicePort != "TCP:8001" {
		t.Errorf("TargetServicePort actual: %q, expect: %q", got[0].TargetServicePort, "TCP:8001")
	}
	if got[0].MonitorPort != "1" {
		t.Errorf("MonitorPort should be preserved, got %q", got[0].MonitorPort)
	}
}

func TestMultiElbsPlugin_OnPodUpdated_ExternalIPFilled(t *testing.T) {
	plugin := &MultiElbsPlugin{
		maxPort:     int32(8000),
		minPort:     int32(8000),
		blockPorts:  []int32{},
		cache:       nil,
		podAllocate: map[string]*lbsPorts{},
		mutex:       sync.RWMutex{},
	}

	networkConf := []gamekruiseiov1alpha1.NetworkConfParams{
		{
			Name:  ElbIdNamesConfigName,
			Value: "elb-1/pool-a",
		},
		{
			Name:  PortProtocolsConfigName,
			Value: "80/TCP",
		},
		{
			Name:  ElbHealthCheckFlagConfigName,
			Value: "off",
		},
	}
	conf, err := parseMultiELBsConfig(networkConf)
	if err != nil {
		t.Fatalf("parseMultiELBsConfig error: %v", err)
	}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "pod-0-pool-a",
			Annotations: map[string]string{
				ElbConfigHashKey: util.GetHash(conf),
			},
		},
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeLoadBalancer,
			Selector: map[string]string{
				SvcSelectorKey: "pod-0",
			},
			Ports: []corev1.ServicePort{
				{
					Name:       "80-tcp",
					Port:       8000,
					Protocol:   corev1.ProtocolTCP,
					TargetPort: intstr.FromInt(80),
				},
			},
		},
		Status: corev1.ServiceStatus{
			LoadBalancer: corev1.LoadBalancerStatus{
				Ingress: []corev1.LoadBalancerIngress{
					{IP: "1.2.3.4"},
				},
			},
		},
	}

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(svc).Build()

	networkConfBytes, _ := json.Marshal(networkConf)
	networkStatusBytes, _ := json.Marshal(gamekruiseiov1alpha1.NetworkStatus{CurrentNetworkState: gamekruiseiov1alpha1.NetworkNotReady})
	pod := &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Pod",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "pod-0",
			UID:       "uid-0",
			Labels:    map[string]string{},
			Annotations: map[string]string{
				gamekruiseiov1alpha1.GameServerNetworkType:   MultiElbsNetwork,
				gamekruiseiov1alpha1.GameServerNetworkConf:   string(networkConfBytes),
				gamekruiseiov1alpha1.GameServerNetworkStatus: string(networkStatusBytes),
			},
		},
		Status: corev1.PodStatus{
			PodIP: "10.0.0.1",
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}

	updatedPod, perr := plugin.OnPodUpdated(c, pod, context.TODO())
	if perr != nil {
		t.Fatalf("OnPodUpdated error: %v", perr)
	}

	var got gamekruiseiov1alpha1.NetworkStatus
	if err := json.Unmarshal([]byte(updatedPod.Annotations[gamekruiseiov1alpha1.GameServerNetworkStatus]), &got); err != nil {
		t.Fatalf("unmarshal network status error: %v", err)
	}
	if got.CurrentNetworkState != gamekruiseiov1alpha1.NetworkReady {
		t.Fatalf("CurrentNetworkState actual: %v, expect: %v", got.CurrentNetworkState, gamekruiseiov1alpha1.NetworkReady)
	}
	if len(got.ExternalAddresses) == 0 {
		t.Fatalf("expect external addresses to be set")
	}
	if got.ExternalAddresses[0].IP != "1.2.3.4" {
		t.Fatalf("external IP actual: %q, expect: %q", got.ExternalAddresses[0].IP, "1.2.3.4")
	}
}
