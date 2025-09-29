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
	"fmt"
	"strconv"
	"strings"
	"sync"

	gamekruiseiov1alpha1 "github.com/openkruise/kruise-game/apis/v1alpha1"
	"github.com/openkruise/kruise-game/cloudprovider"
	cperrors "github.com/openkruise/kruise-game/cloudprovider/errors"
	provideroptions "github.com/openkruise/kruise-game/cloudprovider/options"
	"github.com/openkruise/kruise-game/cloudprovider/utils"
	"github.com/openkruise/kruise-game/pkg/util"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	log "k8s.io/klog/v2"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	MultiElbsNetwork = "HuaweiCloud-Multi-ELBs"
	AliasMultiElbs   = "Multi-ELBs-Network"

	// ConfigNames defined by OKG
	ElbIdNamesConfigName     = "ElbIdNames"
	AllocatePolicyConfigName = "AllocatePolicy"

	// service annotation defined by OKG
	LBIDBelongIndexKey = "game.kruise.io/lb-belong-index"

	// service label defined by OKG
	ServiceBelongNetworkTypeKey = "game.kruise.io/network-type"

	PrefixReadyReadinessGate = "target-health.elb.k8s.cce/"

	ElbMappingPoolAnnotationKey = "cce.io/game.kruise.isp-name"

	ElbHealthCheckFlagAnnotationKey = "kubernetes.io/elb.health-check-flag"
	ElbHealthCheckFlagConfigName    = "LBHealthCheckFlag"

	ElbHealthCheckOptionsAnnotationKey      = "kubernetes.io/elb.health-check-options"
	ElbHealthCheckOptionsConfigName         = "LBHealthCheckConfig"
	ElbUserDefineConfigName                 = "UserDefine"
	AllocateLoadBalancerNodePortsConfigName = "AllocateLoadBalancerNodePorts"

	ElbPortMappingResultCount = "cce.io/game.kruise.mapping-result-count"
)

var (
	notAllowedAnnotationKeyMap = map[string]struct{}{
		ElbAutocreateAnnotationKey:         {},
		ElbMappingPoolAnnotationKey:        {},
		ElbClassAnnotationKey:              {},
		ElbIdAnnotationKey:                 {},
		ElbHealthCheckOptionsAnnotationKey: {},
	}
)

type MultiElbsPlugin struct {
	maxPort    int32
	minPort    int32
	blockPorts []int32
	cache      [][]bool
	// podAllocate format {pod ns/name}: -{lbId: xxx-a, port: -8001 -8002} -{lbId: xxx-b, port: -8001 -8002}
	podAllocate map[string]*lbsPorts
	mutex       sync.RWMutex
}

type lbsPorts struct {
	index      int
	lbIds      []string
	ports      []int32
	targetPort []int
	protocols  []corev1.Protocol
}

func (m *MultiElbsPlugin) Name() string {
	return MultiElbsNetwork
}

func (m *MultiElbsPlugin) Alias() string {
	return AliasMultiElbs
}

func (m *MultiElbsPlugin) Init(c client.Client, options cloudprovider.CloudProviderOptions, ctx context.Context) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	elbOptions := options.(provideroptions.HwCloudOptions).CCEELBOptions.MultiELBOptions
	m.minPort = elbOptions.MinPort
	m.maxPort = elbOptions.MaxPort
	m.blockPorts = elbOptions.BlockPorts

	svcList := &corev1.ServiceList{}
	err := c.List(ctx, svcList, client.MatchingLabels{ServiceBelongNetworkTypeKey: MultiElbsNetwork})
	if err != nil {
		return err
	}
	m.podAllocate, m.cache = initMultiLBCache(svcList.Items, m.maxPort, m.minPort, m.blockPorts)

	log.Infof("[%s] podAllocate cache complete initialization: ", MultiElbsNetwork)
	for podNsName, lps := range m.podAllocate {
		log.Infof("[%s] pod %s: %v", MultiElbsNetwork, podNsName, *lps)
	}
	return nil
}

func initMultiLBCache(svcList []corev1.Service, maxPort, minPort int32, blockPorts []int32) (map[string]*lbsPorts, [][]bool) {
	podAllocate := make(map[string]*lbsPorts)
	cache := make([][]bool, 0)

	for _, svc := range svcList {
		index, err := strconv.Atoi(svc.GetAnnotations()[LBIDBelongIndexKey])
		if err != nil {
			continue
		}
		lenCache := len(cache)
		for i := lenCache; i <= index; i++ {
			cacheLevel := make([]bool, int(maxPort-minPort)+1)
			for _, p := range blockPorts {
				cacheLevel[int(p-minPort)] = true
			}
			cache = append(cache, cacheLevel)
		}

		ports := make([]int32, 0)
		protocols := make([]corev1.Protocol, 0)
		targetPorts := make([]int, 0)
		for _, port := range svc.Spec.Ports {
			cache[index][(port.Port - minPort)] = true
			ports = append(ports, port.Port)
			protocols = append(protocols, port.Protocol)
			targetPorts = append(targetPorts, port.TargetPort.IntValue())
		}

		nsName := svc.GetNamespace() + "/" + svc.Spec.Selector[SvcSelectorKey]
		if podAllocate[nsName] == nil {
			podAllocate[nsName] = &lbsPorts{
				index:      index,
				lbIds:      []string{svc.Labels[ElbIdAnnotationKey]},
				ports:      ports,
				protocols:  protocols,
				targetPort: targetPorts,
			}
		} else {
			podAllocate[nsName].lbIds = append(podAllocate[nsName].lbIds, svc.Labels[ElbIdAnnotationKey])
		}
	}
	return podAllocate, cache
}

func (m *MultiElbsPlugin) OnPodAdded(c client.Client, pod *corev1.Pod, ctx context.Context) (*corev1.Pod, cperrors.PluginError) {
	networkManager := utils.NewNetworkManager(pod, c)
	networkConfig := networkManager.GetNetworkConfig()
	conf, err := parseMultiELBsConfig(networkConfig)
	if err != nil {
		return pod, cperrors.NewPluginError(cperrors.ParameterError, err.Error())
	}
	var lbNames []string
	for _, lbName := range conf.lbNames {
		if !util.IsStringInList(lbName, lbNames) {
			lbNames = append(lbNames, lbName)
		}
	}
	//for _, lbName := range lbNames {
	//	pod.Spec.ReadinessGates = append(pod.Spec.ReadinessGates, corev1.PodReadinessGate{
	//		ConditionType: corev1.PodConditionType(PrefixReadyReadinessGate + pod.GetName() + "-" + strings.ToLower(lbName)),
	//	})
	//}

	return pod, nil
}

func (m *MultiElbsPlugin) OnPodUpdated(c client.Client, pod *corev1.Pod, ctx context.Context) (*corev1.Pod, cperrors.PluginError) {
	networkManager := utils.NewNetworkManager(pod, c)

	networkStatus, _ := networkManager.GetNetworkStatus()
	networkConfig := networkManager.GetNetworkConfig()
	if networkStatus == nil {
		pod, err := networkManager.UpdateNetworkStatus(gamekruiseiov1alpha1.NetworkStatus{
			CurrentNetworkState: gamekruiseiov1alpha1.NetworkNotReady,
		}, pod)
		return pod, cperrors.ToPluginError(err, cperrors.InternalError)
	}

	conf, err := parseMultiELBsConfig(networkConfig)
	if err != nil {
		return pod, cperrors.NewPluginError(cperrors.ParameterError, err.Error())
	}

	podNsName := pod.GetNamespace() + "/" + pod.GetName()
	podLbsPorts, err := m.allocate(conf, podNsName)
	if err != nil {
		return pod, cperrors.ToPluginError(err, cperrors.ParameterError)
	}

	// 收集需要更新的Service
	var servicesToUpdate []*corev1.Service
	var servicesToCreate []*corev1.Service
	var needNetworkNotReady bool

	for _, lbId := range conf.idList[podLbsPorts.index] {
		// get svc
		lbName := conf.lbNames[lbId]
		svc := &corev1.Service{}
		err = c.Get(ctx, types.NamespacedName{
			Name:      pod.GetName() + "-" + strings.ToLower(lbName),
			Namespace: pod.GetNamespace(),
		}, svc)
		if err != nil {
			if errors.IsNotFound(err) {
				service, err := m.consSvc(podLbsPorts, conf, pod, lbName, c, ctx)
				if err != nil {
					return pod, cperrors.ToPluginError(err, cperrors.ParameterError)
				}
				servicesToCreate = append(servicesToCreate, service)
			} else {
				return pod, cperrors.NewPluginError(cperrors.ApiCallError, err.Error())
			}
		} else {
			// old svc remain
			if svc.OwnerReferences[0].Kind == "Pod" && svc.OwnerReferences[0].UID != pod.UID {
				log.Infof("[%s] waiting old svc %s/%s deleted. old owner pod uid is %s, but now is %s", "HwCloud-ELB", svc.Namespace, svc.Name, svc.OwnerReferences[0].UID, pod.UID)
				return pod, nil
			}

			// 检查是否需要更新配置
			if util.GetHash(conf) != svc.GetAnnotations()[ElbConfigHashKey] {
				needNetworkNotReady = true
				service, err := m.consSvc(podLbsPorts, conf, pod, lbName, c, ctx)
				if err != nil {
					return pod, cperrors.ToPluginError(err, cperrors.ParameterError)
				}
				servicesToUpdate = append(servicesToUpdate, service)
			}
		}
	}

	// 先创建所有需要创建的Service
	for _, service := range servicesToCreate {
		err = c.Create(ctx, service)
		if err != nil {
			return pod, cperrors.NewPluginError(cperrors.ApiCallError, err.Error())
		}
	}

	// 更新所有需要更新的Service
	for _, service := range servicesToUpdate {
		err = c.Update(ctx, service)
		if err != nil {
			return pod, cperrors.NewPluginError(cperrors.ApiCallError, err.Error())
		}
	}

	// 如果有Service被更新或创建，说明网络配置发生变化，需要设置网络为NotReady状态
	if len(servicesToUpdate) > 0 || len(servicesToCreate) > 0 {
		if needNetworkNotReady && networkStatus != nil {
			networkStatus.CurrentNetworkState = gamekruiseiov1alpha1.NetworkNotReady
			pod, err = networkManager.UpdateNetworkStatus(*networkStatus, pod)
			if err != nil {
				return pod, cperrors.NewPluginError(cperrors.InternalError, err.Error())
			}
		}
		// 返回，等待下一次reconcile
		return pod, nil
	}

	// 处理剩余的未更新Service的状态检查
	endPoints := ""
	for i, lbId := range conf.idList[podLbsPorts.index] {
		lbName := conf.lbNames[lbId]
		svc := &corev1.Service{}
		err = c.Get(ctx, types.NamespacedName{
			Name:      pod.GetName() + "-" + strings.ToLower(lbName),
			Namespace: pod.GetNamespace(),
		}, svc)
		if err != nil {
			if !errors.IsNotFound(err) {
				return pod, cperrors.NewPluginError(cperrors.ApiCallError, err.Error())
			}
			continue
		}

		// disable network
		if networkManager.GetNetworkDisabled() && svc.Spec.Type == corev1.ServiceTypeLoadBalancer {
			svc.Spec.Type = corev1.ServiceTypeClusterIP
			return pod, cperrors.ToPluginError(c.Update(ctx, svc), cperrors.ApiCallError)
		}

		// enable network
		if !networkManager.GetNetworkDisabled() && svc.Spec.Type == corev1.ServiceTypeClusterIP {
			svc.Spec.Type = corev1.ServiceTypeLoadBalancer
			return pod, cperrors.ToPluginError(c.Update(ctx, svc), cperrors.ApiCallError)
		}

		// network not ready
		if svc.Status.LoadBalancer.Ingress == nil || len(svc.Status.LoadBalancer.Ingress) == 0 {
			networkStatus.CurrentNetworkState = gamekruiseiov1alpha1.NetworkNotReady
			pod, err = networkManager.UpdateNetworkStatus(*networkStatus, pod)
			return pod, cperrors.ToPluginError(err, cperrors.InternalError)
		}

		ingressIP := svc.Status.LoadBalancer.Ingress[0].IP
		_, readyCondition := util.GetPodConditionFromList(pod.Status.Conditions, corev1.PodReady)
		if readyCondition == nil || readyCondition.Status == corev1.ConditionFalse {
			networkStatus.CurrentNetworkState = gamekruiseiov1alpha1.NetworkNotReady
			pod, err = networkManager.UpdateNetworkStatus(*networkStatus, pod)
			return pod, cperrors.ToPluginError(err, cperrors.InternalError)
		}

		// allow not ready containers
		if util.IsAllowNotReadyContainers(networkManager.GetNetworkConfig()) {
			toUpDateSvc, err := utils.AllowNotReadyContainers(c, ctx, pod, svc, false)
			if err != nil {
				return pod, err
			}

			if toUpDateSvc {
				err := c.Update(ctx, svc)
				if err != nil {
					return pod, cperrors.ToPluginError(err, cperrors.ApiCallError)
				}
			}
		}

		// network ready
		internalAddresses := make([]gamekruiseiov1alpha1.NetworkAddress, 0)
		externalAddresses := make([]gamekruiseiov1alpha1.NetworkAddress, 0)

		host := svc.Status.LoadBalancer.Ingress[0].Hostname
		if host == "" {
			host = ingressIP
		}
		endPoints = endPoints + host + "/" + lbName
		if i != len(conf.idList[podLbsPorts.index])-1 {
			endPoints = endPoints + ","
		}
		for _, port := range svc.Spec.Ports {
			instrIPort := port.TargetPort
			instrEPort := intstr.FromInt(int(port.Port))
			internalAddress := gamekruiseiov1alpha1.NetworkAddress{
				IP: pod.Status.PodIP,
				Ports: []gamekruiseiov1alpha1.NetworkPort{
					{
						Name:     port.Name,
						Port:     &instrIPort,
						Protocol: port.Protocol,
					},
				},
			}
			externalAddress := gamekruiseiov1alpha1.NetworkAddress{
				EndPoint: endPoints,
				IP:       "",
				Ports: []gamekruiseiov1alpha1.NetworkPort{
					{
						Name:     port.Name,
						Port:     &instrEPort,
						Protocol: port.Protocol,
					},
				},
			}
			internalAddresses = append(internalAddresses, internalAddress)
			externalAddresses = append(externalAddresses, externalAddress)
		}

		networkStatus.InternalAddresses = internalAddresses
		networkStatus.ExternalAddresses = externalAddresses
	}

	networkStatus.CurrentNetworkState = gamekruiseiov1alpha1.NetworkReady
	pod, err = networkManager.UpdateNetworkStatus(*networkStatus, pod)
	return pod, cperrors.ToPluginError(err, cperrors.InternalError)
}

func (m *MultiElbsPlugin) OnPodDeleted(c client.Client, pod *corev1.Pod, ctx context.Context) cperrors.PluginError {
	networkManager := utils.NewNetworkManager(pod, c)
	networkConfig := networkManager.GetNetworkConfig()
	sc, err := parseMultiELBsConfig(networkConfig)
	if err != nil {
		return cperrors.NewPluginError(cperrors.ApiCallError, err.Error())
	}

	var podKeys []string
	if sc.isFixed {
		gss, err := util.GetGameServerSetOfPod(pod, c, ctx)
		if err != nil && !errors.IsNotFound(err) {
			return cperrors.ToPluginError(err, cperrors.ApiCallError)
		}
		// gss exists in cluster, do not deAllocate.
		if err == nil && gss.GetDeletionTimestamp() == nil {
			return nil
		}
		// gss not exists in cluster, deAllocate all the ports related to it.
		for key := range m.podAllocate {
			gssName := pod.GetLabels()[gamekruiseiov1alpha1.GameServerOwnerGssKey]
			if strings.Contains(key, pod.GetNamespace()+"/"+gssName) {
				podKeys = append(podKeys, key)
			}
		}
	} else {
		podKeys = append(podKeys, pod.GetNamespace()+"/"+pod.GetName())
	}

	for _, podKey := range podKeys {
		m.deAllocate(podKey)
	}

	return nil
}

func init() {
	MultiElbsPlugin := MultiElbsPlugin{
		mutex: sync.RWMutex{},
	}
	hwCloudProvider.registerPlugin(&MultiElbsPlugin)
}

type multiELBsConfig struct {
	lbNames                       map[string]string
	idList                        [][]string
	targetPorts                   []int
	protocols                     []corev1.Protocol
	isFixed                       bool
	externalTrafficPolicy         corev1.ServiceExternalTrafficPolicyType
	allocatePolicy                string
	elbClass                      string
	lbHealthCheckFlag             string
	lbHealthCheckConfig           string
	userDefine                    string
	idNums                        int
	allocateLoadBalancerNodePorts bool
}

func (m *MultiElbsPlugin) consSvc(podLbsPorts *lbsPorts, conf *multiELBsConfig, pod *corev1.Pod, lbName string, c client.Client, ctx context.Context) (*corev1.Service, error) {
	var selectId string
	for _, lbId := range podLbsPorts.lbIds {
		if conf.lbNames[lbId] == lbName {
			selectId = lbId
			break
		}
	}
	portProtocolNum := 0
	svcPorts := make([]corev1.ServicePort, 0)
	for i := 0; i < len(podLbsPorts.ports); i++ {
		if podLbsPorts.protocols[i] == ProtocolTCPUDP {
			svcPorts = append(svcPorts, corev1.ServicePort{
				Name:       strconv.Itoa(podLbsPorts.targetPort[i]) + "-" + strings.ToLower(string(corev1.ProtocolTCP)),
				Port:       podLbsPorts.ports[i],
				TargetPort: intstr.FromInt(podLbsPorts.targetPort[i]),
				Protocol:   corev1.ProtocolTCP,
			})
			svcPorts = append(svcPorts, corev1.ServicePort{
				Name:       strconv.Itoa(podLbsPorts.targetPort[i]) + "-" + strings.ToLower(string(corev1.ProtocolUDP)),
				Port:       podLbsPorts.ports[i],
				TargetPort: intstr.FromInt(podLbsPorts.targetPort[i]),
				Protocol:   corev1.ProtocolUDP,
			})
			portProtocolNum += 2
		} else {
			svcPorts = append(svcPorts, corev1.ServicePort{
				Name:       strconv.Itoa(podLbsPorts.targetPort[i]) + "-" + strings.ToLower(string(podLbsPorts.protocols[i])),
				Port:       podLbsPorts.ports[i],
				TargetPort: intstr.FromInt(podLbsPorts.targetPort[i]),
				Protocol:   podLbsPorts.protocols[i],
			})
			portProtocolNum += 1
		}
	}

	log.Infof("%v服务的elb id:%v", pod.GetName()+"-"+strings.ToLower(lbName), selectId)
	svcAnnotations := map[string]string{
		ElbIdAnnotationKey:              selectId,
		ElbConfigHashKey:                util.GetHash(conf),
		ElbHealthCheckFlagAnnotationKey: conf.lbHealthCheckFlag,
	}
	if conf.userDefine != "" {
		hwOptions := make(map[string]string)
		err := json.Unmarshal([]byte(conf.userDefine), &hwOptions)
		if err != nil {
			log.Warningf("[%s] failed to unmarshal userDefine config: %s, err: %v", MultiElbsNetwork, conf.userDefine, err)
		} else {
			log.Infof("[%s] successfully unmarshaled userDefine config: %v", MultiElbsNetwork, hwOptions)
		}
		for k, v := range hwOptions {
			if _, exists := notAllowedAnnotationKeyMap[k]; !exists {
				svcAnnotations[k] = v
				log.Infof("添加注解%v:%v", k, v)
			} else {
				log.Warningf("[%s] not allowed annotation key %s in UserDefine", MultiElbsNetwork, k)
			}
		}
	}

	if conf.lbHealthCheckFlag == "on" && conf.lbHealthCheckConfig != "" {
		processedHealthCheckConfig, err := processHealthCheckOptions(conf.lbHealthCheckConfig, podLbsPorts)
		if err != nil {
			log.Warningf("[%s] failed to process health check options: %v", MultiElbsNetwork, err)
			svcAnnotations[ElbHealthCheckOptionsAnnotationKey] = conf.lbHealthCheckConfig
		} else {
			svcAnnotations[ElbHealthCheckOptionsAnnotationKey] = processedHealthCheckConfig
		}
	}

	svcAnnotations[LBIDBelongIndexKey] = strconv.Itoa(podLbsPorts.index)
	svcAnnotations[ElbMappingPoolAnnotationKey] = lbName
	svcAnnotations[ElbClassAnnotationKey] = conf.elbClass
	svcAnnotations[ElbPortMappingResultCount] = strconv.Itoa(conf.idNums * portProtocolNum)

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        pod.GetName() + "-" + strings.ToLower(lbName),
			Namespace:   pod.GetNamespace(),
			Annotations: svcAnnotations,
			Labels: map[string]string{
				ServiceBelongNetworkTypeKey: MultiElbsNetwork,
			},
			OwnerReferences: getSvcOwnerReference(c, ctx, pod, conf.isFixed),
		},
		Spec: corev1.ServiceSpec{
			AllocateLoadBalancerNodePorts: ptr.To[bool](conf.allocateLoadBalancerNodePorts),
			ExternalTrafficPolicy:         conf.externalTrafficPolicy,
			Type:                          corev1.ServiceTypeLoadBalancer,
			Selector: map[string]string{
				SvcSelectorKey: pod.GetName(),
			},
			Ports: svcPorts,
		},
	}, nil
}

func (m *MultiElbsPlugin) allocate(conf *multiELBsConfig, nsName string) (*lbsPorts, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.podAllocate == nil {
		return nil, cperrors.NewPluginError(cperrors.ApiCallError, "podAllocate is nil")
	}

	// check if pod is already allocated
	if m.podAllocate[nsName] != nil {
		existingLbs := m.podAllocate[nsName]

		// Check if configuration has changed and requires reallocation
		// This happens when new ELBs are added to the configuration
		if existingLbs.index < len(conf.idList) {
			// Check if the allocation matches the new configuration
			existingLbIdsMap := make(map[string]struct{})
			for _, id := range existingLbs.lbIds {
				existingLbIdsMap[id] = struct{}{}
			}

			configLbIdsMap := make(map[string]struct{})
			for _, id := range conf.idList[existingLbs.index] {
				configLbIdsMap[id] = struct{}{}
			}

			// Determine if re-allocation is needed
			needsReallocation := false
			if len(existingLbs.lbIds) != len(conf.idList[existingLbs.index]) {
				// Different number of ELBs - needs reallocation
				needsReallocation = true
			} else {
				// Check if all config ELBs exist in current allocation
				for configLbId := range configLbIdsMap {
					if _, exists := existingLbIdsMap[configLbId]; !exists {
						needsReallocation = true
						break
					}
				}
			}

			if needsReallocation {
				// Deallocate current allocation
				for _, port := range existingLbs.ports {
					m.cache[existingLbs.index][port-m.minPort] = false
				}
				delete(m.podAllocate, nsName)
			} else {
				// Allocation is still valid
				return m.podAllocate[nsName], nil
			}
		} else {
			// Index out of bounds for new configuration - reallocate
			// Deallocate current allocation
			for _, port := range existingLbs.ports {
				m.cache[existingLbs.index][port-m.minPort] = false
			}
			delete(m.podAllocate, nsName)
		}
	}

	// if the pod has not been allocated or was just deallocated due to config change, allocate new ports to it
	var ports []int32
	needNum := len(conf.targetPorts)
	index := -1

	// init cache according to conf.idList
	lenCache := len(m.cache)

	if lenCache > len(conf.idList) {
		m.cache = m.cache[:len(conf.idList)]
	}
	for i := lenCache; i < len(conf.idList); i++ {
		cacheLevel := make([]bool, int(m.maxPort-m.minPort)+1)
		for _, p := range m.blockPorts {
			cacheLevel[int(p-m.minPort)] = true
		}
		m.cache = append(m.cache, cacheLevel)
	}

	// find allocated ports
	switch conf.allocatePolicy {
	case "default":
		for i := 0; i < len(m.cache); i++ {
			sum := 0
			ports = make([]int32, 0)
			for j := 0; j < len(m.cache[i]); j++ {
				if !m.cache[i][j] {
					ports = append(ports, int32(j)+m.minPort)
					sum++
					if sum == needNum {
						index = i
						break
					}
				}
			}
			if index != -1 {
				break
			}
		}
	case "balanced":
		maxAvailable := 0
		for i := 0; i < len(m.cache); i++ {
			sum := 0
			for j := 0; j < len(m.cache[i]); j++ {
				if !m.cache[i][j] {
					sum++
				}
			}
			if sum > maxAvailable {
				maxAvailable = sum
				index = i
			}
		}
		if maxAvailable < needNum {
			return nil, fmt.Errorf("no available ports found")
		}
		ports = make([]int32, 0)
		for j := 0; j < len(m.cache[index]); j++ {
			if !m.cache[index][j] {
				ports = append(ports, int32(j)+m.minPort)
				if len(ports) == needNum {
					break
				}
			}
		}
	}

	if index == -1 {
		return nil, fmt.Errorf("no available ports found")
	}
	if index >= len(conf.idList) {
		return nil, fmt.Errorf("ElbIdNames configuration have not synced")
	}
	for _, port := range ports {
		m.cache[index][port-m.minPort] = true
	}
	m.podAllocate[nsName] = &lbsPorts{
		index:      index,
		lbIds:      conf.idList[index],
		ports:      ports,
		protocols:  conf.protocols,
		targetPort: conf.targetPorts,
	}
	log.Infof("[%s] pod %s allocated: lbIds %v; ports %v", MultiElbsNetwork, nsName, conf.idList[index], ports)
	return m.podAllocate[nsName], nil
}

func processHealthCheckOptions(healthCheckConfig string, podLbsPorts *lbsPorts) (string, error) {
	var healthCheckOptions []HealthCheckOption
	err := json.Unmarshal([]byte(healthCheckConfig), &healthCheckOptions)
	if err != nil {
		return "", fmt.Errorf("failed to unmarshal health check options: %v", err)
	}

	// Update target_service_port based on pod_target_port mapping
	for i := range healthCheckOptions {
		podTargetPortStr := healthCheckOptions[i].PodTargetPort
		var originalPortStr, protocol string

		if podTargetPortStr != "" {
			// Process pod_target_port field
			parts := strings.Split(podTargetPortStr, ":")
			if len(parts) != 2 {
				log.Warningf("Invalid pod_target_port format: %s", podTargetPortStr)
				continue
			}
			protocol = parts[0]
			originalPortStr = parts[1]
		} else if healthCheckOptions[i].TargetServicePort != "" {
			// Process existing target_service_port field as fallback
			targetPortStr := healthCheckOptions[i].TargetServicePort
			parts := strings.Split(targetPortStr, ":")
			if len(parts) != 2 {
				log.Warningf("Invalid target_service_port format: %s", targetPortStr)
				continue
			}
			protocol = parts[0]
			originalPortStr = parts[1]
		} else {
			// No port configuration found
			continue
		}

		// Convert the port part to integer to match with pod ports
		podPort, err := strconv.Atoi(originalPortStr)
		if err != nil {
			log.Warningf("Invalid port number in port specification: %s", originalPortStr)
			continue
		}

		// Look for the corresponding service port based on the pod port and protocol
		found := false

		// First, look for exact pod port and protocol match
		for j, targetPodPort := range podLbsPorts.targetPort {
			if targetPodPort == podPort && j < len(podLbsPorts.protocols) {
				serviceProtocol := strings.ToUpper(string(podLbsPorts.protocols[j]))

				// Handle TCPUDP protocol case
				if serviceProtocol == "TCPUDP" {
					// For TCPUDP, the same service port can handle both TCP and UDP protocols
					servicePort := podLbsPorts.ports[j]
					// Set the target_service_port to use the actual service port
					healthCheckOptions[i].TargetServicePort = fmt.Sprintf("%s:%d", protocol, servicePort)
					// Clear the pod_target_port as it's not needed in the service annotation
					healthCheckOptions[i].PodTargetPort = ""
					found = true
					break
				} else if serviceProtocol == protocol {
					// Exact protocol match
					servicePort := podLbsPorts.ports[j]
					// Set the target_service_port to use the actual service port
					healthCheckOptions[i].TargetServicePort = fmt.Sprintf("%s:%d", protocol, servicePort)
					// Clear the pod_target_port as it's not needed in the service annotation
					healthCheckOptions[i].PodTargetPort = ""
					found = true
					break
				}
			}
		}

		// If no exact match found, try index-based mapping as fallback
		if !found && podPort > 0 && podPort <= len(podLbsPorts.targetPort) {
			// Use the port as 1-based index to select service port
			servicePort := podLbsPorts.ports[podPort-1]
			healthCheckOptions[i].TargetServicePort = fmt.Sprintf("%s:%d", protocol, servicePort)
			// Clear the pod_target_port as it's not needed in the service annotation
			if podTargetPortStr != "" {
				healthCheckOptions[i].PodTargetPort = ""
			}
			found = true
		}

		if !found {
			log.Warningf("Could not find matching service port for port: %s", podTargetPortStr)
		}
	}

	// Marshal the updated health check options
	updatedConfig, err := json.Marshal(healthCheckOptions)
	if err != nil {
		return "", fmt.Errorf("failed to marshal updated health check options: %v", err)
	}

	return string(updatedConfig), nil
}

func (m *MultiElbsPlugin) deAllocate(nsName string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	podLbsPorts := m.podAllocate[nsName]
	if podLbsPorts == nil {
		return
	}
	for _, port := range podLbsPorts.ports {
		m.cache[podLbsPorts.index][port-m.minPort] = false
	}
	delete(m.podAllocate, nsName)

	log.Infof("[%s] pod %s deallocate: lbIds %s ports %v", MultiElbsNetwork, nsName, podLbsPorts.lbIds, podLbsPorts.ports)
}

// HealthCheckOption represents a single health check configuration
type HealthCheckOption struct {
	Protocol          string `json:"protocol,omitempty"`
	Delay             string `json:"delay,omitempty"`
	Timeout           string `json:"timeout,omitempty"`
	MaxRetries        string `json:"max_retries,omitempty"`
	PodTargetPort     string `json:"pod_target_port,omitempty"` // Field from GSS config
	TargetServicePort string `json:"target_service_port"`       // Field to be used in service annotation
	MonitorPort       string `json:"monitor_port,omitempty"`
	Path              string `json:"path,omitempty"`
	ExpectedCodes     string `json:"expected_codes,omitempty"`
}

func parseMultiELBsConfig(conf []gamekruiseiov1alpha1.NetworkConfParams) (*multiELBsConfig, error) {
	// lbNames format {id}: {name}
	var elbHealthCheckConfig, userDefine string
	var idNums int
	lbNames := make(map[string]string)
	idList := make([][]string, 0)
	nameNums := make(map[string]int)
	ports := make([]int, 0)
	protocols := make([]corev1.Protocol, 0)
	isFixed := false
	externalTrafficPolicy := corev1.ServiceExternalTrafficPolicyTypeCluster
	allocatePolicy := "default"
	elbClass := "performance"
	elbHealthCheckFlag := "on"
	allocateLoadBalancerNodePorts := true

	for _, c := range conf {
		switch c.Name {
		case ElbIdNamesConfigName:
			for _, ElbIdNamesConfig := range strings.Split(c.Value, ",") {
				if ElbIdNamesConfig != "" {
					// Parse format: {elb-id-0}/{name-0}
					parts := strings.Split(ElbIdNamesConfig, "/")
					if len(parts) != 2 {
						return nil, fmt.Errorf("invalid ElbIdNames %s. You should input as the format {elb-id-0}/{name-0}", c.Value)
					}

					id := parts[0]
					name := parts[1]

					nameNum := nameNums[name]
					if nameNum >= len(idList) {
						idList = append(idList, []string{id})
					} else {
						idList[nameNum] = append(idList[nameNum], id)
					}
					nameNums[name]++
					lbNames[id] = name
					idNums++
				}
			}
		case PortProtocolsConfigName:
			for _, pp := range strings.Split(c.Value, ",") {
				ppSlice := strings.Split(pp, "/")
				port, err := strconv.Atoi(ppSlice[0])
				if err != nil {
					return nil, fmt.Errorf("invalid PortProtocols %s", c.Value)
				}
				ports = append(ports, port)
				if len(ppSlice) != 2 {
					protocols = append(protocols, corev1.ProtocolTCP)
				} else {
					protocols = append(protocols, corev1.Protocol(ppSlice[1]))
				}
			}
		case FixedConfigName:
			v, err := strconv.ParseBool(c.Value)
			if err != nil {
				return nil, fmt.Errorf("invalid Fixed %s", c.Value)
			}
			isFixed = v
		case ExternalTrafficPolicyTypeConfigName:
			if strings.EqualFold(c.Value, string(corev1.ServiceExternalTrafficPolicyTypeLocal)) {
				externalTrafficPolicy = corev1.ServiceExternalTrafficPolicyTypeLocal
			}
		case AllocateLoadBalancerNodePortsConfigName:
			v, err := strconv.ParseBool(c.Value)
			if err != nil {
				return nil, fmt.Errorf("invalid AllocateLoadBalancerNodePorts %s", c.Value)
			}
			allocateLoadBalancerNodePorts = v
		case AllocatePolicyConfigName:
			allocatePolicy = c.Value
			if allocatePolicy != "default" && allocatePolicy != "balanced" {
				return nil, fmt.Errorf("invalid AllocatePolicy %s", allocatePolicy)
			}
		case ElbClassConfigName:
			elbClass = c.Value
		case ElbHealthCheckFlagConfigName:
			elbHealthCheckFlag = c.Value
		case ElbHealthCheckOptionsConfigName:
			elbHealthCheckConfig = c.Value
		case ElbUserDefineConfigName:
			userDefine = c.Value
		default:
		}
	}

	// check idList
	if len(idList) == 0 {
		return nil, fmt.Errorf("invalid ElbIdNames. You should input as the format {elb-id-0}/{name-0}")
	}
	num := len(idList[0])
	for i := 1; i < len(idList); i++ {
		if num != len(idList[i]) {
			return nil, fmt.Errorf("invalid ElbIdNames. The number of names should be same")
		}
		num = len(idList[i])
	}

	// check ports & protocols
	if len(ports) == 0 || len(protocols) == 0 {
		return nil, fmt.Errorf("invalid PortProtocols, which can not be empty")
	}

	return &multiELBsConfig{
		lbNames:                       lbNames,
		idList:                        idList,
		targetPorts:                   ports,
		protocols:                     protocols,
		isFixed:                       isFixed,
		externalTrafficPolicy:         externalTrafficPolicy,
		allocatePolicy:                allocatePolicy,
		elbClass:                      elbClass,
		lbHealthCheckFlag:             elbHealthCheckFlag,
		lbHealthCheckConfig:           elbHealthCheckConfig,
		userDefine:                    userDefine,
		idNums:                        idNums,
		allocateLoadBalancerNodePorts: allocateLoadBalancerNodePorts,
	}, nil
}
