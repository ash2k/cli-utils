// Copyright 2019 The Kubernetes Authors.
// SPDX-License-Identifier: Apache-2.0

package prune

import (
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/meta/testrestmapper"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/cli-runtime/pkg/resource"
	"k8s.io/client-go/dynamic/fake"
	"k8s.io/kubectl/pkg/scheme"
	"sigs.k8s.io/cli-utils/pkg/apply/event"
	"sigs.k8s.io/cli-utils/pkg/common"
	"sigs.k8s.io/cli-utils/pkg/inventory"
)

var testNamespace = "test-inventory-namespace"
var inventoryObjName = "test-inventory-obj"
var pod1Name = "pod-1"
var pod2Name = "pod-2"
var pod3Name = "pod-3"

var testInventoryLabel = "test-app-label"

var inventoryObj = unstructured.Unstructured{
	Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]interface{}{
			"name":      inventoryObjName,
			"namespace": testNamespace,
			"labels": map[string]interface{}{
				common.InventoryLabel: testInventoryLabel,
			},
		},
	},
}

var pod1 = unstructured.Unstructured{
	Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]interface{}{
			"name":      pod1Name,
			"namespace": testNamespace,
			"uid":       "uid1",
		},
	},
}

var pod1Info = &resource.Info{
	Namespace: testNamespace,
	Name:      pod1Name,
	Object:    &pod1,
}

var pod2 = unstructured.Unstructured{
	Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]interface{}{
			"name":      pod2Name,
			"namespace": testNamespace,
			"uid":       "uid2",
		},
	},
}

var pod2Info = &resource.Info{
	Namespace: testNamespace,
	Name:      pod2Name,
	Object:    &pod2,
}

var pod3 = unstructured.Unstructured{
	Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]interface{}{
			"name":      pod3Name,
			"namespace": testNamespace,
			"uid":       "uid3",
		},
	},
}

var pod3Info = &resource.Info{
	Namespace: testNamespace,
	Name:      pod3Name,
	Object:    &pod3,
}

// Returns a inventory object with the inventory set from
// the passed "children".
func createInventoryInfo(name string, children ...*resource.Info) *resource.Info {
	inventoryName := inventoryObjName
	if len(name) > 0 {
		inventoryName = name
	}
	inventoryObjCopy := inventoryObj.DeepCopy()
	var inventoryInfo = &resource.Info{
		Namespace: testNamespace,
		Name:      inventoryName,
		Object:    inventoryObjCopy,
	}
	infos := []*resource.Info{inventoryInfo}
	infos = append(infos, children...)
	_ = inventory.AddObjsToInventory(infos)
	return inventoryInfo
}

// preventDelete object contains the "on-remove:keep" lifecycle directive.
var preventDelete = unstructured.Unstructured{
	Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]interface{}{
			"name":      "test-prevent-delete",
			"namespace": testNamespace,
			"annotations": map[string]interface{}{
				common.OnRemoveAnnotation: common.OnRemoveKeep,
			},
			"uid": "prevent-delete",
		},
	},
}

var preventDeleteInfo = &resource.Info{
	Namespace: testNamespace,
	Name:      "test-prevent-delete",
	Object:    &preventDelete,
}

func TestPrune(t *testing.T) {
	tests := map[string]struct {
		// pastInfos/currentInfos do NOT contain the inventory object.
		// Inventory object is generated from these past/current objects.
		pastInfos    []*resource.Info
		currentInfos []*resource.Info
		prunedInfos  []*resource.Info
		isError      bool
	}{
		"Past and current objects are empty; no pruned objects": {
			pastInfos:    []*resource.Info{},
			currentInfos: []*resource.Info{},
			prunedInfos:  []*resource.Info{},
			isError:      false,
		},
		"Past and current objects are the same; no pruned objects": {
			pastInfos:    []*resource.Info{pod1Info, pod2Info},
			currentInfos: []*resource.Info{pod2Info, pod1Info},
			prunedInfos:  []*resource.Info{},
			isError:      false,
		},
		"No past objects; no pruned objects": {
			pastInfos:    []*resource.Info{},
			currentInfos: []*resource.Info{pod2Info, pod1Info},
			prunedInfos:  []*resource.Info{},
			isError:      false,
		},
		"No current objects; all previous objects pruned": {
			pastInfos:    []*resource.Info{pod1Info, pod2Info, pod3Info},
			currentInfos: []*resource.Info{},
			prunedInfos:  []*resource.Info{pod1Info, pod2Info, pod3Info},
			isError:      false,
		},
		"Omitted object is pruned": {
			pastInfos:    []*resource.Info{pod1Info, pod2Info},
			currentInfos: []*resource.Info{pod2Info, pod3Info},
			prunedInfos:  []*resource.Info{pod1Info},
			isError:      false,
		},
		"Prevent delete lifecycle annotation stops pruning": {
			pastInfos:    []*resource.Info{preventDeleteInfo, pod2Info},
			currentInfos: []*resource.Info{pod2Info, pod3Info},
			prunedInfos:  []*resource.Info{},
			isError:      false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			po := NewPruneOptions(populateObjectIds(tc.currentInfos, t))
			po.InventoryFactoryFunc = inventory.WrapInventoryObj
			// Set up the previously applied objects.
			pastInventoryInfo := createInventoryInfo("past-group", tc.pastInfos...)
			po.invClient = inventory.NewFakeInventoryClient([]*resource.Info{pastInventoryInfo})
			// Set up the currently applied objects.
			currentInventoryInfo := createInventoryInfo("current-group", tc.currentInfos...)
			currentInfos := append(tc.currentInfos, currentInventoryInfo)
			// The event channel can not block; make sure its bigger than all
			// the events that can be put on it.
			eventChannel := make(chan event.Event, len(tc.pastInfos)+1) // Add one for inventory object
			defer close(eventChannel)
			// Set up the fake dynamic client to recognize all objects, and the RESTMapper.
			po.client = fake.NewSimpleDynamicClient(scheme.Scheme,
				pod1Info.Object, pod2Info.Object, pod3Info.Object)
			po.mapper = testrestmapper.TestOnlyStaticRESTMapper(scheme.Scheme,
				scheme.Scheme.PrioritizedVersionsAllGroups()...)
			// Run the prune and validate.
			err := po.Prune(currentInfos, eventChannel, Options{
				DryRun: true,
			})
			if !tc.isError {
				if err != nil {
					t.Fatalf("Unexpected error during Prune(): %#v", err)
				}
				// Validate the prune events on the event channel.
				expectedPruneEvents := len(tc.prunedInfos) + 1 // One extra for pruning inventory object
				actualPruneEvents := len(eventChannel)
				if expectedPruneEvents != actualPruneEvents {
					t.Errorf("Expected (%d) prune events, got (%d)",
						expectedPruneEvents, actualPruneEvents)
				}
			} else if err == nil {
				t.Fatalf("Expected error during Prune() but received none")
			}
		})
	}
}

// populateObjectIds returns a pointer to a set of strings containing
// the UID's of the passed objects (infos).
func populateObjectIds(infos []*resource.Info, t *testing.T) sets.String {
	uids := sets.NewString()
	for _, currInfo := range infos {
		currObj := currInfo.Object
		metadata, err := meta.Accessor(currObj)
		if err != nil {
			t.Fatalf("Unexpected error retrieving object metadata: %#v", err)
		}
		uid := string(metadata.GetUID())
		uids.Insert(uid)
	}
	return uids
}

func TestPreventDeleteAnnotation(t *testing.T) {
	tests := map[string]struct {
		annotations map[string]string
		expected    bool
	}{
		"Nil map returns false": {
			annotations: nil,
			expected:    false,
		},
		"Empty map returns false": {
			annotations: map[string]string{},
			expected:    false,
		},
		"Wrong annotation key/value is false": {
			annotations: map[string]string{
				"foo": "bar",
			},
			expected: false,
		},
		"Annotation key without value is false": {
			annotations: map[string]string{
				common.OnRemoveAnnotation: "bar",
			},
			expected: false,
		},
		"Annotation key and value is true": {
			annotations: map[string]string{
				common.OnRemoveAnnotation: common.OnRemoveKeep,
			},
			expected: true,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			actual := preventDeleteAnnotation(tc.annotations)
			if tc.expected != actual {
				t.Errorf("preventDeleteAnnotation Expected (%t), got (%t)", tc.expected, actual)
			}
		})
	}
}
