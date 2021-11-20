// Copyright 2019 The Kubernetes Authors.
// SPDX-License-Identifier: Apache-2.0

package apply

import (
	"context"
	"fmt"
	"sort"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/klog/v2"
	"sigs.k8s.io/cli-utils/pkg/apply/cache"
	"sigs.k8s.io/cli-utils/pkg/apply/event"
	"sigs.k8s.io/cli-utils/pkg/apply/filter"
	"sigs.k8s.io/cli-utils/pkg/apply/info"
	"sigs.k8s.io/cli-utils/pkg/apply/mutator"
	"sigs.k8s.io/cli-utils/pkg/apply/poller"
	"sigs.k8s.io/cli-utils/pkg/apply/prune"
	"sigs.k8s.io/cli-utils/pkg/apply/solver"
	"sigs.k8s.io/cli-utils/pkg/apply/taskrunner"
	"sigs.k8s.io/cli-utils/pkg/common"
	"sigs.k8s.io/cli-utils/pkg/inventory"
	"sigs.k8s.io/cli-utils/pkg/object"
	"sigs.k8s.io/cli-utils/pkg/ordering"
)

// Applier performs the step of applying a set of resources into a cluster,
// conditionally waits for all of them to be fully reconciled and finally
// performs prune to clean up any resources that has been deleted.
// The applier performs its function by executing a list queue of tasks,
// each of which is one of the steps in the process of applying a set
// of resources to the cluster. The actual execution of these tasks are
// handled by a StatusRunner. So the taskqueue is effectively a
// specification that is executed by the StatusRunner. Based on input
// parameters and/or the set of resources that needs to be applied to the
// cluster, different sets of tasks might be needed.
type Applier struct {
	pruner        *prune.Pruner
	statusPoller  poller.Poller
	invClient     inventory.InventoryClient
	client        dynamic.Interface
	openAPIGetter discovery.OpenAPISchemaInterface
	mapper        meta.RESTMapper
	infoHelper    info.InfoHelper
}

// prepareObjects returns the set of objects to apply and to prune or
// an error if one occurred.
func (a *Applier) prepareObjects(localInv inventory.InventoryInfo, localObjs object.UnstructuredSet,
	o *Options) (object.UnstructuredSet, object.UnstructuredSet, error) {
	if localInv == nil {
		return nil, nil, fmt.Errorf("the local inventory can't be nil")
	}
	if err := inventory.ValidateNoInventory(localObjs); err != nil {
		return nil, nil, err
	}
	// Add the inventory annotation to the resources being applied.
	for _, localObj := range localObjs {
		inventory.AddInventoryIDAnnotation(localObj, localInv)
	}
	// If the inventory uses the Name strategy and an inventory ID is provided,
	// verify that the existing inventory object (if there is one) has an ID
	// label that matches.
	// TODO(seans): This inventory id validation should happen in destroy and status.
	if localInv.Strategy() == inventory.NameStrategy && localInv.ID() != "" {
		prevInvObjs, err := a.invClient.GetClusterInventoryObjs(localInv)
		if err != nil {
			return nil, nil, err
		}
		if len(prevInvObjs) > 1 {
			panic(fmt.Errorf("found %d inv objects with Name strategy", len(prevInvObjs)))
		}
		if len(prevInvObjs) == 1 {
			invObj := prevInvObjs[0]
			val := invObj.GetLabels()[common.InventoryLabel]
			if val != localInv.ID() {
				return nil, nil, fmt.Errorf("inventory-id of inventory object in cluster doesn't match provided id %q", localInv.ID())
			}
		}
	}
	pruneObjs, err := a.pruner.GetPruneObjs(localInv, localObjs, prune.Options{
		DryRunStrategy: o.dryRunStrategy,
	})
	if err != nil {
		return nil, nil, err
	}
	sort.Sort(ordering.SortableUnstructureds(localObjs))
	return localObjs, pruneObjs, nil
}

// Apply performs the apply step. This happens synchronously with updates
// on progress and any errors reported back via callbacks.
// Cancelling the operation or setting timeout on how long to Wait
// for it complete can be done with the passed in context.
// Note: There isn't currently any way to interrupt the operation
// before all the given resources have been applied to the cluster. Any
// cancellation or timeout will only affect how long we Wait for the
// resources to become current.
func (a *Applier) Apply(ctx context.Context, invInfo inventory.InventoryInfo, objects object.UnstructuredSet, options *Options) {
	klog.V(4).Infof("apply run for %d objects", len(objects))
	// Validate the resources to make sure we catch those problems early
	// before anything has been updated in the cluster.
	if err := (&object.Validator{
		Mapper: a.mapper,
	}).Validate(objects); err != nil {
		postErrorEvent(options, err)
		return
	}

	applyObjs, pruneObjs, err := a.prepareObjects(invInfo, objects, options)
	if err != nil {
		postErrorEvent(options, err)
		return
	}
	klog.V(4).Infof("calculated %d apply objs; %d prune objs", len(applyObjs), len(pruneObjs))

	// Fetch the queue (channel) of tasks that should be executed.
	klog.V(4).Infoln("applier building task queue...")
	taskBuilder := &solver.TaskQueueBuilder{
		Pruner:        a.pruner,
		DynamicClient: a.client,
		OpenAPIGetter: a.openAPIGetter,
		InfoHelper:    a.infoHelper,
		Mapper:        a.mapper,
		InvClient:     a.invClient,
		Destroy:       false,
	}
	solverOpts := solver.Options{
		ServerSideOptions:      options.serverSideOptions,
		ReconcileTimeout:       options.reconcileTimeout,
		Prune:                  options.prune,
		DryRunStrategy:         options.dryRunStrategy,
		PrunePropagationPolicy: options.prunePropagationPolicy,
		PruneTimeout:           options.pruneTimeout,
		InventoryPolicy:        options.inventoryPolicy,
	}
	// Build list of apply validation filters.
	var applyFilters []filter.ValidationFilter
	if options.inventoryPolicy != inventory.AdoptAll {
		applyFilters = append(applyFilters, filter.InventoryPolicyApplyFilter{
			Client:    a.client,
			Mapper:    a.mapper,
			Inv:       invInfo,
			InvPolicy: options.inventoryPolicy,
		})
	}

	// Build list of prune validation filters.
	pruneFilters := []filter.ValidationFilter{
		filter.PreventRemoveFilter{},
		filter.InventoryPolicyFilter{
			Inv:       invInfo,
			InvPolicy: options.inventoryPolicy,
		},
		filter.LocalNamespacesFilter{
			LocalNamespaces: localNamespaces(invInfo, object.UnstructuredsToObjMetasOrDie(objects)),
		},
	}
	// Build list of apply mutators.
	// Share a thread-safe cache with the status poller.
	resourceCache := cache.NewResourceCacheMap()
	applyMutators := []mutator.Interface{
		&mutator.ApplyTimeMutator{
			Client:        a.client,
			Mapper:        a.mapper,
			ResourceCache: resourceCache,
		},
	}
	// Build the task queue by appending tasks in the proper order.
	taskQueue, err := taskBuilder.
		AppendInvAddTask(invInfo, applyObjs, options.dryRunStrategy).
		AppendApplyWaitTasks(applyObjs, applyFilters, applyMutators, solverOpts).
		AppendPruneWaitTasks(pruneObjs, pruneFilters, solverOpts).
		AppendInvSetTask(invInfo, options.dryRunStrategy).
		Build()
	if err != nil {
		postErrorEvent(options, err)
		return
	}
	// Send event to inform the caller about the resources that
	// will be applied/pruned.
	postEvent(options, event.Event{
		Type: event.InitType,
		InitEvent: event.InitEvent{
			ActionGroups: taskQueue.ToActionGroups(),
		},
	})
	// Create a new TaskStatusRunner to execute the taskQueue.
	klog.V(4).Infoln("applier building TaskStatusRunner...")
	allIds := object.UnstructuredsToObjMetasOrDie(append(applyObjs, pruneObjs...))
	runner := taskrunner.NewTaskStatusRunner(allIds, a.statusPoller, resourceCache)
	klog.V(4).Infoln("applier running TaskStatusRunner...")
	eventChannel := make(chan event.Event)
	go func() {
		defer close(eventChannel)
		err = runner.Run(ctx, taskQueue.ToChannel(), eventChannel, taskrunner.Options{
			PollInterval:     options.pollInterval,
			UseCache:         true,
			EmitStatusEvents: options.emitStatusEvents,
		})
	}()
	for e := range eventChannel {
		postEvent(options, e)
	}
	if err != nil {
		postErrorEvent(options, err)
		return
	}
}

func postErrorEvent(cfg *Options, err error) {
	postEvent(cfg, event.Event{
		Type: event.ErrorType,
		ErrorEvent: event.ErrorEvent{
			Err: err,
		},
	})
}

func postEvent(cfg *Options, event event.Event) {
	for _, listener := range cfg.eventListeners {
		listener(event)
	}
}

// localNamespaces stores a set of strings of all the namespaces
// for the passed non cluster-scoped localObjs, plus the namespace
// of the passed inventory object. This is used to skip deleting
// namespaces which have currently applied objects in them.
func localNamespaces(localInv inventory.InventoryInfo, localObjs []object.ObjMetadata) sets.String {
	namespaces := sets.NewString()
	for _, obj := range localObjs {
		if obj.Namespace != "" {
			namespaces.Insert(obj.Namespace)
		}
	}
	invNamespace := localInv.Namespace()
	if invNamespace != "" {
		namespaces.Insert(invNamespace)
	}
	return namespaces
}
