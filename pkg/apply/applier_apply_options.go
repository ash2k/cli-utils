// Copyright 2021 The Kubernetes Authors.
// SPDX-License-Identifier: Apache-2.0

package apply

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/cli-utils/pkg/apply/event"
	"sigs.k8s.io/cli-utils/pkg/common"
	"sigs.k8s.io/cli-utils/pkg/inventory"
)

const defaultPollInterval = 2 * time.Second

type Options struct {
	// Encapsulates the fields for server-side apply.
	serverSideOptions common.ServerSideOptions

	// reconcileTimeout defines whether the applier should wait
	// until all applied resources have been reconciled, and if so,
	// how long to wait.
	reconcileTimeout time.Duration

	// pollInterval defines how often we should poll for the status
	// of resources.
	pollInterval time.Duration

	// emitStatusEvents defines whether status events should be
	// emitted on the eventChannel to the caller.
	emitStatusEvents bool

	// prune defines whether pruning of previously applied
	// objects should happen after apply.
	prune bool

	// dryRunStrategy defines whether changes should actually be performed,
	// or if it is just talk and no action.
	dryRunStrategy common.DryRunStrategy

	// prunePropagationPolicy defines the deletion propagation policy
	// that should be used for pruning. If this is not provided, the
	// default is to use the Background policy.
	prunePropagationPolicy metav1.DeletionPropagation

	// pruneTimeout defines whether we should wait for all resources
	// to be fully deleted after pruning, and if so, how long we should
	// wait.
	pruneTimeout time.Duration

	// inventoryPolicy defines the inventory policy of apply.
	inventoryPolicy inventory.InventoryPolicy

	// eventListeners is a list of event listeners that are called in specified order when an event happens.
	// Listeners are never called concurrently with each other.
	eventListeners []func(event.Event)
}

func NewOptions() *Options {
	return &Options{
		pollInterval:           defaultPollInterval,
		prune:                  true,
		prunePropagationPolicy: metav1.DeletePropagationBackground,
	}
}

func (o *Options) ServerSideOptions(serverSideOptions common.ServerSideOptions) *Options {
	o.serverSideOptions = serverSideOptions
	return o
}

func (o *Options) ReconcileTimeout(reconcileTimeout time.Duration) *Options {
	o.reconcileTimeout = reconcileTimeout
	return o
}

func (o *Options) PollInterval(pollInterval time.Duration) *Options {
	o.pollInterval = pollInterval
	return o
}

func (o *Options) EmitStatusEvents(emitStatusEvents bool) *Options {
	o.emitStatusEvents = emitStatusEvents
	return o
}

func (o *Options) Prune(prune bool) *Options {
	o.prune = prune
	return o
}

func (o *Options) DryRunStrategy(dryRunStrategy common.DryRunStrategy) *Options {
	o.dryRunStrategy = dryRunStrategy
	return o
}

func (o *Options) PrunePropagationPolicy(prunePropagationPolicy metav1.DeletionPropagation) *Options {
	o.prunePropagationPolicy = prunePropagationPolicy
	return o
}

func (o *Options) PruneTimeout(pruneTimeout time.Duration) *Options {
	o.pruneTimeout = pruneTimeout
	return o
}

func (o *Options) InventoryPolicy(inventoryPolicy inventory.InventoryPolicy) *Options {
	o.inventoryPolicy = inventoryPolicy
	return o
}

func (o *Options) EventListener(eventListener ...func(event.Event)) *Options {
	o.eventListeners = append(o.eventListeners, eventListener...)
	return o
}

// EventChannelListener is a convenience helper that attaches a listener that pushes events into a channel.
func (o *Options) EventChannelListener(eventChannel chan<- event.Event) *Options {
	return o.EventListener(func(e event.Event) {
		eventChannel <- e
	})
}

// CollectEventsInto is a convenience helper that attaches a listener that appends events to the eventSink slice.
func (o *Options) CollectEventsInto(eventSink *[]event.Event) *Options {
	return o.EventListener(func(e event.Event) {
		*eventSink = append(*eventSink, e)
	})
}

// CollectErrorInto is a convenience helper that captures error from the first event of ErrorType type.
func (o *Options) CollectErrorInto(errSink *error) *Options {
	assigned := false
	return o.EventListener(func(e event.Event) {
		if e.Type == event.ErrorType && !assigned {
			assigned = true
			*errSink = e.ErrorEvent.Err
		}
	})
}
