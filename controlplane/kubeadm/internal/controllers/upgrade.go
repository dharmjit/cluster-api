/*
Copyright 2020 The Kubernetes Authors.

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

package controllers

import (
	"context"
	"encoding/json"

	"github.com/blang/semver/v4"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	"k8s.io/utils/pointer"
	ctrl "sigs.k8s.io/controller-runtime"

	clusterv1 "sigs.k8s.io/cluster-api/api/v1beta1"
	controlplanev1 "sigs.k8s.io/cluster-api/controlplane/kubeadm/api/v1beta1"
	"sigs.k8s.io/cluster-api/controlplane/kubeadm/internal"
	"sigs.k8s.io/cluster-api/util"
	"sigs.k8s.io/cluster-api/util/collections"
	"sigs.k8s.io/cluster-api/util/patch"
	"sigs.k8s.io/cluster-api/util/version"
)

func (r *KubeadmControlPlaneReconciler) upgradeControlPlane(
	ctx context.Context,
	controlPlane *internal.ControlPlane,
	machinesRequireUpgrade collections.Machines,
) (ctrl.Result, error) {
	logger := ctrl.LoggerFrom(ctx)

	if controlPlane.KCP.Spec.RolloutStrategy == nil || (controlPlane.KCP.Spec.RolloutStrategy.Type != controlplanev1.RollingUpdateStrategyType && controlPlane.KCP.Spec.RolloutStrategy.Type != controlplanev1.InplaceUpdateStrategyType) {
		return ctrl.Result{}, errors.New("rolloutStrategy is not set")
	}

	// TODO: handle reconciliation of etcd members and kubeadm config in case they get out of sync with cluster

	workloadCluster, err := controlPlane.GetWorkloadCluster(ctx)
	if err != nil {
		logger.Error(err, "failed to get remote client for workload cluster", "cluster key", util.ObjectKey(controlPlane.Cluster))
		return ctrl.Result{}, err
	}

	parsedVersion, err := semver.ParseTolerant(controlPlane.KCP.Spec.Version)
	if err != nil {
		return ctrl.Result{}, errors.Wrapf(err, "failed to parse kubernetes version %q", controlPlane.KCP.Spec.Version)
	}

	if err := workloadCluster.ReconcileKubeletRBACRole(ctx, parsedVersion); err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to reconcile the remote kubelet RBAC role")
	}

	if err := workloadCluster.ReconcileKubeletRBACBinding(ctx, parsedVersion); err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to reconcile the remote kubelet RBAC binding")
	}

	// Ensure kubeadm cluster role  & bindings for v1.18+
	// as per https://github.com/kubernetes/kubernetes/commit/b117a928a6c3f650931bdac02a41fca6680548c4
	if err := workloadCluster.AllowBootstrapTokensToGetNodes(ctx); err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to set role and role binding for kubeadm")
	}

	if err := workloadCluster.UpdateKubernetesVersionInKubeadmConfigMap(ctx, parsedVersion); err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to update the kubernetes version in the kubeadm config map")
	}

	if controlPlane.KCP.Spec.KubeadmConfigSpec.ClusterConfiguration != nil {
		// We intentionally only parse major/minor/patch so that the subsequent code
		// also already applies to beta versions of new releases.
		parsedVersionTolerant, err := version.ParseMajorMinorPatchTolerant(controlPlane.KCP.Spec.Version)
		if err != nil {
			return ctrl.Result{}, errors.Wrapf(err, "failed to parse kubernetes version %q", controlPlane.KCP.Spec.Version)
		}
		// Get the imageRepository or the correct value if nothing is set and a migration is necessary.
		imageRepository := internal.ImageRepositoryFromClusterConfig(controlPlane.KCP.Spec.KubeadmConfigSpec.ClusterConfiguration, parsedVersionTolerant)

		if err := workloadCluster.UpdateImageRepositoryInKubeadmConfigMap(ctx, imageRepository, parsedVersion); err != nil {
			return ctrl.Result{}, errors.Wrap(err, "failed to update the image repository in the kubeadm config map")
		}
	}

	if controlPlane.KCP.Spec.KubeadmConfigSpec.ClusterConfiguration != nil && controlPlane.KCP.Spec.KubeadmConfigSpec.ClusterConfiguration.Etcd.Local != nil {
		meta := controlPlane.KCP.Spec.KubeadmConfigSpec.ClusterConfiguration.Etcd.Local.ImageMeta
		if err := workloadCluster.UpdateEtcdVersionInKubeadmConfigMap(ctx, meta.ImageRepository, meta.ImageTag, parsedVersion); err != nil {
			return ctrl.Result{}, errors.Wrap(err, "failed to update the etcd version in the kubeadm config map")
		}

		extraArgs := controlPlane.KCP.Spec.KubeadmConfigSpec.ClusterConfiguration.Etcd.Local.ExtraArgs
		if err := workloadCluster.UpdateEtcdExtraArgsInKubeadmConfigMap(ctx, extraArgs, parsedVersion); err != nil {
			return ctrl.Result{}, errors.Wrap(err, "failed to update the etcd extra args in the kubeadm config map")
		}
	}

	if controlPlane.KCP.Spec.KubeadmConfigSpec.ClusterConfiguration != nil {
		if err := workloadCluster.UpdateAPIServerInKubeadmConfigMap(ctx, controlPlane.KCP.Spec.KubeadmConfigSpec.ClusterConfiguration.APIServer, parsedVersion); err != nil {
			return ctrl.Result{}, errors.Wrap(err, "failed to update api server in the kubeadm config map")
		}

		if err := workloadCluster.UpdateControllerManagerInKubeadmConfigMap(ctx, controlPlane.KCP.Spec.KubeadmConfigSpec.ClusterConfiguration.ControllerManager, parsedVersion); err != nil {
			return ctrl.Result{}, errors.Wrap(err, "failed to update controller manager in the kubeadm config map")
		}

		if err := workloadCluster.UpdateSchedulerInKubeadmConfigMap(ctx, controlPlane.KCP.Spec.KubeadmConfigSpec.ClusterConfiguration.Scheduler, parsedVersion); err != nil {
			return ctrl.Result{}, errors.Wrap(err, "failed to update scheduler in the kubeadm config map")
		}
	}

	if err := workloadCluster.UpdateKubeletConfigMap(ctx, parsedVersion); err != nil {
		return ctrl.Result{}, errors.Wrap(err, "failed to upgrade kubelet config map")
	}

	switch controlPlane.KCP.Spec.RolloutStrategy.Type {
	case controlplanev1.RollingUpdateStrategyType:
		// RolloutStrategy is currently defaulted and validated to be RollingUpdate
		// We can ignore MaxUnavailable because we are enforcing health checks before we get here.
		maxNodes := *controlPlane.KCP.Spec.Replicas + int32(controlPlane.KCP.Spec.RolloutStrategy.RollingUpdate.MaxSurge.IntValue())
		if int32(controlPlane.Machines.Len()) < maxNodes {
			// scaleUp ensures that we don't continue scaling up while waiting for Machines to have NodeRefs
			return r.scaleUpControlPlane(ctx, controlPlane)
		}
		return r.scaleDownControlPlane(ctx, controlPlane, machinesRequireUpgrade)
	case controlplanev1.InplaceUpdateStrategyType:
		return r.inplaceUpgradeControlPlane(ctx, controlPlane, machinesRequireUpgrade)
	default:
		logger.Info("RolloutStrategy type is not set to RollingUpdateStrategyType, unable to determine the strategy for rolling out machines")
		return ctrl.Result{}, nil
	}
}

func (r *KubeadmControlPlaneReconciler) inplaceUpgradeControlPlane(ctx context.Context, controlPlane *internal.ControlPlane, machines collections.Machines) (ctrl.Result, error) {
	logger := ctrl.LoggerFrom(ctx)

	// Run preflight checks to ensure that the control plane is stable before proceeding with an in-place upgrade operation; if not, wait.
	if result, err := r.preflightChecks(ctx, controlPlane); err != nil || !result.IsZero() {
		return result, err
	}

	// if controlPlane.KCP.Spec.MachineTemplate.ObjectMeta.Annotations == nil {
	// 	controlPlane.KCP.Spec.MachineTemplate.ObjectMeta.Annotations = make(map[string]string)
	// }
	// _, ok := controlPlane.KCP.Spec.MachineTemplate.ObjectMeta.Annotations["controlplane.cluster.x-k8s.io/inplace-upgrade"]
	// if !ok {
	// 	controlPlane.KCP.Spec.MachineTemplate.ObjectMeta.Annotations["controlplane.cluster.x-k8s.io/inplace-upgrade"] = ""
	// }

	if err := r.inplaceUpdateMachine(ctx, controlPlane.KCP, machines.Oldest()); err != nil {
		logger.Error(err, "Failed to update control plane Machine")
		r.recorder.Eventf(controlPlane.KCP, corev1.EventTypeWarning, "FailedInplaceUpdate", "Failed to inplace Update control plane Machine for cluster % control plane: %v", klog.KObj(controlPlane.Cluster), err)
		return ctrl.Result{}, err
	}
	// Requeue the control plane, in case there are other operations to perform
	return ctrl.Result{Requeue: true}, nil
}

func (r *KubeadmControlPlaneReconciler) inplaceUpdateMachine(ctx context.Context, kcp *controlplanev1.KubeadmControlPlane, machine *clusterv1.Machine) (reterr error) {
	// original := client.MergeFrom(machine.DeepCopy())
	patchHelper, err := patch.NewHelper(machine, r.Client)
	if err != nil {
		return errors.Wrap(err, "failed to create patch helper for the machine")
	}
	defer func() {
		ctrl.LoggerFrom(ctx).Info("In-place Upgrading", "Machine", machine)
		if err := patchHelper.Patch(ctx, machine); err != nil {
			reterr = errors.Wrap(err, "failed to patch the machine")
		}
	}()

	machine.Spec.Upgrade.Run = pointer.Bool(true)
	upgradeStartedAt := v1.Now()
	machine.Spec.Upgrade.StartedAt = &upgradeStartedAt
	if kcp.Spec.Version != *machine.Spec.Version {
		machine.Spec.Version = &kcp.Spec.Version
	}
	clusterConfig, err := json.Marshal(kcp.Spec.KubeadmConfigSpec.ClusterConfiguration)
	if err != nil {
		return errors.Wrap(err, "failed to marshal cluster configuration")
	}
	machine.ObjectMeta.Annotations[controlplanev1.KubeadmClusterConfigurationAnnotation] = string(clusterConfig)
	// 	if err := r.Client.Patch(ctx, updatedMachine, original); err != nil {
	// 		// if err := patchHelper.Patch(ctx, machine); err != nil {
	// 		reterr = errors.Wrap(err, "failed to patch the machine")
	// 	}
	// }
	return
}
