// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
package main

import (
	"flag"
	"io"
	"os"
	"path/filepath"
	"time"

	helmControllerAPIV2 "github.com/fluxcd/helm-controller/api/v2beta1"
	helmControllerV2 "github.com/fluxcd/helm-controller/controllers"
	helper "github.com/fluxcd/pkg/runtime/controller"
	sourcev1 "github.com/fluxcd/source-controller/api/v1beta2"
	"github.com/fluxcd/source-controller/controllers"
	helmSourceController "github.com/fluxcd/source-controller/controllers"
	"github.com/go-logr/logr"
	cmapiv1 "github.com/jetstack/cert-manager/pkg/apis/certmanager/v1"
	"helm.sh/helm/v3/pkg/getter"
	"helm.sh/helm/v3/pkg/registry"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	redpandav1alpha1 "github.com/redpanda-data/redpanda/src/go/k8s/apis/redpanda/v1alpha1"
)

// +kubebuilder:rbac:groups=helm.toolkit.fluxcd.io,resources=helmreleases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=helm.toolkit.fluxcd.io,resources=helmreleases/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=helm.toolkit.fluxcd.io,resources=helmreleases/finalizers,verbs=update
// +kubebuilder:rbac:groups=source.toolkit.fluxcd.io,resources=helmcharts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=source.toolkit.fluxcd.io,resources=helmcharts/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=source.toolkit.fluxcd.io,resources=helmcharts/finalizers,verbs=get;create;update;patch;delete
// +kubebuilder:rbac:groups=source.toolkit.fluxcd.io,resources=helmrepositories,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=source.toolkit.fluxcd.io,resources=helmrepositories/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=source.toolkit.fluxcd.io,resources=helmrepositories/finalizers,verbs=get;create;update;patch;delete

// addtional resources
// +kubebuilder:rbac:groups=source.toolkit.fluxcd.io,resources=buckets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=source.toolkit.fluxcd.io,resources=gitrepositories,verbs=get;list;watch;create;update;patch;delete

// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

const (
	defaultConfiguratorContainerImage = "vectorized/configurator"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
	getters  = getter.Providers{
		getter.Provider{
			Schemes: []string{"http", "https"},
			New:     getter.NewHTTPGetter,
		},
		getter.Provider{
			Schemes: []string{"oci"},
			New:     getter.NewOCIGetter,
		},
	}
)

//nolint:wsl // the init was generated by kubebuilder
func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(redpandav1alpha1.AddToScheme(scheme))
	utilruntime.Must(cmapiv1.AddToScheme(scheme))
	utilruntime.Must(helmControllerAPIV2.AddToScheme(scheme))
	utilruntime.Must(sourcev1.AddToScheme(scheme))
	//+kubebuilder:scaffold:scheme
}

//nolint:funlen // length looks good
func main() {
	var (
		clusterDomain               string
		metricsAddr                 string
		enableLeaderElection        bool
		probeAddr                   string
		webhookEnabled              bool
		configuratorBaseImage       string
		configuratorTag             string
		configuratorImagePullPolicy string
		decommissionWaitInterval    time.Duration
		restrictToRedpandaVersion   string

		// allowPVCDeletion controls the PVC deletion feature in the Cluster custom resource.
		// PVCs will be deleted when its Pod has been deleted and the Node that Pod is assigned to
		// does not exist, or has the NoExecute taint. This is intended to support the rancher.io/local-path
		// storage driver.
		allowPVCDeletion bool
	)

	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.StringVar(&clusterDomain, "cluster-domain", "cluster.local", "Set the Kubernetes local domain (Kubelet's --cluster-domain)")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.BoolVar(&webhookEnabled, "webhook-enabled", false, "Enable webhook Manager")
	flag.StringVar(&configuratorBaseImage, "configurator-base-image", defaultConfiguratorContainerImage, "Set the configurator base image")
	flag.StringVar(&configuratorTag, "configurator-tag", "latest", "Set the configurator tag")
	flag.StringVar(&configuratorImagePullPolicy, "configurator-image-pull-policy", "Always", "Set the configurator image pull policy")
	flag.DurationVar(&decommissionWaitInterval, "decommission-wait-interval", 8*time.Second, "Set the time to wait for a node decommission to happen in the cluster")
	flag.BoolVar(&redpandav1alpha1.AllowDownscalingInWebhook, "allow-downscaling", false, "Allow to reduce the number of replicas in existing clusters (alpha feature)")
	flag.BoolVar(&allowPVCDeletion, "allow-pvc-deletion", false, "Allow the operator to delete PVCs for Pods assigned to failed or missing Nodes (alpha feature)")
	flag.BoolVar(&redpandav1alpha1.AllowConsoleAnyNamespace, "allow-console-any-ns", false, "Allow to create Console in any namespace. Allowing this copies Redpanda SchemaRegistry TLS Secret to namespace (alpha feature)")
	flag.StringVar(&restrictToRedpandaVersion, "restrict-redpanda-version", "", "Restrict management of clusters to those with this version")
	flag.StringVar(&redpandav1alpha1.SuperUsersPrefix, "superusers-prefix", "", "Prefix to add in username of superusers managed by operator. This will only affect new clusters, enabling this will not add prefix to existing clusters (alpha feature)")

	opts := zap.Options{
		Development: true,
	}

	opts.BindFlags(flag.CommandLine)

	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		MetricsBindAddress:     metricsAddr,
		Port:                   9443,
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "aa9fc693.vectorized.io",
	})
	if err != nil {
		setupLog.Error(err, "Unable to start manager")
		os.Exit(1)
	}

	//configurator := resources.ConfiguratorSettings{
	//	ConfiguratorBaseImage: configuratorBaseImage,
	//	ConfiguratorTag:       configuratorTag,
	//	ImagePullPolicy:       corev1.PullPolicy(configuratorImagePullPolicy),
	//}

	//if err = (&redpandacontrollers.ClusterReconciler{
	//	Client:                    mgr.GetClient(),
	//	Log:                       ctrl.Log.WithName("controllers").WithName("redpanda").WithName("Cluster"),
	//	Scheme:                    mgr.GetScheme(),
	//	AdminAPIClientFactory:     adminutils.NewInternalAdminAPI,
	//	DecommissionWaitInterval:  decommissionWaitInterval,
	//	RestrictToRedpandaVersion: restrictToRedpandaVersion,
	//}).WithClusterDomain(clusterDomain).WithConfiguratorSettings(configurator).WithAllowPVCDeletion(allowPVCDeletion).SetupWithManager(mgr); err != nil {
	//	setupLog.Error(err, "Unable to create controller", "controller", "Cluster")
	//	os.Exit(1)
	//}

	//if err = (&redpandacontrollers.ClusterConfigurationDriftReconciler{
	//	Client:                    mgr.GetClient(),
	//	Log:                       ctrl.Log.WithName("controllers").WithName("redpanda").WithName("ClusterConfigurationDrift"),
	//	Scheme:                    mgr.GetScheme(),
	//	AdminAPIClientFactory:     adminutils.NewInternalAdminAPI,
	//	RestrictToRedpandaVersion: restrictToRedpandaVersion,
	//}).WithClusterDomain(clusterDomain).SetupWithManager(mgr); err != nil {
	//	setupLog.Error(err, "Unable to create controller", "controller", "ClusterConfigurationDrift")
	//	os.Exit(1)
	//}
	//
	//if err = redpandacontrollers.NewClusterMetricsController(mgr.GetClient()).
	//	SetupWithManager(mgr); err != nil {
	//	setupLog.Error(err, "Unable to create controller", "controller", "ClustersMetrics")
	//	os.Exit(1)
	//}

	// Setup webhooks
	//if webhookEnabled {
	//	setupLog.Info("Setup webhook")
	//	if err = (&redpandav1alpha1.Cluster{}).SetupWebhookWithManager(mgr); err != nil {
	//		setupLog.Error(err, "Unable to create webhook", "webhook", "RedpandaCluster")
	//		os.Exit(1)
	//	}
	//	hookServer := mgr.GetWebhookServer()
	//	hookServer.Register("/mutate-redpanda-vectorized-io-v1alpha1-console", &webhook.Admission{Handler: &redpandawebhooks.ConsoleDefaulter{Client: mgr.GetClient()}})
	//	hookServer.Register("/validate-redpanda-vectorized-io-v1alpha1-console", &webhook.Admission{Handler: &redpandawebhooks.ConsoleValidator{Client: mgr.GetClient()}})
	//}

	//if err = (&redpandacontrollers.ConsoleReconciler{
	//	Client:                  mgr.GetClient(),
	//	Scheme:                  mgr.GetScheme(),
	//	Log:                     ctrl.Log.WithName("controllers").WithName("redpanda").WithName("Console"),
	//	AdminAPIClientFactory:   adminutils.NewInternalAdminAPI,
	//	Store:                   consolepkg.NewStore(mgr.GetClient(), mgr.GetScheme()),
	//	EventRecorder:           mgr.GetEventRecorderFor("Console"),
	//	KafkaAdminClientFactory: consolepkg.NewKafkaAdmin,
	//}).WithClusterDomain(clusterDomain).SetupWithManager(mgr); err != nil {
	//	setupLog.Error(err, "unable to create controller", "controller", "Console")
	//	os.Exit(1)
	//}

	storage := mustInitStorage("", "", 60*time.Second, 2, setupLog)

	metricsH := helper.MustMakeMetrics(mgr)

	// TODO fill this in with options
	helmOpts := helmControllerV2.HelmReleaseReconcilerOptions{
		MaxConcurrentReconciles:   1,                // "The number of concurrent HelmRelease reconciles."
		DependencyRequeueInterval: 30 * time.Second, // The interval at which failing dependencies are reevaluated.
		HTTPRetry:                 9,                // The maximum number of retries when failing to fetch artifacts over HTTP.
		RateLimiter:               workqueue.NewItemExponentialFailureRateLimiter(30*time.Second, 60*time.Second),
	}

	// Helm Release Controller
	helmRelease := helmControllerV2.HelmReleaseReconciler{
		Client:        mgr.GetClient(),
		Config:        mgr.GetConfig(),
		Scheme:        mgr.GetScheme(),
		EventRecorder: mgr.GetEventRecorderFor("HelmReleaseReconciler"),
	}
	if err = helmRelease.SetupWithManager(mgr, helmOpts); err != nil {
		setupLog.Error(err, "Unable to create controller", "controller", "HelmRelease")
	}

	// Helm Release Chart Controller
	helmChart := helmSourceController.HelmChartReconciler{
		Client:                  mgr.GetClient(),
		RegistryClientGenerator: clientGenerator,
		Getters:                 getters,
		Metrics:                 metricsH,
		Storage:                 storage,
	}
	if err = helmChart.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Unable to create controller", "controller", "HelmChart")
	}

	helmRepository := helmSourceController.HelmRepositoryReconciler{
		Client:         mgr.GetClient(),
		EventRecorder:  mgr.GetEventRecorderFor("HelmRepositoryReconciler"),
		Getters:        getters,
		ControllerName: "redpanda-controller",
		TTL:            15 * time.Minute,
		Metrics:        metricsH,
		Storage:        storage,
	}
	if err = helmRepository.SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Unable to create controller", "controller", "HelmRepository")
	}

	// if err = (&redpandacontrollers.RedpandaReconciler{
	// 	Client: mgr.GetClient(),
	// 	Scheme: mgr.GetScheme(),
	// }).SetupWithManager(mgr, helmOpts); err != nil {
	// 	setupLog.Error(err, "unable to create controller", "controller", "Redpanda")
	// 	os.Exit(1)
	// }
	//+kubebuilder:scaffold:builder

	if err := mgr.AddHealthzCheck("health", healthz.Ping); err != nil {
		setupLog.Error(err, "Unable to set up health check")
		os.Exit(1)
	}

	if err := mgr.AddReadyzCheck("check", healthz.Ping); err != nil {
		setupLog.Error(err, "Unable to set up ready check")
		os.Exit(1)
	}

	//if webhookEnabled {
	//	hookServer := mgr.GetWebhookServer()
	//	if err := mgr.AddReadyzCheck("webhook", hookServer.StartedChecker()); err != nil {
	//		setupLog.Error(err, "unable to create ready check")
	//		os.Exit(1)
	//	}
	//
	//	if err := mgr.AddHealthzCheck("webhook", hookServer.StartedChecker()); err != nil {
	//		setupLog.Error(err, "unable to create health check")
	//		os.Exit(1)
	//	}
	//}
	setupLog.Info("Starting manager")

	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "Problem running manager")
		os.Exit(1)
	}
}

func clientGenerator(isLogin bool) (*registry.Client, string, error) {
	if isLogin {
		// create a temporary file to store the credentials
		// this is needed because otherwise the credentials are stored in ~/.docker/config.json.
		credentialsFile, err := os.CreateTemp("", "credentials")
		if err != nil {
			return nil, "", err
		}

		var errs []error
		rClient, err := registry.NewClient(registry.ClientOptWriter(io.Discard), registry.ClientOptCredentialsFile(credentialsFile.Name()))
		if err != nil {
			errs = append(errs, err)
			// attempt to delete the temporary file
			if credentialsFile != nil {
				err := os.Remove(credentialsFile.Name())
				if err != nil {
					errs = append(errs, err)
				}
			}
			return nil, "", errors.NewAggregate(errs)
		}
		return rClient, credentialsFile.Name(), nil
	}

	rClient, err := registry.NewClient(registry.ClientOptWriter(io.Discard))
	if err != nil {
		return nil, "", err
	}
	return rClient, "", nil
}

func mustInitStorage(path string, storageAdvAddr string, artifactRetentionTTL time.Duration, artifactRetentionRecords int, l logr.Logger) *controllers.Storage {
	if path == "" {
		p, _ := os.Getwd()
		path = filepath.Join(p, "bin")
		os.MkdirAll(path, 0o700)
	}

	storage, err := controllers.NewStorage(path, storageAdvAddr, artifactRetentionTTL, artifactRetentionRecords)
	if err != nil {
		l.Error(err, "unable to initialise storage")
		os.Exit(1)
	}

	return storage
}
