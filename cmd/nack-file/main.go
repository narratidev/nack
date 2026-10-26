// nack-file runs NACK's control-loop reconcilers without NACK's CRDs:
// Accounts, Streams, Consumers, KeyValues and ObjectStores are read from a
// directory (a mounted ConfigMap) and kept in memory. Only Secrets come from
// the Kubernetes API, in the controller's own namespace: the creds and CA an
// Account names. A namespaced Role that can read Secrets is all it needs.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/nats-io/nack/internal/configsource"
	"github.com/nats-io/nack/internal/controller"
	api "github.com/nats-io/nack/pkg/jetstream/apis/jetstream/v1beta2"
)

func main() {
	klog.InitFlags(nil)
	var dir, namespace, server, creds, ca, cacheDir, probeAddr string
	var readOnly bool
	var poll, resync, retry time.Duration
	flag.StringVar(&dir, "config-dir", "/etc/nats-config", "Directory with the resources (a mounted ConfigMap).")
	flag.StringVar(&namespace, "namespace", os.Getenv("POD_NAMESPACE"), "The namespace: resources land in it and Secrets are read from it.")
	flag.StringVar(&server, "s", "", "NATS server URL, for resources without an Account.")
	flag.StringVar(&creds, "creds", "", "NATS credentials file, for resources without an Account.")
	flag.StringVar(&ca, "tlsca", "", "NATS TLS certificate authority, for resources without an Account.")
	flag.StringVar(&cacheDir, "cache-dir", "/tmp/nack", "Directory for the creds and TLS files taken from Secrets.")
	flag.BoolVar(&readOnly, "read-only", false, "Reconcile without changing anything in NATS.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.DurationVar(&poll, "poll-interval", 10*time.Second, "How often the directory is checked for changes.")
	flag.DurationVar(&resync, "resync-interval", time.Minute, "How often every resource is reconciled.")
	flag.DurationVar(&retry, "retry-interval", 15*time.Second, "The wait after a failed reconcile.")
	flag.Parse()

	logger := klog.NewKlogr()
	log.SetLogger(logger)
	ctrl.SetLogger(logger)
	l := logger.WithName("nack-file")
	if err := run(l, dir, namespace, server, creds, ca, cacheDir, probeAddr, readOnly, poll, resync, retry); err != nil {
		l.Error(err, "stopped")
		os.Exit(1)
	}
}

func run(l logr.Logger, dir, namespace, server, creds, ca, cacheDir, probeAddr string, readOnly bool,
	poll, resync, retry time.Duration) error {
	if namespace == "" {
		return fmt.Errorf("--namespace (or POD_NAMESPACE) is required")
	}
	if err := os.MkdirAll(cacheDir, 0o700); err != nil {
		return fmt.Errorf("create cache dir: %w", err)
	}
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(api.AddToScheme(scheme))

	kinds := []client.Object{&api.Account{}, &api.Stream{}, &api.KeyValue{}, &api.ObjectStore{}, &api.Consumer{}}
	mem := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(kinds...).Build()
	cluster, err := client.New(ctrl.GetConfigOrDie(), client.Options{Scheme: scheme})
	if err != nil {
		return fmt.Errorf("kubernetes client: %w", err)
	}
	c := configsource.NewSplitClient(mem, client.NewNamespacedClient(cluster, namespace))

	natsCfg := &controller.NatsConfig{ClientName: "nack-file", ServerURL: server, Credentials: creds, CAs: []string{}}
	if ca != "" {
		natsCfg.CAs = []string{ca}
	}
	base, err := controller.NewJSController(c, natsCfg, &controller.Config{
		ReadOnly:        readOnly,
		Namespace:       namespace,
		CacheDir:        cacheDir,
		RequeueInterval: resync,
	})
	if err != nil {
		return err
	}

	runner := &configsource.Runner{
		Dir:       dir,
		Namespace: namespace,
		Scheme:    scheme,
		Mem:       c,
		Poll:      poll,
		Resync:    resync,
		Retry:     retry,
		Log:       l,
		Kinds: []configsource.Kind{
			{Object: &api.Account{}, Reconciler: &controller.AccountReconciler{Scheme: scheme, JetStreamController: base}},
			{Object: &api.Stream{}, Reconciler: &controller.StreamReconciler{Scheme: scheme, JetStreamController: base}},
			{Object: &api.KeyValue{}, Reconciler: &controller.KeyValueReconciler{Scheme: scheme, JetStreamController: base}},
			{Object: &api.ObjectStore{}, Reconciler: &controller.ObjectStoreReconciler{Scheme: scheme, JetStreamController: base}},
			{Object: &api.Consumer{}, Reconciler: &controller.ConsumerReconciler{Scheme: scheme, JetStreamController: base}},
		},
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go serveProbes(l, probeAddr, runner)
	l.Info("starting", "configDir", dir, "namespace", namespace, "readOnly", readOnly)
	return runner.Run(ctx)
}

func serveProbes(l logr.Logger, addr string, runner *configsource.Runner) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := runner.Ready(r); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	if err := http.ListenAndServe(addr, mux); err != nil {
		l.Error(err, "probe endpoint stopped")
	}
}
