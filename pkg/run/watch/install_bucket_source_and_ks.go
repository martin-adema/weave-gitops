package watch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1beta2"
	"github.com/fluxcd/pkg/apis/meta"
	sourcev1 "github.com/fluxcd/source-controller/api/v1beta2"
	"github.com/fsnotify/fsnotify"
	"github.com/minio/minio-go/v7"
	ignore "github.com/sabhiram/go-gitignore"
	"github.com/weaveworks/weave-gitops/pkg/logger"
	"github.com/weaveworks/weave-gitops/pkg/run"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/cli-utils/pkg/object"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type SetupBucketSourceAndKSParams struct {
	Namespace     string
	Path          string
	Timeout       time.Duration
	DevBucketPort int32
	SessionName   string
	Username      string
}

func SetupBucketSourceAndKS(ctx context.Context, log logger.Logger, kubeClient client.Client, params SetupBucketSourceAndKSParams) error {
	var devBucketCredentials = fmt.Sprintf("%s-credentials", RunDevBucketName)

	secret := corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      devBucketCredentials,
			Namespace: params.Namespace,
		},
		Data: map[string][]byte{
			"accesskey": []byte("user"),
			"secretkey": []byte("doesn't matter"),
		},
		Type: "Opaque",
	}
	source := sourcev1.Bucket{
		ObjectMeta: metav1.ObjectMeta{
			Name:      RunDevBucketName,
			Namespace: params.Namespace,
			Annotations: map[string]string{
				"metadata.weave.works/description": "This is a temporary Bucket created by GitOps Run. This will be cleaned up when this instance of GitOps Run is ended.",
				"metadata.weave.works/run-id":      params.SessionName,
				"metadata.weave.works/username":    params.Username,
			},
		},
		Spec: sourcev1.BucketSpec{
			Interval:   metav1.Duration{Duration: 30 * 24 * time.Hour}, // 30 days
			Provider:   "generic",
			BucketName: RunDevBucketName,
			Endpoint:   fmt.Sprintf("%s.%s.svc.cluster.local:%d", RunDevBucketName, GitOpsRunNamespace, params.DevBucketPort),
			Insecure:   true,
			Timeout:    &metav1.Duration{Duration: params.Timeout},
			SecretRef:  &meta.LocalObjectReference{Name: devBucketCredentials},
		},
	}
	ks := kustomizev1.Kustomization{
		ObjectMeta: metav1.ObjectMeta{
			Name:      RunDevKsName,
			Namespace: params.Namespace,
			Annotations: map[string]string{
				"metadata.weave.works/description": "This is a temporary Kustomization created by GitOps Run. This will be cleaned up when this instance of GitOps Run is ended.",
				"metadata.weave.works/run-id":      params.SessionName,
				"metadata.weave.works/username":    params.Username,
			},
		},
		Spec: kustomizev1.KustomizationSpec{
			Interval: metav1.Duration{Duration: 30 * 24 * time.Hour}, // 30 days
			Prune:    true,                                           // GC the kustomization
			SourceRef: kustomizev1.CrossNamespaceSourceReference{
				Kind: sourcev1.BucketKind,
				Name: RunDevBucketName,
			},
			Timeout: &metav1.Duration{Duration: params.Timeout},
			Path:    params.Path,
			Wait:    true,
		},
	}

	// create secret
	log.Actionf("Checking secret %s ...", secret.Name)

	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(&secret), &secret); err != nil && apierrors.IsNotFound(err) {
		if err := kubeClient.Create(ctx, &secret); err != nil {
			return fmt.Errorf("couldn't create secret %s: %v", secret.Name, err.Error())
		} else {
			log.Successf("Created secret %s", secret.Name)
		}
	} else if err == nil {
		log.Successf("Secret %s already existed", secret.Name)
	}

	// create source
	log.Actionf("Checking bucket source %s ...", source.Name)

	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(&source), &source); err != nil && apierrors.IsNotFound(err) {
		if err := kubeClient.Create(ctx, &source); err != nil {
			return fmt.Errorf("couldn't create source %s: %v", source.Name, err.Error())
		} else {
			log.Successf("Created source %s", source.Name)
		}
	} else if err == nil {
		log.Successf("Source %s already existed", source.Name)
	}

	// create ks
	log.Actionf("Checking Kustomization %s ...", ks.Name)

	if err := kubeClient.Get(ctx, client.ObjectKeyFromObject(&ks), &ks); err != nil && apierrors.IsNotFound(err) {
		if err := kubeClient.Create(ctx, &ks); err != nil {
			return fmt.Errorf("couldn't create kustomization %s: %v", ks.Name, err.Error())
		} else {
			log.Successf("Created ks %s", ks.Name)
		}
	} else if err == nil {
		log.Successf("Kustomization %s already existed", source.Name)
	}

	log.Successf("Setup Bucket Source and Kustomization successfully")

	return nil
}

// SyncDir recursively uploads all files in a directory to an S3 bucket with minio library
func SyncDir(ctx context.Context, log logger.Logger, dir string, bucket string, client *minio.Client, ignorer *ignore.GitIgnore) error {
	log.Actionf("Refreshing bucket %s ...", bucket)

	if err := client.RemoveBucketWithOptions(ctx, bucket, minio.RemoveBucketOptions{
		ForceDelete: true,
	}); err != nil {
		// if error is not bucket not found, return error
		if !strings.Contains(err.Error(), "NoSuchBucket") {
			return err
		}
	}

	if err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
		return err
	}

	uploadCount := 0
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Failuref("Error walking directory: %v", err)
			return err
		}

		if info.IsDir() {
			// if it's a hidden directory, ignore it
			if strings.HasPrefix(info.Name(), ".") {
				return filepath.SkipDir
			}

			return nil
		}

		objectName, err := filepath.Rel(dir, path)
		if err != nil {
			log.Failuref("Error getting relative path: %v", err)
			return err
		}
		if ignorer.MatchesPath(path) {
			return nil
		}
		// upload the file
		_, err = client.FPutObject(ctx, bucket, objectName, path, minio.PutObjectOptions{})

		if err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			errResp, ok := err.(minio.ErrorResponse)
			if ok && errResp.Code == "MissingContentLength" {
				// This happens when the file was empty - this is OK
				return nil
			}
			// Report the error, but continue anyway - this could be e.g.
			// a file with odd permissions, which isn't necessarily a problem
			log.Failuref("Couldn't upload %v: %v", path, err)
			return nil
		}
		uploadCount = uploadCount + 1
		if uploadCount%10 == 0 {
			fmt.Print(".")
		}
		return nil
	})

	fmt.Println()
	log.Actionf("Uploaded %d files", uploadCount)

	if err != nil && !errors.Is(err, context.Canceled) {
		log.Failuref("Error syncing directory: %v", err)
		return err
	}

	return nil
}

// CleanupBucketSourceAndKS removes the bucket source and ks
func CleanupBucketSourceAndKS(ctx context.Context, log logger.Logger, kubeClient client.Client, namespace string) error {
	var devBucketCredentials = fmt.Sprintf("%s-credentials", RunDevBucketName)

	// delete secret
	secret := corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      devBucketCredentials,
			Namespace: namespace,
		},
	}

	log.Actionf("Deleting secret %s ...", secret.Name)

	if err := kubeClient.Delete(ctx, &secret); err != nil {
		log.Failuref("Error deleting secret %s: %v", secret.Name, err.Error())
	} else {
		log.Successf("Deleted secret %s", secret.Name)
	}

	// delete source
	source := sourcev1.Bucket{
		ObjectMeta: metav1.ObjectMeta{
			Name:      RunDevBucketName,
			Namespace: namespace,
		},
	}

	log.Actionf("Deleting source %s ...", source.Name)

	if err := kubeClient.Delete(ctx, &source); err != nil {
		log.Failuref("Error deleting source %s: %v", source.Name, err.Error())
	} else {
		log.Successf("Deleted source %s", source.Name)
	}

	// delete ks
	ks := kustomizev1.Kustomization{
		ObjectMeta: metav1.ObjectMeta{
			Name:      RunDevKsName,
			Namespace: namespace,
		},
	}

	log.Actionf("Deleting ks %s ...", ks.Name)

	if err := kubeClient.Delete(ctx, &ks); err != nil {
		log.Failuref("Error deleting ks %s: %v", ks.Name, err.Error())
	} else {
		log.Successf("Deleted ks %s", ks.Name)
	}

	log.Successf("Cleanup Bucket Source and Kustomization successfully")

	return nil
}

// findConditionMessages finds the messages in the condition of objects in the inventory.
func findConditionMessages(ctx context.Context, kubeClient client.Client, ks *kustomizev1.Kustomization) ([]string, error) {
	if ks.Status.Inventory == nil {
		return nil, fmt.Errorf("inventory is nil")
	}

	gvks := map[string]schema.GroupVersionKind{}
	// collect gvk of the objects
	for _, entry := range ks.Status.Inventory.Entries {
		objMeta, err := object.ParseObjMetadata(entry.ID)
		if err != nil {
			return nil, fmt.Errorf("invalid inventory item '%s', error: %w", entry.ID, err)
		}

		gvkID := strings.Join([]string{objMeta.GroupKind.Group, entry.Version, objMeta.GroupKind.Kind}, "_")

		if _, exist := gvks[gvkID]; !exist {
			gvks[gvkID] = schema.GroupVersionKind{
				Group:   objMeta.GroupKind.Group,
				Version: entry.Version,
				Kind:    objMeta.GroupKind.Kind,
			}
		}
	}

	var messages []string

	for _, gvk := range gvks {
		unstructuredList := &unstructured.UnstructuredList{}
		unstructuredList.SetGroupVersionKind(gvk)

		if err := kubeClient.List(ctx, unstructuredList,
			client.MatchingLabelsSelector{
				Selector: labels.Set(
					map[string]string{
						"kustomize.toolkit.fluxcd.io/name":      ks.Name,
						"kustomize.toolkit.fluxcd.io/namespace": ks.Namespace,
					},
				).AsSelector(),
			},
		); err != nil {
			return nil, err
		}

		for _, u := range unstructuredList.Items {
			if conditions, found, err := unstructured.NestedSlice(u.UnstructuredContent(), "status", "conditions"); err == nil && found {
				for _, condition := range conditions {
					c := condition.(map[string]interface{})
					if status, found, err := unstructured.NestedString(c, "status"); err == nil && found {
						if status != "True" {
							if message, found, err := unstructured.NestedString(c, "message"); err == nil && found {
								messages = append(messages, fmt.Sprintf("%s %s/%s: %s", u.GetKind(), u.GetNamespace(), u.GetName(), message))
							}
						}
					}
				}
			}
		}
	}

	return messages, nil
}

func WatchDirsForFileWalker(watcher *fsnotify.Watcher, ignorer *ignore.GitIgnore) func(path string, info os.FileInfo, err error) error {
	return func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return fmt.Errorf("error walking path: %v", err)
		}

		if info.IsDir() {
			// if it's a hidden directory, ignore it
			if strings.HasPrefix(info.Name(), ".") {
				return filepath.SkipDir
			}

			if ignorer.MatchesPath(path) {
				return filepath.SkipDir
			}

			if err := watcher.Add(path); err != nil {
				return err
			}
		}

		return nil
	}
}

func InitializeTargetDir(targetPath string) error {
	shouldCreate := false
	stat, err := os.Stat(targetPath)

	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	} else if err != nil {
		err := os.Mkdir(targetPath, 0755)
		if err != nil {
			return err
		}
		shouldCreate = true
	} else if !stat.IsDir() {
		return fmt.Errorf("target must be a directory")
	} else {
		f, err := os.Open(targetPath)
		if err != nil {
			return err
		}

		_, err = f.Readdirnames(1)
		if err != nil && errors.Is(err, io.EOF) {
			shouldCreate = true
		} else if err != nil {
			return err
		}
	}

	if shouldCreate {
		f, err := os.Create(filepath.Join(targetPath, "kustomization.yaml"))
		if err != nil {
			return fmt.Errorf("error creating entrypoint kustomization.yaml: %w", err)
		}

		_, err = f.Write([]byte(`---
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources: [] # 👋 Start adding the resources you want to sync here
`))
		if err != nil {
			return err
		}
	}

	return nil
}

// ReconcileDevBucketSourceAndKS reconciles the dev-bucket and dev-ks asynchronously.
func ReconcileDevBucketSourceAndKS(ctx context.Context, log logger.Logger, kubeClient client.Client, namespace string, timeout time.Duration) error {
	const interval = 3 * time.Second / 2

	// reconcile dev-bucket
	sourceRequestedAt, err := run.RequestReconciliation(ctx, kubeClient,
		types.NamespacedName{
			Name:      RunDevBucketName,
			Namespace: namespace,
		}, schema.GroupVersionKind{
			Group:   "source.toolkit.fluxcd.io",
			Version: "v1beta2",
			Kind:    sourcev1.BucketKind,
		})
	if err != nil {
		return err
	}

	// wait for the reconciliation of dev-bucket to be done
	if err := wait.Poll(interval, timeout, func() (bool, error) {
		devBucket := &sourcev1.Bucket{}
		if err := kubeClient.Get(ctx, types.NamespacedName{
			Name:      RunDevBucketName,
			Namespace: namespace,
		}, devBucket); err != nil {
			return false, err
		}

		return devBucket.Status.GetLastHandledReconcileRequest() == sourceRequestedAt, nil
	}); err != nil {
		return err
	}

	// wait for devBucket to be ready
	if err := wait.Poll(interval, timeout, func() (bool, error) {
		devBucket := &sourcev1.Bucket{}
		if err := kubeClient.Get(ctx, types.NamespacedName{
			Name:      RunDevBucketName,
			Namespace: namespace,
		}, devBucket); err != nil {
			return false, err
		}
		return apimeta.IsStatusConditionPresentAndEqual(devBucket.Status.Conditions, meta.ReadyCondition, metav1.ConditionTrue), nil
	}); err != nil {
		return err
	}

	// reconcile dev-ks
	ksRequestedAt, err := run.RequestReconciliation(ctx, kubeClient,
		types.NamespacedName{
			Name:      RunDevKsName,
			Namespace: namespace,
		}, schema.GroupVersionKind{
			Group:   "kustomize.toolkit.fluxcd.io",
			Version: "v1beta2",
			Kind:    kustomizev1.KustomizationKind,
		})
	if err != nil {
		return err
	}

	if err := wait.Poll(interval, timeout, func() (bool, error) {
		devKs := &kustomizev1.Kustomization{}
		if err := kubeClient.Get(ctx, types.NamespacedName{
			Name:      RunDevKsName,
			Namespace: namespace,
		}, devKs); err != nil {
			return false, err
		}

		return devKs.Status.GetLastHandledReconcileRequest() == ksRequestedAt, nil
	}); err != nil {
		return err
	}

	devKs := &kustomizev1.Kustomization{}
	devKsErr := wait.Poll(interval, timeout, func() (bool, error) {
		if err := kubeClient.Get(ctx, types.NamespacedName{
			Name:      RunDevKsName,
			Namespace: namespace,
		}, devKs); err != nil {
			return false, err
		}

		healthy := apimeta.IsStatusConditionPresentAndEqual(
			devKs.Status.Conditions,
			kustomizev1.HealthyCondition,
			metav1.ConditionTrue,
		)
		return healthy, nil
	})

	if devKsErr != nil {
		messages, err := findConditionMessages(ctx, kubeClient, devKs)
		if err != nil {
			return err
		}

		for _, msg := range messages {
			log.Failuref(msg)
		}
	}

	return devKsErr
}

func CreateIgnorer(gitRootDir string) *ignore.GitIgnore {
	ignoreFile := filepath.Join(gitRootDir, ".gitignore")

	var ignorer *ignore.GitIgnore = nil
	if _, err := os.Stat(ignoreFile); err == nil {
		ignorer, err = ignore.CompileIgnoreFile(ignoreFile)
		if err != nil {
			// If we couldn't parse gitignore, just ignore nothing
			ignorer = nil
		}
	}

	if ignorer == nil {
		// Whether there was no gitignore file or the one that was there was broken,
		// fall back to just no ignore lines - this is just a pass-through
		ignorer = ignore.CompileIgnoreLines()
	}

	return ignorer
}
