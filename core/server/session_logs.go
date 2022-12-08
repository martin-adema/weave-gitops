package server

import (
	"context"
	"fmt"
	"io"

	sourcev1 "github.com/fluxcd/source-controller/api/v1beta2"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	pb "github.com/weaveworks/weave-gitops/pkg/api/core"
	"github.com/weaveworks/weave-gitops/pkg/server/auth"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// GetSessionLogs returns the logs for a session.
func (cs *coreServer) GetSessionLogs(ctx context.Context, msg *pb.GetSessionLogsRequest) (*pb.GetSessionLogsResponse, error) {
	const (
		sourceName = "run-dev-bucket"
		bucketName = "gitops-run-logs"
	)
	var secretName := sourceName + "-credentials"

	clustersClient, err := cs.clustersManager.GetImpersonatedClient(ctx, auth.Principal(ctx))
	if err != nil {
		return nil, fmt.Errorf("error getting impersonating client: %w", err)
	}

	cli, err := clustersClient.Scoped(msg.GetClusterName())
	if err != nil {
		return nil, fmt.Errorf("getting cluster client: %w", err)
	}

	// get secret
	secret := corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: msg.GetNamespace(),
		},
	}

	// get secret
	if err := cli.Get(ctx, client.ObjectKeyFromObject(&secret), &secret); err != nil {
		return nil, err
	}

	accessKey := string(secret.Data["accesskey"])
	secretKey := string(secret.Data["secretkey"])

	// get bucket source
	bucket := sourcev1.Bucket{
		ObjectMeta: metav1.ObjectMeta{
			Name:      sourceName,
			Namespace: msg.GetNamespace(),
		},
	}

	err = cli.Get(ctx, client.ObjectKeyFromObject(&bucket), &bucket)
	if err != nil {
		return nil, err
	}

	minioClient, err := minio.New(
		bucket.Spec.Endpoint,
		&minio.Options{
			Creds:        credentials.NewStaticV4(accessKey, secretKey, ""),
			Secure:       !bucket.Spec.Insecure,
			BucketLookup: minio.BucketLookupPath,
		},
	)
	if err != nil {
		return nil, err
	}

	logs := []string{}
	lastToken := ""

	for obj := range minioClient.ListObjects(ctx, bucketName, minio.ListObjectsOptions{
		Prefix:     msg.GetSessionId(),
		StartAfter: msg.GetToken(),
	}) {
		if obj.Err != nil {
			return nil, obj.Err
		}

		o, err := minioClient.GetObject(ctx, bucketName, obj.Key, minio.GetObjectOptions{})
		if err != nil {
			return nil, err
		}

		if err := o.Close(); err != nil {
			return nil, err
		}

		b, err := io.ReadAll(o)
		if err != nil {
			return nil, err
		}

		logs = append(logs, string(b))
		lastToken = obj.Key
	}

	return &pb.GetSessionLogsResponse{
		Logs:      logs,
		NextToken: lastToken,
	}, nil
}
