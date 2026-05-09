/*
Copyright © contributors to CloudNativePG, established as
CloudNativePG a Series of LF Projects, LLC.

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

package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/ptr"

	cloudnativepgv1 "github.com/cloudnative-pg/api/pkg/api/v1"
	pluginv1 "github.com/cloudnative-pg/plugin-barman-cloud/api/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	pluginName            = "barman-cloud.cloudnative-pg.io"
	recoveryPod           = "recovery-pod"
	hibernationAnnotation = "cnpg.io/hibernation"
	pgdataDir             = "/var/lib/postgresql/data/pgdata"
	barmanRestorePath     = "/venv/bin/barman-cloud-restore"
	sidecarImage          = "docker.io/library/barman-cloud-sidecar:latest"

	barmanObjectNameKey       = "barmanObjectName"
	pluginMetadataTimelineKey = "timeline"

	restoreServiceAccount  = "barman-restore-sa"
	restoreScriptConfigMap = "barman-restore-script"

	postgresUID         = 26
	jobDeletionWaitTime = 30 * time.Second
	jobPollInterval     = 5 * time.Second
	jobTimeout          = 15 * time.Minute
)

var (
	runtimeScheme = runtime.NewScheme()
)

func init() {
	_ = cloudnativepgv1.AddToScheme(runtimeScheme)
	pluginv1.AddKnownTypes(runtimeScheme)
	_ = corev1.AddToScheme(runtimeScheme)
	_ = batchv1.AddToScheme(runtimeScheme)
}

func main() {
	var (
		backupName  string
		namespace   string
		clusterName string
		dryRun      bool
		force       bool
	)

	flag.StringVar(&backupName, "backup", "", "Backup 资源名称 (必需)")
	flag.StringVar(&namespace, "namespace", "", "命名空间 (可选，默认为当前命名空间)")
	flag.StringVar(&clusterName, "cluster", "", "PostgreSQL 集群名称 (必需)")
	flag.BoolVar(&dryRun, "dry-run", false, "仅打印操作，不实际执行")
	flag.BoolVar(&force, "force", false, "跳过确认提示（危险！）")

	fmt.Println("⚠️  前提条件: 请确保已在目标命名空间配置 RBAC")
	fmt.Println("   配置命令: kubectl apply -f restore.yaml -n <命名空间>")
	fmt.Println()

	flag.Parse()

	if backupName == "" || clusterName == "" {
		fmt.Println("错误: 必须指定 --backup 和 --cluster 参数")
		flag.Usage()
		os.Exit(1)
	}

	if namespace == "" {
		var err error
		namespace, err = getCurrentNamespace()
		if err != nil {
			fmt.Printf("警告: 无法获取当前命名空间: %v, 使用 'default'\n", err)
			namespace = "default"
		}
	}

	ctx := context.Background()
	cfg, err := getKubeConfig()
	if err != nil {
		fmt.Printf("错误: 无法获取 Kubernetes 配置: %v\n", err)
		os.Exit(1)
	}

	c, err := client.New(cfg, client.Options{Scheme: runtimeScheme})
	if err != nil {
		fmt.Printf("错误: 无法创建 Kubernetes 客户端: %v\n", err)
		os.Exit(1)
	}

	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		fmt.Printf("错误: 无法创建 Clientset: %v\n", err)
		os.Exit(1)
	}

	// 获取 Backup 资源
	var backup cloudnativepgv1.Backup
	if err := c.Get(ctx, client.ObjectKey{Name: backupName, Namespace: namespace}, &backup); err != nil {
		fmt.Printf("错误: 无法获取 Backup 资源: %v\n", err)
		os.Exit(1)
	}

	if backup.Status.Phase != cloudnativepgv1.BackupPhaseCompleted {
		fmt.Printf("错误: 备份状态不是 'completed', 当前状态: %s\n", backup.Status.Phase)
		os.Exit(1)
	}

	printBackupInfo(&backup)

	// 获取 Cluster 配置以找到 ObjectStore 和 PVC 名称
	var cluster cloudnativepgv1.Cluster
	if err := c.Get(ctx, client.ObjectKey{Name: clusterName, Namespace: namespace}, &cluster); err != nil {
		fmt.Printf("错误: 获取 Cluster 失败: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("✓ 找到集群: %s (当前副本数: %d)\n", clusterName, cluster.Spec.Instances)
	fmt.Println()

	// 构建恢复参数
	s3Creds, params, err := buildRestoreParams(ctx, c, &backup, &cluster, namespace)
	if err != nil {
		fmt.Printf("错误: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("=== 恢复流程（全部在 Job 内部执行） ===")
	fmt.Println("1. 启用 Hibernation 停止 PostgreSQL Pod")
	fmt.Println("2. 清理旧数据目录并执行 barman-cloud-restore")
	fmt.Println("3. 配置恢复参数并创建 recovery.signal")
	fmt.Println("4. 关闭 Hibernation 唤醒 Cluster")
	fmt.Println("5. 等待 PostgreSQL Pod 启动并就绪")
	fmt.Println()

	fmt.Printf("恢复参数: 集群=%s, 命名空间=%s, timeline=%s, end_lsn=%s\n",
		params.ClusterName, params.Namespace, params.Timeline, params.EndLSN)
	fmt.Println()

	if dryRun {
		fmt.Println("✓ 仅打印模式，未执行任何操作")
		return
	}

	// 确认操作
	if !force {
		fmt.Println("⚠ 警告: 此操作将停止 PostgreSQL 并从备份恢复数据！")
		fmt.Println("  - 当前集群数据将被备份覆盖")
		fmt.Println("  - PostgreSQL 服务会中断")
		fmt.Println("  - 恢复时间取决于备份大小")
		fmt.Print("\n请输入 'yes' 确认继续: ")

		reader := bufio.NewReader(os.Stdin)
		confirm, _ := reader.ReadString('\n')
		confirm = strings.TrimSpace(strings.ToLower(confirm))

		if confirm != "yes" {
			fmt.Println("操作已取消")
			return
		}
	}

	// 执行恢复流程
	if err := executeRestoreWorkflow(ctx, c, clientset, &cluster, s3Creds, params, namespace); err != nil {
		fmt.Printf("\n错误: 恢复执行失败: %v\n", err)
		fmt.Println("\n建议手动检查:")
		fmt.Printf("  1. 检查 Pod 状态: kubectl get pods -n %s\n", namespace)
		fmt.Printf("  2. 检查恢复 Pod 日志: kubectl logs -n %s %s\n", namespace, recoveryPod)
		fmt.Printf("  3. 清理恢复 Pod: kubectl delete pod -n %s %s --force\n", namespace, recoveryPod)
		os.Exit(1)
	}

	fmt.Println("\n✓ 恢复流程已完成！")
	fmt.Println("\n下一步操作:")
	fmt.Printf("  1. 查看 PostgreSQL 日志: kubectl logs -n %s -l cnpg.io/cluster=%s -c postgres\n", namespace, clusterName)
	fmt.Printf("  2. 检查集群状态: kubectl cnpg status %s -n %s\n", clusterName, namespace)
}

func printBackupInfo(backup *cloudnativepgv1.Backup) {
	fmt.Printf("✓ 找到备份: %s\n", backup.Name)
	fmt.Printf("  备份 ID: %s\n", backup.Status.BackupID)
	fmt.Printf("  服务器名称: %s\n", backup.Status.ServerName)
	fmt.Printf("  目标路径: %s\n", backup.Status.DestinationPath)
	if backup.Status.StoppedAt != nil {
		fmt.Printf("  备份时间: %s\n", backup.Status.StoppedAt.Time.Format("2006-01-02 15:04:05"))
	}
	fmt.Printf("  起始 WAL: %s\n", backup.Status.BeginWal)
	fmt.Printf("  结束 WAL: %s\n", backup.Status.EndWal)
	fmt.Println()
}

func getKubeConfig() (*rest.Config, error) {
	kubeconfig := os.Getenv("KUBECONFIG")
	if kubeconfig == "" {
		home := os.Getenv("HOME")
		if home == "" {
			home = os.Getenv("USERPROFILE")
		}
		kubeconfig = filepath.Join(home, ".kube", "config")
	}

	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		config, err = rest.InClusterConfig()
		if err != nil {
			return nil, err
		}
	}
	return config, nil
}

func getCurrentNamespace() (string, error) {
	if data, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); err == nil {
		return string(data), nil
	}
	return "default", nil
}

type S3Credentials struct {
	SecretName string
	AccessKey  string
	SecretKey  string
}

type RestoreParams struct {
	BarmanArgs  string
	Timeline    string
	EndLSN      string
	ClusterName string
	Namespace   string
}

func buildRestoreParams(ctx context.Context, c client.Client, backup *cloudnativepgv1.Backup, cluster *cloudnativepgv1.Cluster, namespace string) (S3Credentials, RestoreParams, error) {
	// 从 Cluster 插件配置中获取 ObjectStore 名称
	var objectStoreName string
	for _, plugin := range cluster.Spec.Plugins {
		if plugin.Name == pluginName && plugin.Parameters[barmanObjectNameKey] != "" {
			objectStoreName = plugin.Parameters[barmanObjectNameKey]
			break
		}
	}

	if objectStoreName == "" {
		return S3Credentials{}, RestoreParams{}, fmt.Errorf("未在 Cluster 插件配置中找到 barmanObjectName")
	}

	// 获取 ObjectStore
	var objectStore pluginv1.ObjectStore
	if err := c.Get(ctx, client.ObjectKey{Name: objectStoreName, Namespace: namespace}, &objectStore); err != nil {
		return S3Credentials{}, RestoreParams{}, fmt.Errorf("获取 ObjectStore 失败: %w", err)
	}

	// 获取 S3 凭证 Secret 引用（Job 会通过环境变量直接从 Secret 加载）
	if objectStore.Spec.Configuration.AWS == nil {
		return S3Credentials{}, RestoreParams{}, fmt.Errorf("ObjectStore 中未配置 AWS 凭证")
	}

	accessKeyRef := objectStore.Spec.Configuration.AWS.AccessKeyIDReference
	secretKeyRef := objectStore.Spec.Configuration.AWS.SecretAccessKeyReference
	if accessKeyRef == nil || secretKeyRef == nil {
		return S3Credentials{}, RestoreParams{}, fmt.Errorf("ObjectStore 中未配置完整的 AWS 凭证引用")
	}

	s3Creds := S3Credentials{
		SecretName: accessKeyRef.Name,
		AccessKey:  accessKeyRef.Key,
		SecretKey:  secretKeyRef.Key,
	}

	// 构建 barman-cloud-restore 命令参数（不含可执行文件）
	var cmdArgs []string

	// 添加 endpoint URL 参数
	if objectStore.Spec.Configuration.EndpointURL != "" {
		cmdArgs = append(cmdArgs, "--endpoint-url", objectStore.Spec.Configuration.EndpointURL)
	}

	// 目标路径（使用 ObjectStore 中的配置）
	destinationPath := objectStore.Spec.Configuration.DestinationPath
	cmdArgs = append(cmdArgs, destinationPath)

	// 服务器名称（如果备份没有则使用集群名称）
	serverName := backup.Status.ServerName
	if serverName == "" {
		serverName = cluster.Name
	}
	cmdArgs = append(cmdArgs, serverName)

	// 备份 ID
	cmdArgs = append(cmdArgs, backup.Status.BackupID)

	cmdArgs = append(cmdArgs, pgdataDir)

	quotedArgs := make([]string, len(cmdArgs))
	for i, arg := range cmdArgs {
		quotedArgs[i] = quoteShellString(arg)
	}
	shellArgs := strings.Join(quotedArgs, " ")

	params := RestoreParams{
		BarmanArgs:  shellArgs,
		Timeline:    backup.Status.PluginMetadata[pluginMetadataTimelineKey],
		EndLSN:      backup.Status.EndLSN,
		ClusterName: cluster.Name,
		Namespace:   namespace,
	}

	return s3Creds, params, nil
}

func executeRestoreWorkflow(ctx context.Context, c client.Client, clientset *kubernetes.Clientset, cluster *cloudnativepgv1.Cluster, s3Creds S3Credentials, params RestoreParams, namespace string) error {
	serviceAccountName := restoreServiceAccount
	configMapName := restoreScriptConfigMap

	// Step 1: 创建恢复 Job
	fmt.Println("\n[1/2] 创建恢复 Job...")
	jobName := fmt.Sprintf("%s-recovery", cluster.Name)
	pvcName := fmt.Sprintf("%s-1", cluster.Name) // 主节点 PVC 命名规则

	var existingJob batchv1.Job
	if err := c.Get(ctx, client.ObjectKey{Name: jobName, Namespace: namespace}, &existingJob); err == nil {
		fmt.Println("清理已存在的恢复 Job...")
		if err := c.Delete(ctx, &existingJob); err != nil {
			fmt.Printf("警告: 删除旧 Job 失败: %v\n", err)
		}
		if err := wait.PollUntilContextTimeout(ctx, jobPollInterval, jobDeletionWaitTime, true, func(ctx context.Context) (bool, error) {
			var j batchv1.Job
			if err := c.Get(ctx, client.ObjectKey{Name: jobName, Namespace: namespace}, &j); err != nil {
				return true, nil
			}
			return false, nil
		}); err != nil {
			fmt.Printf("警告: 等待旧 Job 删除超时: %v\n", err)
		}
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: namespace,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: ptr.To(int32(0)),
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:    "recovery",
							Image:   sidecarImage,
							Command: []string{"bash", "/scripts/restore.sh"},
							Env: []corev1.EnvVar{
								{
									Name:  "PGDATA_DIR",
									Value: pgdataDir,
								},
								{
									Name:  "CLUSTER_NAME",
									Value: params.ClusterName,
								},
								{
									Name:  "NAMESPACE",
									Value: params.Namespace,
								},
								{
									Name:  "BARMAN_ARGS",
									Value: params.BarmanArgs,
								},
								{
									Name:  "RECOVERY_TIMELINE",
									Value: params.Timeline,
								},
								{
									Name:  "RECOVERY_LSN",
									Value: params.EndLSN,
								},
								{
									Name: "AWS_ACCESS_KEY_ID",
									ValueFrom: &corev1.EnvVarSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: s3Creds.SecretName,
											},
											Key: s3Creds.AccessKey,
										},
									},
								},
								{
									Name: "AWS_SECRET_ACCESS_KEY",
									ValueFrom: &corev1.EnvVarSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{
												Name: s3Creds.SecretName,
											},
											Key: s3Creds.SecretKey,
										},
									},
								},
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "pgdata",
									MountPath: "/var/lib/postgresql/data",
								},
								{
									Name:      "script",
									MountPath: "/scripts",
								},
							},
							SecurityContext: &corev1.SecurityContext{
								RunAsUser:  ptr.To(int64(postgresUID)),
								RunAsGroup: ptr.To(int64(postgresUID)),
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "pgdata",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: pvcName,
								},
							},
						},
						{
							Name: "script",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: configMapName,
									},
									DefaultMode: ptr.To(int32(0755)),
								},
							},
						},
					},
					ServiceAccountName: serviceAccountName,
					RestartPolicy:      corev1.RestartPolicyNever,
				},
			},
		},
	}

	if err := c.Create(ctx, job); err != nil {
		return fmt.Errorf("创建恢复 Job 失败: %w", err)
	}
	fmt.Println("✓ 恢复 Job 已创建")

	// Step 2: 等待 Job 完成
	fmt.Println("\n[2/2] 等待恢复 Job 完成...")
	fmt.Println("这可能需要几分钟时间，请耐心等待...")
	fmt.Println("Job 将执行全部流程: 停止集群 -> 恢复数据 -> 唤醒集群 -> 等待 PostgreSQL 启动成功")

	if err := wait.PollUntilContextTimeout(ctx, jobPollInterval, jobTimeout, true, func(ctx context.Context) (bool, error) {
		var j batchv1.Job
		if err := c.Get(ctx, client.ObjectKey{Name: jobName, Namespace: namespace}, &j); err != nil {
			return false, err
		}

		for _, cond := range j.Status.Conditions {
			if cond.Type == batchv1.JobComplete && cond.Status == corev1.ConditionTrue {
				return true, nil
			}
			if cond.Type == batchv1.JobFailed && cond.Status == corev1.ConditionTrue {
				return false, fmt.Errorf("恢复 Job 失败: %s", cond.Message)
			}
		}
		return false, nil
	}); err != nil {
		// 输出 Job 日志帮助调试
		fmt.Printf("\n恢复 Job 超时或失败，尝试获取日志...\n")
		if podList, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
			LabelSelector: fmt.Sprintf("job-name=%s", jobName),
		}); err == nil && len(podList.Items) > 0 {
			podName := podList.Items[0].Name
			fmt.Printf("Pod %s 日志:\n", podName)
			req := clientset.CoreV1().Pods(namespace).GetLogs(podName, &corev1.PodLogOptions{})
			if logs, err := req.Stream(ctx); err == nil {
				defer logs.Close()
				_, _ = io.Copy(os.Stdout, logs)
			}
		}
		return fmt.Errorf("等待恢复 Job 完成失败: %w", err)
	}
	fmt.Println("✓ 恢复 Job 已完成")

	// 清理 Job（可选，用户可能想保留日志）
	fmt.Printf("\n提示: 恢复 Job '%s' 已保留，可通过以下命令查看日志:\n", jobName)
	fmt.Printf("  kubectl logs -n %s job/%s\n", namespace, jobName)

	return nil
}

func quoteShellString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'"
}
