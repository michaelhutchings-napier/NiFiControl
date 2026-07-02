package controller

import (
	"context"
	"net/http"
	"testing"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/nifi"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type fakeReportingTaskClient struct {
	validation string // validation status reported on create (default VALID)
	store      *nifi.ReportingTaskEntity
	created    []nifi.ReportingTaskEntity
	updated    []nifi.ReportingTaskEntity
	runStatus  []string
	deleted    []string
}

func (f *fakeReportingTaskClient) GetReportingTask(ctx context.Context, baseURI, id string) (*nifi.ReportingTaskEntity, error) {
	if f.store != nil && nifi.ReportingTaskEntityID(*f.store) == id {
		s := *f.store
		return &s, nil
	}
	return nil, &nifi.HTTPStatusError{StatusCode: http.StatusNotFound}
}

func (f *fakeReportingTaskClient) CreateReportingTask(ctx context.Context, baseURI string, entity nifi.ReportingTaskEntity) (*nifi.ReportingTaskEntity, error) {
	f.created = append(f.created, entity)
	created := entity
	created.ID = "rt-created"
	created.Component.ID = "rt-created"
	created.Component.State = "STOPPED"
	created.Component.ValidationStatus = "VALID"
	if f.validation != "" {
		created.Component.ValidationStatus = f.validation
	}
	f.store = &created
	s := created
	return &s, nil
}

func (f *fakeReportingTaskClient) UpdateReportingTask(ctx context.Context, baseURI string, entity nifi.ReportingTaskEntity) (*nifi.ReportingTaskEntity, error) {
	f.updated = append(f.updated, entity)
	updated := entity
	updated.Revision.Version++
	if f.store != nil {
		updated.Component.State = f.store.Component.State
		updated.Component.ValidationStatus = f.store.Component.ValidationStatus
	}
	f.store = &updated
	s := updated
	return &s, nil
}

func (f *fakeReportingTaskClient) UpdateReportingTaskRunStatus(ctx context.Context, baseURI, id string, revisionVersion int64, state string) (*nifi.ReportingTaskEntity, error) {
	f.runStatus = append(f.runStatus, state)
	if f.store == nil {
		return nil, &nifi.HTTPStatusError{StatusCode: http.StatusNotFound}
	}
	s := *f.store
	s.Component.State = state
	s.Revision.Version = revisionVersion + 1
	f.store = &s
	out := s
	return &out, nil
}

func (f *fakeReportingTaskClient) DeleteReportingTask(ctx context.Context, baseURI, id string, revisionVersion int64) error {
	f.deleted = append(f.deleted, id)
	f.store = nil
	return nil
}

func reportingTaskTestClient(scheme *runtime.Scheme, objects ...client.Object) client.Client {
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objects...).
		WithStatusSubresource(&nifiv1alpha1.NiFiCluster{}, &nifiv1alpha1.NiFiReportingTask{}).
		Build()
}

func newReportingTask(name string, state nifiv1alpha1.RuntimeState) *nifiv1alpha1.NiFiReportingTask {
	return &nifiv1alpha1.NiFiReportingTask{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiReportingTaskSpec{
			ClusterRef: nifiv1alpha1.ClusterReference{Name: "production"},
			Type:       "org.apache.nifi.reporting.ambari.AmbariReportingTask",
			Scheduling: nifiv1alpha1.ComponentScheduling{Period: "60 sec"},
			State:      state,
		},
	}
}

func TestNiFiReportingTaskReconcileCreatesAndStarts(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	rt := newReportingTask("metrics", nifiv1alpha1.RuntimeStateEnabled)
	k8sClient := reportingTaskTestClient(scheme, cluster, rt)
	tasks := &fakeReportingTaskClient{}
	r := &NiFiReportingTaskReconciler{Client: k8sClient, Scheme: scheme, ReportingTaskClient: tasks}
	reconcileTwice(t, r, rt.Name)

	if len(tasks.created) != 1 {
		t.Fatalf("create reporting tasks = %#v", tasks.created)
	}
	if len(tasks.runStatus) == 0 || tasks.runStatus[len(tasks.runStatus)-1] != "RUNNING" {
		t.Fatalf("expected the task to be started (RUNNING), run-status calls = %#v", tasks.runStatus)
	}
	got := &nifiv1alpha1.NiFiReportingTask{}
	_ = k8sClient.Get(context.Background(), types.NamespacedName{Name: rt.Name, Namespace: "default"}, got)
	if !got.Status.Ready || got.Status.NiFiID != "rt-created" {
		t.Fatalf("status = %+v", got.Status)
	}
	assertControllerCondition(t, got.Status.Conditions, nifiv1alpha1.ConditionReady, metav1.ConditionTrue, "ReportingTaskReady")
}

func TestNiFiReportingTaskReconcileCreatesDisabledStaysStopped(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	rt := newReportingTask("metrics", nifiv1alpha1.RuntimeStateDisabled)
	k8sClient := reportingTaskTestClient(scheme, cluster, rt)
	tasks := &fakeReportingTaskClient{}
	r := &NiFiReportingTaskReconciler{Client: k8sClient, Scheme: scheme, ReportingTaskClient: tasks}
	reconcileTwice(t, r, rt.Name)

	if len(tasks.created) != 1 {
		t.Fatalf("create reporting tasks = %#v", tasks.created)
	}
	if len(tasks.runStatus) != 0 {
		t.Fatalf("a disabled task should not be started: run-status calls = %#v", tasks.runStatus)
	}
	got := &nifiv1alpha1.NiFiReportingTask{}
	_ = k8sClient.Get(context.Background(), types.NamespacedName{Name: rt.Name, Namespace: "default"}, got)
	if !got.Status.Ready {
		t.Fatalf("status = %+v", got.Status)
	}
}

func TestNiFiReportingTaskReconcileInvalidNotStarted(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	rt := newReportingTask("metrics", nifiv1alpha1.RuntimeStateEnabled)
	k8sClient := reportingTaskTestClient(scheme, cluster, rt)
	tasks := &fakeReportingTaskClient{validation: "INVALID"}
	r := &NiFiReportingTaskReconciler{Client: k8sClient, Scheme: scheme, ReportingTaskClient: tasks}
	reconcileTwice(t, r, rt.Name)

	if len(tasks.runStatus) != 0 {
		t.Fatalf("an INVALID task must not be started: run-status calls = %#v", tasks.runStatus)
	}
	got := &nifiv1alpha1.NiFiReportingTask{}
	_ = k8sClient.Get(context.Background(), types.NamespacedName{Name: rt.Name, Namespace: "default"}, got)
	assertControllerCondition(t, got.Status.Conditions, nifiv1alpha1.ConditionReady, metav1.ConditionFalse, "ReportingTaskInvalid")
}

func TestNiFiReportingTaskDeleteStopsThenDeletes(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	rt := newReportingTask("metrics", nifiv1alpha1.RuntimeStateEnabled)
	rt.Finalizers = []string{NiFiControlFinalizer}
	rt.Spec.DeletionPolicy = nifiv1alpha1.DeletionPolicyDelete
	rt.Status = nifiv1alpha1.NiFiReportingTaskStatus{CommonStatus: nifiv1alpha1.CommonStatus{Ready: true, NiFiID: "rt-1", ObservedGeneration: 1}}
	k8sClient := reportingTaskTestClient(scheme, cluster, rt)
	tasks := &fakeReportingTaskClient{store: &nifi.ReportingTaskEntity{ID: "rt-1", Revision: nifi.Revision{Version: 2}, Component: nifi.ReportingTaskComponent{ID: "rt-1", State: "RUNNING"}}}
	r := &NiFiReportingTaskReconciler{Client: k8sClient, Scheme: scheme, ReportingTaskClient: tasks}

	if err := k8sClient.Delete(context.Background(), rt); err != nil {
		t.Fatal(err)
	}
	reconcileTwice(t, r, rt.Name)

	if len(tasks.runStatus) == 0 || tasks.runStatus[0] != "STOPPED" {
		t.Fatalf("a running task should be stopped before deletion: run-status calls = %#v", tasks.runStatus)
	}
	if len(tasks.deleted) != 1 || tasks.deleted[0] != "rt-1" {
		t.Fatalf("expected the task to be deleted: %#v", tasks.deleted)
	}
	got := &nifiv1alpha1.NiFiReportingTask{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: rt.Name, Namespace: "default"}, got); !apierrors.IsNotFound(err) {
		t.Fatalf("finalizer should be removed and the task deleted; got err=%v", err)
	}
}
