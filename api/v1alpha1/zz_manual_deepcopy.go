package v1alpha1

import "k8s.io/apimachinery/pkg/runtime"

func (in *NiFiCluster) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	out := new(NiFiCluster)
	*out = *in
	out.ObjectMeta = *in.ObjectMeta.DeepCopy()
	return out
}

func (in *NiFiClusterList) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	out := new(NiFiClusterList)
	*out = *in
	out.ListMeta = in.ListMeta
	if in.Items != nil {
		out.Items = append([]NiFiCluster(nil), in.Items...)
	}
	return out
}

func (in *NiFiRegistryClient) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	out := new(NiFiRegistryClient)
	*out = *in
	out.ObjectMeta = *in.ObjectMeta.DeepCopy()
	return out
}

func (in *NiFiRegistryClientList) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	out := new(NiFiRegistryClientList)
	*out = *in
	if in.Items != nil {
		out.Items = append([]NiFiRegistryClient(nil), in.Items...)
	}
	return out
}

func (in *NiFiParameterContext) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	out := new(NiFiParameterContext)
	*out = *in
	out.ObjectMeta = *in.ObjectMeta.DeepCopy()
	return out
}

func (in *NiFiParameterContextList) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	out := new(NiFiParameterContextList)
	*out = *in
	if in.Items != nil {
		out.Items = append([]NiFiParameterContext(nil), in.Items...)
	}
	return out
}

func (in *NiFiControllerService) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	out := new(NiFiControllerService)
	*out = *in
	out.ObjectMeta = *in.ObjectMeta.DeepCopy()
	return out
}

func (in *NiFiControllerServiceList) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	out := new(NiFiControllerServiceList)
	*out = *in
	if in.Items != nil {
		out.Items = append([]NiFiControllerService(nil), in.Items...)
	}
	return out
}

func (in *NiFiFlowBundle) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	out := new(NiFiFlowBundle)
	*out = *in
	out.ObjectMeta = *in.ObjectMeta.DeepCopy()
	return out
}

func (in *NiFiFlowBundleList) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	out := new(NiFiFlowBundleList)
	*out = *in
	if in.Items != nil {
		out.Items = append([]NiFiFlowBundle(nil), in.Items...)
	}
	return out
}

func (in *NiFiFlowDeployment) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	out := new(NiFiFlowDeployment)
	*out = *in
	out.ObjectMeta = *in.ObjectMeta.DeepCopy()
	return out
}

func (in *NiFiFlowDeploymentList) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	out := new(NiFiFlowDeploymentList)
	*out = *in
	if in.Items != nil {
		out.Items = append([]NiFiFlowDeployment(nil), in.Items...)
	}
	return out
}
