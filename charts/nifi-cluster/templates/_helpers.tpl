{{- define "nifi-cluster.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "nifi-cluster.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{- define "nifi-cluster.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/name: {{ include "nifi-cluster.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Render one operator CR from a values-list item {name, spec, [annotations], [labels]}. The spec
is passed through verbatim; for kinds that take a clusterRef, this chart's own NiFiCluster is
injected when the item omits it, so it only has to be set once (on the cluster). Call with a
dict: {root, kind, item, injectClusterRef}.
*/}}
{{- define "nifi-cluster.resource" -}}
{{- $ := .root -}}
{{- $spec := .item.spec | default dict -}}
---
apiVersion: nifi.controlnifi.io/v1alpha1
kind: {{ .kind }}
metadata:
  name: {{ required "each resource list item needs a name" .item.name }}
  namespace: {{ $.Release.Namespace }}
  labels:
    {{- include "nifi-cluster.labels" $ | nindent 4 }}
    {{- with .item.labels }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
  {{- with .item.annotations }}
  annotations:
    {{- toYaml . | nindent 4 }}
  {{- end }}
spec:
  {{- if and .injectClusterRef (not (hasKey $spec "clusterRef")) }}
  clusterRef:
    name: {{ include "nifi-cluster.fullname" $ }}
  {{- end }}
  {{- toYaml $spec | nindent 2 }}
{{- end }}
