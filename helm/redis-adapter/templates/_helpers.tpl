{{/*
Chart fullname
*/}}
{{- define "redis-adapter.fullname" -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "redis-adapter.labels" -}}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/instance: {{ .Release.Name }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end }}

{{/*
Resolve profile counts
*/}}
{{- define "redis-adapter.bpmCount" -}}
{{- $p := index .Values.profiles .Values.profile -}}
{{- $p.bpm -}}
{{- end }}

{{- define "redis-adapter.blmCount" -}}
{{- $p := index .Values.profiles .Values.profile -}}
{{- $p.blm -}}
{{- end }}

{{- define "redis-adapter.bcmCount" -}}
{{- $p := index .Values.profiles .Values.profile -}}
{{- $p.bcm -}}
{{- end }}

{{/*
Redis count helpers (ceiling division)
BPM: 1:1, BLM: 8:1, BCM: 4:1
*/}}
{{- define "redis-adapter.bpmRedisCount" -}}
{{- include "redis-adapter.bpmCount" . -}}
{{- end }}

{{- define "redis-adapter.blmRedisCount" -}}
{{- $p := index .Values.profiles .Values.profile -}}
{{- div (add $p.blm 7) 8 -}}
{{- end }}

{{- define "redis-adapter.bcmRedisCount" -}}
{{- $p := index .Values.profiles .Values.profile -}}
{{- div (add $p.bcm 3) 4 -}}
{{- end }}
