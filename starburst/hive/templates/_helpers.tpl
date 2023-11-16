{{/* -------------------------------------------------------------------------------------------------------------- */}}
{{/* Hive                                                                                                            */}}
{{/* -------------------------------------------------------------------------------------------------------------- */}}

{{- define "hive.config-checksums" -}}
checksum/registry-secret: {{ include (print $.Template.BasePath "/registry-secret.yaml") . | sha256sum }}
checksum/init-file: {{ include (print $.Template.BasePath "/initfile/init-file-secret.yaml") . | sha256sum }}
{{- end -}}

{{- define "hive.database.internal.scheme" -}}
{{- $driver := .Values.database.internal.driver -}}
{{- if contains "postgresql" $driver -}}
jdbc:postgresql
{{- else if contains "mysql" $driver -}}
jdbc:mysql
{{- else if contains "mariadb" $driver -}}
jdbc:mariadb
{{- end -}}
{{- end -}}

{{/* Calculate heap size */}}
{{- define "hive.heap" -}}
{{- $memSize := include "memory-to-kb" .Values.resources.limits.memory -}}
{{- $heapSize := (div (mul $memSize .Values.heapSizePercentage) 100) -}}
{{- $heapSize -}}
{{- end }}
