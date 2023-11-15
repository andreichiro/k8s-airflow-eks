{{- $_ := set $ "secretNamesToKeys" dict -}}
{{- range $index, $helperDefinition := .Values -}}
{{- if eq $helperDefinition.Values.helperType "secretRef" -}}
{{- include "helperFunction.secretRef.validation" $helperDefinition -}}
{{- else -}}
{{- include "helperFunction.secretEnv.validation" $helperDefinition -}}
{{- end }}
{{- $secretName := ( index $helperDefinition.Values.helperArguments 0 ) -}}
{{- $_ := set $.secretNamesToKeys $secretName list -}}
{{- end -}}
{{- range $index, $helperDefinition := .Values -}}
{{- $secretName := ( index $helperDefinition.Values.helperArguments 0 ) -}}
{{- $secretKeys := ( index $.secretNamesToKeys $secretName ) -}}
{{- if eq (len $helperDefinition.Values.helperArguments) 2 -}}
{{- $_ := set $.secretNamesToKeys $secretName (( append $secretKeys ( index $helperDefinition.Values.helperArguments 1 )) | uniq) -}}
{{- end -}}
{{- end -}}
{{- range $index, $helperDefinition := .Values -}}
{{- $secretName := ( index $helperDefinition.Values.helperArguments 0 ) -}}
{{- $k8sSecretName := ( include "normalizer.secretName" $secretName ) -}}
{{- $secretKeys := ( index $.secretNamesToKeys $secretName ) -}}
{{- if hasPrefix $helperDefinition.Values.secretPrefix $secretName -}}
---
apiVersion: 'kubernetes-client.io/v1'
kind: ExternalSecret
metadata:
  name: {{ $k8sSecretName }}
spec:
  backendType: {{ $helperDefinition.Values.backendType }}
{{- if eq $helperDefinition.Values.helperType "secretRef" }}
  data:
    - key: {{ $secretName }}
      name: {{ index $secretKeys 0 }}
{{ else -}}
{{- if eq (len $secretKeys) 0 }}
  data:
    - key: {{ $secretName }}
      name: {{ include "normalizer.envName" $secretName }}
{{ else }}
  data:
{{- range $index, $secretKey := $secretKeys -}}
{{- $env_variable_name := (printf "%s_%s" $k8sSecretName $secretKey) }}
    - key: {{ $secretName }}
      name: {{ include "normalizer.envName" $env_variable_name }}
      property: {{ $secretKey }}
{{ end -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end }}
