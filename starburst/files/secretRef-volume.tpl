{{- range $index, $helperDefinition := .Values -}}
{{- include "helperFunction.secretRef.validation" $helperDefinition -}}
{{- $secretName := ( index $helperDefinition.Values.helperArguments 0 ) -}}
{{- $k8sSecretName := ( include "normalizer.secretName" $secretName ) -}}
{{- $secretKey := ( index $helperDefinition.Values.helperArguments (sub (len $helperDefinition.Values.helperArguments) 1) ) -}}
- name: "secretref-{{ $helperDefinition.Values.helperId }}"
  secret:
    secretName: {{ $k8sSecretName }}
{{ end -}}
