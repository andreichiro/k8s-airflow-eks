{{- $_ := set $ "k8sSecretNameList" list -}}
{{- range $index, $helperDefinition := .Values -}}
{{- include "helperFunction.secretEnv.validation" $helperDefinition -}}
{{- $secretName := ( index $helperDefinition.Values.helperArguments 0 ) -}}
{{- $k8sSecretName := ( include "normalizer.secretName" $secretName ) -}}
{{- $_ := set $ "k8sSecretNameList" (append $.k8sSecretNameList $k8sSecretName) -}}
{{ end -}}
{{- range $index, $k8sSecretName := ($.k8sSecretNameList | uniq) -}}
- secretRef:
    name: "{{ $k8sSecretName }}"
{{ end -}}
