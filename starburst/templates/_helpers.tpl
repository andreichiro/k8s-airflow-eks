{{/* -------------------------------------------------------------------------------------------------------------- */}}
{{/* Secrets
{{/* -------------------------------------------------------------------------------------------------------------- */}}

{{/* Secret names are valid domain names. But we added extra charactes */}}
{{/* - '/' - to allow reference external secrets using slashes */}}
{{/*  */}}
{{/* This requires removing those slashes in Secret templates name field */}}
{{/* This operation is centralized in "normalizer.secretName" nested template */}}
{{- define "secretNameRegExp" -}}
^[a-z0-9]([-/a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-/a-z0-9]*[a-z0-9])?)*$
{{- end -}}

{{- define "fileNameRegExp" -}}
^[a-zA-Z0-9._-]+$
{{- end -}}

{{- define "envVarNameRegExp" -}}
^[a-zA-Z][a-zA-Z0-9_]*$
{{- end -}}

{{- define "validator.secretName" -}}
{{- if regexMatch (include "secretNameRegExp" .) . -}}
1
{{- end -}}
{{- end -}}

{{- define "validator.fileName" -}}
{{- if regexMatch (include "fileNameRegExp" .) . -}}
1
{{- end -}}
{{- end -}}

{{- define "validator.envVariable" -}}
{{- if regexMatch (include "envVarNameRegExp" .) . -}}
1
{{- end -}}
{{- end -}}

{{/* Secret added possiblity to use slashes in secret names to be compatible */}}
{{/* with popular naming strategy in KeyVaults. This requires cleancing them */}}
{{/* before storing them as K8s Secrets which names need to be valid domains. */}}
{{/* This operation is done in below nested template */}}
{{- define "normalizer.secretName" -}}
{{- regexReplaceAll "/" . "." -}}
{{- end -}}

{{/* Environment variables build from external secrets are build from */}}
{{/* concatenation of secret name and actual property name within secret. */}}
{{/* Thaks to that we avoid overriding same properties from diff. secrets */}}
{{/* This requires cleancing effectiv env. variable name from illegal characters */}}
{{/* it's done in below nested template */}}
{{- define "normalizer.envName" -}}
{{- regexReplaceAll "[-./]" ( upper . ) "_" -}}
{{- end -}}

{{/* -------------------------------------------------------------------------------------------------------------- */}}
{{/* Helper functions                                                                                               */}}
{{/* -------------------------------------------------------------------------------------------------------------- */}}

{{- define "helperFunction.renderer" -}}
{{- $helperFunctionsRemovedComments := regexReplaceAllLiteral "#.*\\n" (toYaml .Values.helperFunctionResolvePipeline) "" -}}
{{- $helperFunctionMatchingRegexp := (printf "%s(:[^ \n]+)*" .Values.helperFunctionName) -}}
{{- $helperFunctionsCalls := regexFindAll $helperFunctionMatchingRegexp $helperFunctionsRemovedComments -1 | uniq -}}
{{- $_ := set . "helperFunctions" list -}}
{{- range $index, $helperFunctionCall := $helperFunctionsCalls -}}
{{- if $helperFunctionCall -}}
{{- $helperFunctionArray := splitList ":" $helperFunctionCall -}}
{{- $valuesCopy := (deepCopy $.Values) -}}
{{- $_ := set $valuesCopy "helperId" $index -}}
{{- $_ := set $valuesCopy "helperType" ( index $helperFunctionArray 0 ) -}}
{{- $_ := set $valuesCopy "helperArguments" ( without $helperFunctionArray $valuesCopy.helperType  ) -}}
{{- $helperDefinition := dict -}}
{{- $_ := set $helperDefinition "Values" $valuesCopy -}}
{{- $_ := set $ "helperFunctions" (append $.helperFunctions $helperDefinition) -}}
{{- end -}}
{{- end -}}
{{ tpl .Values.helperFunctionTemplate (dict "Values" $.helperFunctions "Template" .Template) }}
{{ end -}}

{{/* -------------------------------------------------------------------------------------------------------------- */}}
{{/* Helper function secretRef                                                                                      */}}

{{- define "helperFunction.secretRef.volumes.coordinator" -}}
{{- $arguments := dict -}}
{{- $_ := set $arguments "helperFunctionResolvePipeline" (dict "etcFiles" .Values.coordinator.etcFiles "additionalProperties" .Values.coordinator.additionalProperties "catalogs" .Values.catalogs) -}}
{{- $_ := set $arguments "helperFunctionTemplate" (.Files.Get "files/secretRef-volume.tpl") -}}
{{- $_ := set $arguments "helperFunctionName" "secretRef" -}}
{{- include "helperFunction.renderer" (dict "Values" $arguments "Template" $.Template) -}}
{{- end -}}

{{- define "helperFunction.secretRef.volumes.worker" -}}
{{- $arguments := dict -}}
{{- $_ := set $arguments "helperFunctionResolvePipeline" (dict "etcFiles" .Values.worker.etcFiles "additionalProperties" .Values.worker.additionalProperties "catalogs" .Values.catalogs) -}}
{{- $_ := set $arguments "helperFunctionTemplate" (.Files.Get "files/secretRef-volume.tpl") -}}
{{- $_ := set $arguments "helperFunctionName" "secretRef" -}}
{{- include "helperFunction.renderer" (dict "Values" $arguments "Template" $.Template) -}}
{{- end -}}

{{- define "helperFunction.secretRef.volumesMounts.coordinator" -}}
{{- $arguments := dict -}}
{{- $_ := set $arguments "helperFunctionResolvePipeline" (dict "etcFiles" .Values.coordinator.etcFiles "additionalProperties" .Values.coordinator.additionalProperties "catalogs" .Values.catalogs) -}}
{{- $_ := set $arguments "helperFunctionTemplate" (.Files.Get "files/secretRef-volume-mount.tpl") -}}
{{- $_ := set $arguments "helperFunctionName" "secretRef" -}}
{{- include "helperFunction.renderer" (dict "Values" $arguments "Template" $.Template) -}}
{{- end -}}

{{- define "helperFunction.secretRef.volumesMounts.worker" -}}
{{- $arguments := dict -}}
{{- $_ := set $arguments "helperFunctionResolvePipeline" (dict "etcFiles" .Values.worker.etcFiles "additionalProperties" .Values.worker.additionalProperties "catalogs" .Values.catalogs) -}}
{{- $_ := set $arguments "helperFunctionTemplate" (.Files.Get "files/secretRef-volume-mount.tpl") -}}
{{- $_ := set $arguments "helperFunctionName" "secretRef" -}}
{{- include "helperFunction.renderer" (dict "Values" $arguments "Template" $.Template) -}}
{{- end -}}

{{- define "helperFunction.secretRef.validation" -}}
{{- $helperArgumentsSize := len $.Values.helperArguments -}}
{{- if not (eq $helperArgumentsSize 2) -}}
{{- fail (printf "\n!!! Invalid HelperFunction definition: secretRef%s !!!\nCause: Not matching to secretRef:<<secret_name>>:<<secret_key>>" $.Values.helperArguments ) -}}
{{- end -}}
{{- $secretName := ( index $.Values.helperArguments 0 ) -}}
{{- if not (include "validator.secretName" $secretName) -}}
{{- fail (printf "\nInvalid HelperFunction definition: secretRef%s !!!\nCause: Secret name '%s' does not match RegExp: %s" $.Values.helperArguments $secretName (include "secretNameRegExp" .)) -}}
{{- end -}}
{{- $fileName := ( index $.Values.helperArguments (sub (len $.Values.helperArguments) 1) ) -}}
{{- if not (include "validator.fileName" $fileName) -}}
{{- fail (printf "\nInvalid HelperFunction definition: secretRef%s !!!\nCause: File name '%s' does not match RegExp: %s" $.Values.helperArguments $fileName (include "fileNameRegExp" .)) -}}
{{- end -}}
{{- end -}}

{{/* -------------------------------------------------------------------------------------------------------------- */}}
{{/* Helper function secretEnv                                                                                      */}}

{{- define "helperFunction.secretEnv.coordinator" -}}
{{- $arguments := dict -}}
{{- $_ := set $arguments "helperFunctionResolvePipeline" (dict "etcFiles" .Values.coordinator.etcFiles "additionalProperties" .Values.coordinator.additionalProperties "catalogs" .Values.catalogs) -}}
{{- $_ := set $arguments "helperFunctionTemplate" (.Files.Get "files/secretEnv.tpl") -}}
{{- $_ := set $arguments "helperFunctionName" "secretEnv" -}}
{{- include "helperFunction.renderer" (dict "Values" $arguments "Template" $.Template) -}}
{{- end -}}

{{- define "helperFunction.secretEnv.worker" -}}
{{- $arguments := dict -}}
{{- $_ := set $arguments "helperFunctionResolvePipeline" (dict "etcFiles" .Values.worker.etcFiles "additionalProperties" .Values.worker.additionalProperties "catalogs" .Values.catalogs) -}}
{{- $_ := set $arguments "helperFunctionTemplate" (.Files.Get "files/secretEnv.tpl") -}}
{{- $_ := set $arguments "helperFunctionName" "secretEnv" -}}
{{- include "helperFunction.renderer" (dict "Values" $arguments "Template" $.Template) -}}
{{- end -}}

{{- define "helperFunction.secretEnv.validation" -}}
{{- $helperArgumentsSize := len $.Values.helperArguments -}}
{{- if not (and (ge $helperArgumentsSize 1) (le $helperArgumentsSize 2)) -}}
{{- fail (printf "\nInvalid HelperFunction definition: secretEnv%s !!!\nCause: Not matching to secretEnv:<<secret_name>>(:<<secret_key>>)?" $.Values.helperArguments ) -}}
{{- end -}}
{{- $secretName := ( index $.Values.helperArguments 0 ) -}}
{{- if not (include "validator.secretName" $secretName) -}}
{{- fail (printf "\nInvalid HelperFunction definition: secretEnv%s !!!\nCause: Secret name '%s' does not match RegExp: %s" $.Values.helperArguments $secretName (include "secretNameRegExp" .)) -}}
{{- end -}}
{{- if eq $helperArgumentsSize 2 -}}
{{- $envVariableName := ( index $.Values.helperArguments 1 ) -}}
{{- if not (include "validator.envVariable" $envVariableName) -}}
{{- fail (printf "\nInvalid HelperFunction definition: secretEnv%s !!!\nCause: Environment variable name '%s' does not match RegExp: %s" $.Values.helperArguments $envVariableName (include "envVarNameRegExp" .)) -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/* -------------------------------------------------------------------------------------------------------------- */}}
{{/* Starburst Enterprise                                                                                           */}}
{{/* -------------------------------------------------------------------------------------------------------------- */}}

{{- define "starburst.config-checksums" -}}
checksum/pullsecret: {{ include (print $.Template.BasePath "/security/pullsecret.yaml") . | sha256sum }}
{{- end -}}

{{- define "starburst.worker-checksums" -}}
{{/* Checksum used in Worker pod annotations is based on Chart values yaml except Coordinator specific settings --- */}}
{{/* and parameters controlling Trino Worker scaling which shouldn't trigger Worker pods restart. ----------------- */}}
{{- $valuesForHash := deepCopy .Values -}}
{{- $_ := unset $valuesForHash "coordinator" -}}
{{- $_ := unset $valuesForHash.worker "autoscaling" -}}
{{- $_ := unset $valuesForHash.worker "count" -}}
{{- $_ := unset $valuesForHash.worker "replicas" -}}
checksum/worker-values: {{ print (tpl (toYaml $valuesForHash) $) | sha256sum }}
{{- end -}}

{{- define "starburst.coordinator-checksums" -}}
{{/* Checksum used in Coordinator pod annotations is based on Chart values yaml --------------------------------------*/}}
{{/* except Worker specific settings which shouldn't trigger Coordinator pod restart. --------------------------------*/}}
{{- $valuesForHash := deepCopy .Values -}}
{{- $_ := unset $valuesForHash "worker" -}}
checksum/coordinator-values: {{ print (tpl (toYaml $valuesForHash) $) | sha256sum }}
{{- end -}}

{{/* Services ----------------------------------------------------------------------------------------------------- */}}

{{- define "starburst.service.name" -}}
{{- if eq .Values.expose.type "ingress" -}}
{{- .Values.expose.ingress.serviceName -}}
{{- else if eq .Values.expose.type "clusterIp" -}}
{{- .Values.expose.clusterIp.name -}}
{{- else if eq .Values.expose.type "loadBalancer" -}}
{{- .Values.expose.loadBalancer.name -}}
{{- else if eq .Values.expose.type "nodePort" -}}
{{- .Values.expose.nodePort.name -}}
{{- else -}}
{{- printf "%s-%s" "starburst-coordinator" (default .Release.Name .Values.nameOverride) | trunc 63 -}}
{{- end -}}
{{- end -}}

{{- define "prometheus.coordinator.service.name" -}}
{{ default (printf "%s-%s" "prometheus-coordinator" (default .Release.Name .Values.nameOverride)) | trunc 63 }}
{{- end -}}

{{- define "prometheus.worker.service.name" -}}
{{ default (printf "%s-%s" "prometheus-worker" (default .Release.Name .Values.nameOverride)) | trunc 63 }}
{{- end -}}

{{/* This function must be consistent with method formatEnvironment from EnvironmentProcessor in init container! */}}
{{- define "starburst.environment" -}}
{{ .Values.environment | default .Values.nameOverride | default .Release.Name | replace "+" "" | replace "-" "" | replace "_" "" }}
{{- end -}}

{{- define "starburst.service.environment.validate" -}}
{{- $names := list "coordinator" "worker" (include "starburst.service.name" .) }}
{{- $invalid := has (include "starburst.environment" .) $names }}
{{- if $invalid }}
{{- fail (printf "\n\n!!! Invalid environment name: %s. Cannot be one of these %s" .Values.environment $names  ) -}}
{{- end }}
{{- end -}}

{{- define "starburst.startup.probe" -}}
{{- $scheme := .Values.internalTls | ternary "HTTPS" "HTTP" -}}
{{- $port := .Values.internalTls | ternary .Values.internal.ports.https.port .Values.internal.ports.http.port -}}
httpGet:
  scheme: {{ $scheme }}
  path: /v1/readiness
  port: {{ $port }}
failureThreshold: 40
periodSeconds: 10
{{- end -}}

{{- define "starburst.readiness.probe" -}}
{{- $scheme := .Values.internalTls | ternary "HTTPS" "HTTP" -}}
{{- $port := .Values.internalTls | ternary .Values.internal.ports.https.port .Values.internal.ports.http.port -}}
httpGet:
  scheme: {{ $scheme }}
  path: /v1/readiness
  port: {{ $port }}
timeoutSeconds: 5
periodSeconds: 10
failureThreshold: 3
{{- end -}}

{{- define "starburst.liveness.probe" -}}
{{- $scheme := .Values.internalTls | ternary "HTTPS" "HTTP" -}}
{{- $port := .Values.internalTls | ternary .Values.internal.ports.https.port .Values.internal.ports.http.port -}}
httpGet:
  scheme: {{ $scheme }}
  path: /v1/info
  port: {{ $port }}
timeoutSeconds: 5
periodSeconds: 60
failureThreshold: 3
{{- end -}}

{{/* Add volumeMounts defined under .Values.additionalVolumes to container spec */}}
{{- define "starburst.process.container" -}}
{{- $context := .context -}}
{{- range .value -}}
{{- $volumeMounts := .volumeMounts | default dict -}}
{{- print "\n- name: " (get . "name") }}
{{- unset (unset . "volumeMounts") "name" | toYaml | nindent 2 }}
{{- print "volumeMounts:" | nindent 2 }}
{{- include "app.volumeMounts" $context | indent 2 }}
{{- if $volumeMounts -}}
{{- $volumeMounts | toYaml | nindent 2 }}
{{- end -}}
{{- end -}}
{{- end -}}
