#!/bin/bash
set -e

export AWS_PAGER=""

CASTAI_API_URL="${CASTAI_API_URL:-https://api.cast.ai}"

kubectl get namespace castai-agent >/dev/null 2>&1
if [ $? -eq 1 ]; then
	echo "Cast AI namespace not found. Please run phase1 of the onboarding script first."
	exit 1
fi

if [ -z $CLUSTER_NAME ]; then
	echo "CLUSTER_NAME environment variable was not provided"
	exit 1
fi

if [ -z $REGION ]; then
	echo "REGION environment variable was not provided"
	exit 1
fi

if [ -z $USER_ARN ]; then
	echo "USER_ARN environment variable was not provided"
	exit 1
fi

if [ -z $CASTAI_API_TOKEN ] || [ -z $CASTAI_API_URL ] || [ -z $CASTAI_CLUSTER_ID ]; then
	echo "CASTAI_API_TOKEN, CASTAI_API_URL or CASTAI_CLUSTER_ID variables were not provided"
	exit 1
fi

if ! [ -x "$(command -v aws)" ]; then
	echo "Error: aws cli is not installed"
	exit 1
fi

if ! [ -x "$(command -v jq)" ]; then
	echo "Error: jq is not installed"
	exit 1
fi

if ! [ -x "$(command -v kubectl)" ]; then
	echo "Error: kubectl is not installed. (kubectl and helm are required to install castai-cluster-controller)"
	exit 1
fi

if ! [ -x "$(command -v helm)" ]; then
	echo "Error: helm is not installed. (kubectl and helm are required to install castai-cluster-controller)"
	exit 1
fi

create_security_group() {
	SG_NAME="cast-${CLUSTER_NAME}-cluster/CastNodeSecurityGroup"
	SG_ID=$(aws ec2 describe-security-groups --filters Name=vpc-id,Values=$CLUSTER_VPC Name=group-name,Values=$SG_NAME --region $REGION --query "SecurityGroups[*].GroupId" --output text)

	if [ -z $SG_ID ]; then
		echo "Creating new security group: '$SG_NAME'"
		SG_DESCRIPTION="CAST AI created security group that allows communication between CAST AI nodes"
		SG_TAGS="ResourceType=security-group,Tags=[{Key=Name,Value=${SG_NAME}},{Key=cast:cluster-id,Value=${CASTAI_CLUSTER_ID}}]"
		SG_ID=$(aws ec2 create-security-group --group-name $SG_NAME --description "${SG_DESCRIPTION}" --tag-specifications "${SG_TAGS}" --vpc-id $CLUSTER_VPC --region $REGION --output text --query 'GroupId')
	else
		echo "Security group already exists: '$SG_NAME'"
	fi

	# Add ingress and egress rules
	aws ec2 authorize-security-group-egress --group-id $SG_ID --region $REGION --protocol -1 --port all >>/dev/null 2>&1
	aws ec2 authorize-security-group-ingress --group-id $SG_ID --region $REGION --protocol -1 --port all --source-group $SG_ID >>/dev/null 2>&1 || true # ignore if rule already exist
}

echo "Fetching cluster information"
CLUSTER=$(aws eks describe-cluster --name "$CLUSTER_NAME" --region "$REGION" --output json)
CLUSTER_VPC=$(echo "$CLUSTER" | jq --raw-output '.cluster.resourcesVpcConfig.vpcId')

CURRENT_CONTEXT=$(kubectl config view --minify -o jsonpath='{.clusters[].name}')
echo "Checking current kubectl context"
if ! [[ "$CURRENT_CONTEXT" == *"$CLUSTER_NAME"* ]]; then
	echo "Error: the current kubectl context doesn't match the cluster. (kubectl config use-context my-cluster-name to select the correct context)"
	exit 1
fi

echo "Validating cluster access"
if ! kubectl describe cm/aws-auth --namespace=kube-system >>/dev/null 2>&1; then
	echo "Error: getting auth ConfigMap: Unauthorized"
	exit 1
fi

ROLE_NAME=cast-eks-${CLUSTER_NAME}-cluster-role-${CASTAI_CLUSTER_ID:0:8}
ACCOUNT_NUMBER=$(aws sts get-caller-identity --output text --query 'Account')
ARN="${REGION}:${ACCOUNT_NUMBER}"
ARN_PARTITION="aws"
if [[ $REGION == us-gov-* ]]; then
	ARN_PARTITION="aws-us-gov"
fi

INLINE_POLICY_JSON="{\"Version\":\"2012-10-17\",\"Statement\":[{\"Sid\":\"RunInstancesTagRestriction\",\"Effect\":\"Allow\",\"Action\":\"ec2:RunInstances\",\"Resource\":\"arn:${ARN_PARTITION}:ec2:${ARN}:instance/*\",\"Condition\":{\"StringEquals\":{\"aws:RequestTag/kubernetes.io/cluster/${CLUSTER_NAME}\":\"owned\"}}},{\"Sid\":\"RunInstancesVpcRestriction\",\"Effect\":\"Allow\",\"Action\":\"ec2:RunInstances\",\"Resource\":\"arn:${ARN_PARTITION}:ec2:${ARN}:subnet/*\",\"Condition\":{\"StringEquals\":{\"ec2:Vpc\":\"arn:${ARN_PARTITION}:ec2:${ARN}:vpc/${CLUSTER_VPC}\"}}},{\"Sid\":\"InstanceActionsTagRestriction\",\"Effect\":\"Allow\",\"Action\":[\"ec2:TerminateInstances\",\"ec2:StartInstances\",\"ec2:StopInstances\",\"ec2:CreateTags\"],\"Resource\":\"arn:${ARN_PARTITION}:ec2:${ARN}:instance/*\",\"Condition\":{\"StringEquals\":{\"ec2:ResourceTag/kubernetes.io/cluster/${CLUSTER_NAME}\":[\"owned\",\"shared\"]}}},{\"Sid\":\"AutoscalingActionsTagRestriction\",\"Effect\":\"Allow\",\"Action\":[\"autoscaling:UpdateAutoScalingGroup\",\"autoscaling:SuspendProcesses\",\"autoscaling:ResumeProcesses\",\"autoscaling:TerminateInstanceInAutoScalingGroup\"],\"Resource\":\"arn:${ARN_PARTITION}:autoscaling:${ARN}:autoScalingGroup:*:autoScalingGroupName/*\",\"Condition\":{\"StringEquals\":{\"autoscaling:ResourceTag/kubernetes.io/cluster/${CLUSTER_NAME}\":[\"owned\",\"shared\"]}}},{\"Sid\":\"EKS\",\"Effect\":\"Allow\",\"Action\":[\"eks:Describe*\",\"eks:List*\"],\"Resource\":[\"arn:${ARN_PARTITION}:eks:${ARN}:cluster/${CLUSTER_NAME}\",\"arn:${ARN_PARTITION}:eks:${ARN}:nodegroup/${CLUSTER_NAME}/*/*\"]}]}"
POLICY_JSON="{\"Version\":\"2012-10-17\",\"Statement\":[{\"Sid\":\"PassRoleEC2\",\"Action\":\"iam:PassRole\",\"Effect\":\"Allow\",\"Resource\":\"arn:${ARN_PARTITION}:iam::*:role/*\",\"Condition\":{\"StringEquals\":{\"iam:PassedToService\":\"ec2.amazonaws.com\"}}},{\"Sid\":\"NonResourcePermissions\",\"Effect\":\"Allow\",\"Action\":[\"iam:CreateServiceLinkedRole\",\"ec2:CreateKeyPair\",\"ec2:DeleteKeyPair\",\"ec2:CreateTags\",\"ec2:ImportKeyPair\"],\"Resource\":\"*\"},{\"Sid\":\"RunInstancesPermissions\",\"Effect\":\"Allow\",\"Action\":\"ec2:RunInstances\",\"Resource\":[\"arn:${ARN_PARTITION}:ec2:*:${ACCOUNT_NUMBER}:network-interface/*\",\"arn:${ARN_PARTITION}:ec2:*:${ACCOUNT_NUMBER}:security-group/*\",\"arn:${ARN_PARTITION}:ec2:*:${ACCOUNT_NUMBER}:volume/*\",\"arn:${ARN_PARTITION}:ec2:*:${ACCOUNT_NUMBER}:key-pair/*\",\"arn:${ARN_PARTITION}:ec2:*::image/*\"]}]}"
ASSUME_ROLE_POLICY_JSON='{"Version":"2012-10-17","Statement":{"Effect":"Allow","Principal":{"AWS":"'"$USER_ARN"'"},"Action":"sts:AssumeRole"}}'

if aws iam get-role --role-name $ROLE_NAME >>/dev/null 2>&1; then
	echo "Role already exists: '$ROLE_NAME'"
	ROLE_ARN=$(aws iam get-role --role-name $ROLE_NAME --output text --query 'Role.Arn')
	aws iam update-assume-role-policy --role-name $ROLE_NAME --policy-document $ASSUME_ROLE_POLICY_JSON
else
	echo "Creating new role: '$ROLE_NAME'"
	ROLE_ARN=$(aws iam create-role --role-name $ROLE_NAME --assume-role-policy-document $ASSUME_ROLE_POLICY_JSON --description "Role to manage '$CLUSTER_NAME' EKS cluster used by CAST AI" --output text --query 'Role.Arn')
fi

INSTANCE_PROFILE="cast-${CLUSTER_NAME:0:40}-eks-${CASTAI_CLUSTER_ID:0:8}"
if aws iam get-instance-profile --instance-profile-name $INSTANCE_PROFILE >>/dev/null 2>&1; then
	echo "Instance profile already exists: '$INSTANCE_PROFILE'"
	INSTANCE_ROLE_ARN=$(aws iam get-role --role-name $INSTANCE_PROFILE --output text --query 'Role.Arn')
	aws iam add-role-to-instance-profile --instance-profile-name $INSTANCE_PROFILE --role-name $INSTANCE_PROFILE >>/dev/null 2>&1 || true
else
	ASSUME_ROLE_JSON="{\"Version\":\"2012-10-17\",\"Statement\":[{\"Sid\":\"\",\"Effect\":\"Allow\",\"Principal\":{\"Service\":[\"ec2.amazonaws.com\"]},\"Action\":[\"sts:AssumeRole\"]}]}"

	if aws iam get-role --role-name $INSTANCE_PROFILE >>/dev/null 2>&1; then
		echo "Instance role already exists: '$INSTANCE_PROFILE'"
		INSTANCE_ROLE_ARN=$(aws iam get-role --role-name $INSTANCE_PROFILE --output text --query 'Role.Arn')
	else
		echo "Creating new instance role: '$INSTANCE_PROFILE'"
		INSTANCE_ROLE_ARN=$(aws iam create-role --role-name $INSTANCE_PROFILE --description 'EKS node instance role used by CAST AI' --assume-role-policy-document $ASSUME_ROLE_JSON --output text --query 'Role.Arn')
	fi

	echo "Attaching policies to the instance role: '$INSTANCE_PROFILE'"
	role_policies=(arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy)
	for i in "${role_policies[@]}"; do
		aws iam attach-role-policy --role-name $INSTANCE_PROFILE --policy-arn $i
	done

	echo "Creating new instance profile: '$INSTANCE_PROFILE'"
	aws iam create-instance-profile --instance-profile-name $INSTANCE_PROFILE >>/dev/null 2>&1
	echo "Adding role to new instance profile: '$INSTANCE_PROFILE'"
	aws iam add-role-to-instance-profile --instance-profile-name $INSTANCE_PROFILE --role-name $INSTANCE_PROFILE
fi

create_security_group

echo "Attaching policies to the role"
POLICY_ARN="arn:aws:iam::${ACCOUNT_NUMBER}:policy/CastEKSPolicy"
if aws iam get-policy --policy-arn $POLICY_ARN >>/dev/null 2>&1; then

	VERSIONS=$(aws iam list-policy-versions --policy-arn $POLICY_ARN --output text --query 'length(Versions[*])')
	if [ "$VERSIONS" -gt "4" ]; then
		LAST_VERSION_ID=$(aws iam list-policy-versions --policy-arn $POLICY_ARN --output text --query 'Versions[-1].VersionId')
		aws iam delete-policy-version --policy-arn $POLICY_ARN --version-id $LAST_VERSION_ID
	fi

	aws iam create-policy-version --policy-arn $POLICY_ARN --policy-document $POLICY_JSON --set-as-default >>/dev/null 2>&1
else
	POLICY_ARN=$(aws iam create-policy --policy-name CastEKSPolicy --policy-document $POLICY_JSON --description "Policy to manage EKS cluster used by CAST AI" --output text --query 'Policy.Arn')
fi

policies=(arn:aws:iam::aws:policy/AmazonEC2ReadOnlyAccess arn:aws:iam::aws:policy/IAMReadOnlyAccess $POLICY_ARN)
for i in "${policies[@]}"; do
	aws iam attach-role-policy --role-name $ROLE_NAME --policy-arn $i
done

aws iam put-role-policy --role-name $ROLE_NAME --policy-name CastEKSRestrictedAccess --policy-document $INLINE_POLICY_JSON

echo "Adding node role to cm/aws-auth: '$ROLE_ARN'"
CAST_NODE_ROLE="- rolearn: ${INSTANCE_ROLE_ARN}\n  username: system:node:{{EC2PrivateDNSName}}\n  groups:\n  - system:bootstrappers\n  - system:nodes\n"
AWS_CLUSTER_ROLES=$(kubectl get -n=kube-system cm/aws-auth -o json | jq '.data.mapRoles | select(. != null and . != "" and . != "[]" and . != "[]\n")' | sed -e 's/^"//' -e 's/"$//')
if [ -z "$AWS_CLUSTER_ROLES" ]; then
	kubectl patch -n=kube-system cm/aws-auth --patch "{\"data\":{\"mapRoles\": \"${CAST_NODE_ROLE}\"}}"
elif [[ "$AWS_CLUSTER_ROLES" == *"$CAST_NODE_ROLE"* ]]; then
	echo "Node role already exists in cm/aws-auth"
else
	kubectl patch -n=kube-system cm/aws-auth --patch "{\"data\":{\"mapRoles\": \"${AWS_CLUSTER_ROLES}${CAST_NODE_ROLE}\"}}"
fi

echo "Adding helm repositories for CAST AI required charts."
helm repo add castai-helm https://castai.github.io/helm-charts
if [[ $INSTALL_NVIDIA_DEVICE_PLUGIN = "true" ]]; then
	helm repo add nvdp https://nvidia.github.io/k8s-device-plugin
fi
helm repo update
echo "Finished adding helm charts repositories."

echo "Installing castai-cluster-controller."
helm upgrade -i cluster-controller castai-helm/castai-cluster-controller -n castai-agent \
	--set castai.apiKey=$CASTAI_API_TOKEN \
	--set castai.apiURL=$CASTAI_API_URL \
	--set castai.clusterID=$CASTAI_CLUSTER_ID
echo "Finished installing castai-cluster-controller."

echo "Installing castai-spot-handler."
helm upgrade -i castai-spot-handler castai-helm/castai-spot-handler -n castai-agent \
	--set castai.apiURL=$CASTAI_API_URL \
	--set castai.clusterID=$CASTAI_CLUSTER_ID \
	--set castai.provider=aws
echo "Finished installing castai-spot-handler."

echo "Installing castai-evictor."
helm upgrade -i castai-evictor castai-helm/castai-evictor -n castai-agent --set replicaCount=0
echo "Finished installing castai-evictor."

if [[ $INSTALL_NVIDIA_DEVICE_PLUGIN = "true" ]]; then
	echo "Installing NVIDIA device plugin required for GPU support."
	helm upgrade -i nvdp nvdp/nvidia-device-plugin -n castai-agent \
		--set-string nodeSelector."nvidia\.com/gpu"=true \
		--set \
		tolerations[0].key=CriticalAddonsOnly,tolerations[0].operator=Exists,tolerations[1].effect=NoSchedule,tolerations[1].key="nvidia\.com/gpu",tolerations[1].operator=Exists,tolerations[2].key="scheduling\.cast\.ai/spot",tolerations[2].operator=Exists,tolerations[3].key="scheduling\.cast\.ai/scoped-autoscaler",tolerations[3].operator=Exists,tolerations[4].key="scheduling\.cast\.ai/node-template",tolerations[4].operator=Exists
	echo "Finished installing NVIDIA device plugin."
fi

if [[ $INSTALL_KVISOR = "true" ]]; then
	helm upgrade -i castai-kvisor castai-helm/castai-kvisor -n castai-agent \
		--set castai.apiURL=$CASTAI_API_URL \
		--set castai.apiKey=$CASTAI_API_TOKEN \
		--set castai.clusterID=$CASTAI_CLUSTER_ID \
		--set structuredConfig.provider=eks
	echo "Finished installing castai-kvisor."
fi

echo "Role ARN: ${ROLE_ARN}"
API_URL="${CASTAI_API_URL}/v1/kubernetes/external-clusters/${CASTAI_CLUSTER_ID}"
BODY='{"eks": { "assumeRoleArn": "'"$ROLE_ARN"'" }}'

echo "Sending role ARN to CAST AI console..."
RESPONSE=$(curl -sSL --write-out "HTTP_STATUS:%{http_code}" -X POST -H "X-API-Key: ${CASTAI_API_TOKEN}" -d "${BODY}" $API_URL)
RESPONSE_STATUS=$(echo "$RESPONSE" | tr -d '\n' | sed -e 's/.*HTTP_STATUS://')
RESPONSE_BODY=$(echo "$RESPONSE" | sed -e 's/HTTP_STATUS\:.*//g')

if [[ $RESPONSE_STATUS -eq 200 ]]; then
	echo "Successfully sent."
else
	echo "Couldn't save role ARN to CAST AI console. Try updating cluster role ARN manually."
	echo "Error details: status=$RESPONSE_STATUS content=$RESPONSE_BODY"
	exit 1
fi

echo "Scaling castai-agent:"
kubectl scale deployments/castai-agent --replicas=2 --namespace castai-agent