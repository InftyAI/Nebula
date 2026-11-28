/*
Copyright 2026 The InftyAI Team.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import corev1 "k8s.io/api/core/v1"

// The component label on logship's relay Pod. Log consumers use it to recognize records
// that wrap another Pod's output.
const (
	LogshipComponentLabel = "app.kubernetes.io/component"
	LogshipComponentValue = "logship"
)

// SecretTypeAWSRole is the type of an imagePullSecret that names an AWS role to pull
// private ECR images with.
const SecretTypeAWSRole corev1.SecretType = "nebula.inftyai.com/aws-role"

// The keys of a SecretTypeAWSRole Secret, both required. The region is the ECR
// registry's, not the workload's.
const (
	AWSRoleARNKey    = "roleARN"
	AWSRoleRegionKey = "region"
)
