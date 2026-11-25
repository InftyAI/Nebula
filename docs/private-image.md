# Private images

A Pod whose image needs a credential names it in `imagePullSecrets`, as for a kubelet. Nebula reads the Secret on the
provider's behalf, since the provider can't read the Pod's namespace. The first Secret that yields a credential for
the image's registry is used; if none does, the Pod isn't placed rather than pulled anonymously.

| Secret type | Credential | Modal | kubernetes |
|---|---|---|---|
| `nebula.inftyai.com/aws-role` | an IAM role the provider assumes, for a private ECR image | yes | no |
| `kubernetes.io/dockerconfigjson` | a registry username and password | yes | no |

The `kubernetes` provider takes neither: the real pod pulls with its node's IAM role, which must be allowed the
repository (see [multi-cluster.md](multi-cluster.md)).

## A private ECR image on Modal

Modal pulls the image itself, with an IAM role it assumes using its own OIDC token. So no AWS credential is stored
anywhere: the Secret names the role, and AWS lets Modal in because the role trusts Modal's tokens.

### 1. Trust Modal's OIDC tokens, once per AWS account

Register `oidc.modal.com` as an IAM OIDC identity provider:

```hcl
resource "aws_iam_openid_connect_provider" "modal" {
  url            = "https://oidc.modal.com"
  client_id_list = ["oidc.modal.com"]
}
```

### 2. Create a role Modal may assume

Its trust policy admits only tokens from your Modal workspace and, optionally, one Modal environment:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "Federated": "arn:aws:iam::<account>:oidc-provider/oidc.modal.com" },
    "Action": "sts:AssumeRoleWithWebIdentity",
    "Condition": {
      "StringEquals": { "oidc.modal.com:aud": "oidc.modal.com" },
      "StringLike":   { "oidc.modal.com:sub": "modal:workspace_id:<workspace>:environment_name:<environment>:*" }
    }
  }]
}
```

Only `sub` and `aud` can be conditioned on: AWS doesn't populate Modal's other claims. End the pattern with `:*`, and
never wildcard a segment inside it: `environment_name:prod*` would also match `prod-other`.

Its permissions allow pulling the repositories it serves, and nothing else:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Effect": "Allow", "Action": "ecr:GetAuthorizationToken", "Resource": "*" },
    { "Effect": "Allow",
      "Action": ["ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer", "ecr:BatchCheckLayerAvailability"],
      "Resource": "arn:aws:ecr:<region>:<account>:repository/<repository>" }
  ]
}
```

One role per set of repositories a tenant may read keeps tenants apart: a role can only pull what its own policy
allows, and only your workspace can assume it.

### 3. Name the role in a Secret, and the Secret in the Pod

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: my-image-pull
  namespace: <the Pod's namespace>
type: nebula.inftyai.com/aws-role
stringData:
  roleARN: arn:aws:iam::<account>:role/<role>
  region: <the registry's region>
---
apiVersion: v1
kind: Pod
spec:
  imagePullSecrets:
    - name: my-image-pull
  containers:
    - image: <account>.dkr.ecr.<region>.amazonaws.com/<repository>:<tag>
```

Both keys are required, and `region` is the registry's. Nothing in the Secret is secret: an ARN is an identifier, and a
Secret is used only because `imagePullSecrets` can reference nothing else.

### 4. What happens at placement

Nebula gives Modal the role as `AWS_ROLE_ARN` and `AWS_REGION` and builds the image with `Images.FromAwsEcr`. Modal
exchanges its OIDC token for the role's credentials (`sts:AssumeRoleWithWebIdentity`), for at most the role's maximum
session, and pulls.

### When it fails

The failure is in the Pod's events:

- `AccessDenied` from `AssumeRoleWithWebIdentity`: the trust. The OIDC provider is missing, or the token's workspace
  or environment doesn't match `sub`.
- `AccessDenied` from ECR: the role's permissions don't cover the repository.
- `needs a non-empty "roleARN" and "region"`: the Secret is incomplete.
