# NodePool status printer column

## Context

`NodePool.status.conditions` already reports whether a pool can be used. The
controller owns a standard `Ready` condition and sets it to `True` for a valid
pool or `False` when an environment-dependent validation, such as provider
registration, fails. However, the default `kubectl get nodepools` table does not
show that signal, so operators must request the full object or write a JSONPath.

## Decision

Add a `Status` CRD printer column whose JSONPath selects the status of the
`Ready` condition:

```text
.status.conditions[?(@.type=="Ready")].status
```

The column is derived directly by the Kubernetes API server when it renders the
table. No duplicate status field or controller change is introduced. This keeps
the condition as the single source of truth and uses the standard condition
values `True`, `False`, and `Unknown`.

The column appears before policy details so pool health is visible immediately:

```text
NAME       STATUS   STRATEGY   PROVIDERS       AGE
gpu-pool   True     Ordered    modal,runpod    2m
```

Before the controller has written the `Ready` condition, the table cell has no
value. This is preferable to manufacturing a fourth status value because absence
already means the controller has not observed the object.

## Compatibility and rollout

This is an additive change to `additionalPrinterColumns`; the stored and served
resource schema is unchanged. Existing clients that read `NodePool` objects are
unaffected. Installing the regenerated CRD is sufficient to enable the column
for existing pools, and the next `kubectl get` uses their existing conditions.

## Verification

Generation is checked into `config/crd/bases`. Regenerating the manifests keeps
the CRD printer column aligned with the marker in `nodepool_types.go`.
