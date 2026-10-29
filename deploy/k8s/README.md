# Kubernetes manifests

```
base/                 -- Deployment, Service, ConfigMap, Secret, PDB, HPA
overlays/local/        -- kind/k3d overlay: dev namespace, smaller replica
                          counts, IfNotPresent pull policy
```

## Rendering / validating

```sh
kubectl kustomize deploy/k8s/base
kubectl kustomize deploy/k8s/overlays/local
kubectl apply --dry-run=client -k deploy/k8s/overlays/local
```

## Local (kind/k3d) usage

The `logengine-api:dev` image referenced by the local overlay is not
pulled from a registry -- build it and load it into the cluster directly:

```sh
docker buildx build -f deploy/docker/Dockerfile --platform linux/arm64 \
  -t logengine-api:dev --load .
kind load docker-image logengine-api:dev
kubectl apply -k deploy/k8s/overlays/local
```
