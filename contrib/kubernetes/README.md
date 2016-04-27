# Run Agro on K8s

## 1) Use my branch of `coreos-kubernetes`

```
git clone http://github.com/coreos/coreos-kubernetes
cd coreos-kubernetes
git remote add barakmich https://github.com/barakmich/coreos-kubernetes.git
git fetch barakmich
git checkout barakmich/agro-k8s
```

When this sets up a *multinode* cluster (using the `generic` scripts) it will pull the mount tool from S3

## 2) Set up your kubectl

???
I suppose Vagrant you ssh into the box? I always use my [10m-k8s](http://github.com/barakmich/10m-k8s) scripts. If there's a kube-aws way, that'd probably work too.

## 3) Launch all things agro on the cluster

```
kubectl create -f agro-k8s-oneshot.yaml
```

And you're done.

## 4) Connect, look around, create a volume

Grab the [latest release](https://github.com/coreos/agro/releases) and use the included `agroctl` tool. Or just build it from source.

```
agroctl -C $IP_IN_CLUSTER:32378 list-peers
```

Which should tell you everything about the cluster. Create a volume, eg:

```
agroctl -C $IP_IN_CLUSTER:32378 volume create-block pg1 2GiB
```

## 5) Run Postgres

And now use this volume in any other kubernetes pods, for example:

```
kubectl create -f postgres-oneshot.yaml
```
