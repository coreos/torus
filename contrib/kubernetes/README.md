# Run Torus on Kubernetes

Running Torus on a kubernetes cluster is as easy as running the included `torus-k8s-oneshot.yaml`. However, this will not install the FlexVolume plugin to allow you to use Torus as a volume provider for other Kubernetes pods.

## Installing a new Torus-enabled Kubernetes on CoreOS (Vagrant, KubeAWS, other services)

### 1) Use my branch of `coreos-kubernetes`

```
git clone http://github.com/coreos/coreos-kubernetes
cd coreos-kubernetes
git remote add barakmich https://github.com/barakmich/coreos-kubernetes.git
git fetch barakmich
git checkout barakmich/torus-k8s
```

When this sets up a *multinode* cluster (using the `generic` scripts) it will pull the mount tool from S3

### 2) Set up your kubectl

[On Vagrant, use the kube-config](https://coreos.com/kubernetes/docs/latest/kubernetes-on-vagrant.html). The [10m-k8s](https://github.com/barakmich/10m-k8s) scripts are helpful for other cloud providers.

### 3) Launch all things torus on the cluster

```
kubectl create -f torus-k8s-oneshot.yaml
```

And you're done.

### 4) Connect, look around

Grab the [latest release](https://github.com/coreos/torus/releases) and use the included `torusctl` tool. Or just build it from source.

```
torusctl -C $IP_IN_CLUSTER:32379 list-peers
```

Which should tell you everything about the cluster. 

### 5) Create a volume

Create a volume, eg:

```
torusblk -C $IP_IN_CLUSTER:32379 volume create pg1 2GiB
```

### 6) Run Postgres

And now use this volume in any other kubernetes pods, for example:

```
kubectl create -f postgres-oneshot.yaml
```

### 7) Put some data into postgres

```
TORUSPOD=$(kubectl get pods -l app=postgres-torus -o name | cut -d/ -f2)
kubectl exec $TORUSPOD -- psql postgres -U postgres < test-data.sql
kubectl exec $TORUSPOD -- psql postgres -U postgres -c 'select * from films'
```

### 8) Move postgres to another node

First lets cordon off the node postgres is currently on, so that when we kill
it, it doesn't go to the same node.
```
PGNODE=$(kubectl get pods -l app=postgres-torus -o jsonpath='{.items[0].spec.nodeName}')
kubectl cordon $PGNODE
kubectl get nodes
```

Node we will delete the existing postgres pod, and then watch for a new one
to come up and replace it
```
kubectl delete pod -l app=postgres-torus
kubectl get pod -l app=postgres-torus -w
```

You should see some output similar to
```
$ kubectl get pods -w -l app=postgres-torus
NAME                             READY     STATUS              RESTARTS   AGE
postgres-torus-1844296455-6z492   1/1       Terminating         1          8m
postgres-torus-1844296455-mv6v9   0/1       ContainerCreating   0          13s
NAME                             READY     STATUS        RESTARTS   AGE
postgres-torus-1844296455-6z492   0/1       Terminating   1          9m
postgres-torus-1844296455-6z492   0/1       Terminating   1         9m
postgres-torus-1844296455-6z492   0/1       Terminating   1         9m
postgres-torus-1844296455-mv6v9   1/1       Running   0         1m
```

Finally we can verify that the data is still there:

```
TORUSPOD=$(kubectl get pods -l app=postgres-torus -o name | cut -d/ -f2)
kubectl exec $TORUSPOD -- psql postgres -U postgres -c 'select * from films'
```

Lastly, let's uncordon that node we cordoned in the beginning:

```
kubectl uncordon $PGNODE
```

## Installing the Torus FlexVolume plugin on generic Kubernetes installations

Kubernetes v1.2 supports FlexVolumes by placing a plugin binary in a specific location. By default, that is

```
/usr/libexec/kubernetes/kubelet-plugins/volume/exec/
```

The `torusblk` tool already conforms to this interface, so it's a simple matter of naming it correctly and placing it correctly.

```
mkdir -p /usr/libexec/kubernetes/kubelet-plugins/volume/exec/coreos.com~torus/
cp torusblk /usr/libexec/kubernetes/kubelet-plugins/volume/exec/coreos.com~torus/torus
```

Notice that the `cp` command renames `torusblk` as `torus` in the target directory.

After that, restart the kubelet (ie, `systemctl restart kubelet`, or `/etc/init.d/kubelet restart`) -- and the plugin is ready.
