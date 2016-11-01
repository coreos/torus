# Run torusd with systemd

### Prerequisites

We assume that `torusd` is installed to `/usr/bin/torusd`.

### Configuration

#### 1) copy torusd.service to `/etc/systemd/system/torusd.service`

~~~
cp  ./torusd.service /etc/systemd/system/torusd.service
~~~

#### 2) copy torusd to `/etc/sysconfig/torusd`

~~~
cp  ./torusd /etc/systemd/system/torusd.service
~~~

#### 3) Edit `/etc/sysconfig/torusd`

`--peer-address` and `--etcd` would be the mandatory configuration to use on your environment.

~~~
TORUSD_PEER_ADDRESS="--peer-address=http://<EDIT_TO_YOUR_HOST_IP>:4000"
TORUSD_ETCD="--etcd=<EDIT_TO_YOUR_ETCD_IP>:<EDIT_TO_YOUR_ETCD_PORT>"
~~~

#### 4) Now you can control torusd with sytemd.

~~~
systemctl start torusd
~~~
