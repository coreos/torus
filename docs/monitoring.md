# Monitoring Torus

## 1) Run `torusd` with a monitor port

`torusd` supports listening for HTTP requests on a monitoring port. Running with the options:

```
--host $IP --port 4321
```

Enables this functionality. When running inside a container, this is automatically done in the entrypoint script.

## 2) Set up Prometheus to monitor your cluster

If you already have a Prometheus monitoring system set up, you're ready. Add these hosts and ports to be scraped.

If not, [Prometheus](https://prometheus.io/) is a fantastic monitoring tool, and Torus exports all its metrics through the monitor port under the expected `/metrics` path.

[Getting started with Prometheus](https://prometheus.io/docs/introduction/getting_started/) is well-documented. Adding an entry under `scrape_configs` for your `prometheus.yaml` proceeds as normal:

```
scrape_configs:
  # The job name is added as a label `job=<job_name>` to any timeseries scraped from this config.
  - job_name: 'torus'

    # Override the global default and scrape targets from this job every 5 seconds.
    scrape_interval: 15s
    scrape_timeout: 30s

    target_groups:
      - targets: ['localhost:4321', 'localhost:4322', 'localhost:4323', 'localhost:4324']
```

## 3) Using grafana

If you're also using [grafana](http://grafana.org/) to build dashboards on your Prometheus metrics, then you can import the default torus dashboard from the repository: https://github.com/coreos/torus/blob/master/grafana.json, and customize to fit your use cases.
