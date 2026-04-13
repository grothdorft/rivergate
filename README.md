# rivergate

A lightweight stream processor for routing and transforming log events between sources and sinks with a declarative config.

---

## Installation

```bash
go install github.com/yourorg/rivergate@latest
```

Or build from source:

```bash
git clone https://github.com/yourorg/rivergate.git && cd rivergate && go build ./...
```

---

## Usage

Define a `rivergate.yaml` config file:

```yaml
sources:
  - name: app_logs
    type: file
    path: /var/log/app.log

transforms:
  - name: add_env
    type: add_field
    field: environment
    value: production

sinks:
  - name: stdout_out
    type: stdout
  - name: elastic_out
    type: elasticsearch
    host: http://localhost:9200
    index: app-logs
```

Run the processor:

```bash
rivergate --config rivergate.yaml
```

Rivergate will read events from the configured sources, apply transforms in order, and route the output to all defined sinks.

---

## Configuration Reference

| Field | Description |
|---|---|
| `sources` | Input log sources (file, stdin, syslog, kafka) |
| `transforms` | Ordered list of transformation steps |
| `sinks` | Output destinations (stdout, file, elasticsearch, kafka) |

---

## License

MIT © yourorg