[![CircleCI][circleci badge]][circleci]
[![Coverage][coverage badge]][covercov]
[![Jira][jira badge]][jira]
[![License][license badge]][license]
[![Erlang Versions][erlang version badge]][erlang]

# Telemetry-net

Telemetry-net is a library for aggregation of metrics across many systems.  It supports counters and histograms.  You can also register a function to be called periodically to populate a value.  You can also specify aggregation tags on the client to specify that certain axes of tags on metrics should be collapsed.

## Configuration

```
  {telemetry,
    [
      % Aggregators retain metrics over time, otherwise all metrics are dropped after trying to forward them.
      {is_aggregator, false},
      % Periodically send metrics to a destination
      {forward_metrics, true},
      % Don't allow receiving metrics
      {receive_metrics, false},
      % Send metrics to all hosts that are in this DNS A record
      {forwarder_destinations, ["master.mesos"]},
      % Send metrics every 60 seconds
      {interval_seconds, 60},
      % Add 20 seconds of jitter to the interval to avoid thundering herd
      {splay_seconds, 20},
    ]
  }
```
## Usage

```
Tags = #{host => "host-324242", destination => "10.1.2.3:5"},
% Aggregate pass-through (include all tags), collapse on hostname to get
% global metrics across all hostnames for various destinations, and also
% aggregate on both hostname and destination for global metrics across
% all hosts and backends.
AggTags = [[], [hostname], [hostname, destination]],
telemetry:counter(connect_successes, Tags, AggTags, 1),
telemetry:histogram(connect_latency, Tags, AggTags, TimeDelta),
```

<!-- Badges -->
[circleci badge]: https://img.shields.io/circleci/project/github/dcos/telemetry-net/master.svg?style=flat-square
[coverage badge]: https://img.shields.io/codecov/c/github/dcos/telemetry-net/master.svg?style=flat-square
[jira badge]: https://img.shields.io/badge/issues-jira-yellow.svg?style=flat-square
[license badge]: https://img.shields.io/github/license/dcos/telemetry-net.svg?style=flat-square
[erlang version badge]: https://img.shields.io/badge/erlang-20.0-blue.svg?style=flat-square

<!-- Links -->
[circleci]: https://circleci.com/gh/dcos/telemetry-net
[covercov]: https://codecov.io/gh/dcos/telemetry-net
[jira]: https://jira.dcos.io/issues/?jql=component+%3D+networking+AND+project+%3D+DCOS_OSS
[license]: ./LICENSE
[erlang]: http://erlang.org/
