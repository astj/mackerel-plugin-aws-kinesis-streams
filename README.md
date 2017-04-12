mackerel-plugin-aws-kinesis-streams
=================================

AWS Kinesis Streams custom metrics plugin for mackerel.io agent.

## DEPRECATED

This plugin is now part of [Official Mackerel Agent Plugins](https://github.com/mackerelio/mackerel-agent-plugins/tree/master/mackerel-plugin-aws-kinesis-streams).
Please use official plugins, not this repository.

## Synopsis

```shell
mackerel-plugin-aws-kinesis-streams -identifier=<stream-name> -region=<aws-region> [-access-key-id=<id>] [-secret-access-key=<key>] [-tempfile=<tempfile>]
```
* collect data from specified AWS Kinesis Streams
* you can set keys by environment variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`

## Example of mackerel-agent.conf

```
[plugin.metrics.aws-kinesis-streams]
command = "/path/to/mackerel-plugin-aws-kinesis-streams"
```
