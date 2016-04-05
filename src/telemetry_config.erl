%%%-------------------------------------------------------------------
%%% @author Tyler Neely
%%% @copyright (C) 2016, Mesosphere
%%% @doc
%%%
%%% @end
%%% Created : 2. Feb 2016 11:44 PM
%%%-------------------------------------------------------------------
-module(telemetry_config).
-author("Tyler Neely").

%% API
-export([interval_seconds/0,
  max_intervals/0,
  splay_seconds/0,
  forwarder_destinations/0,
  forward_to_all_resolved_hosts/0,
  is_aggregator/0,
  enable_metric_database/0,
  forward_metrics/0,
  receive_metrics/0,
  opentsdb_endpoint/0,
  rendered_metric_receiver_modules/0
  ]).


interval_seconds() ->
  application:get_env(telemetry, interval_seconds, 60).


max_intervals() ->
  application:get_env(telemetry, max_intervals, 60).


splay_seconds() ->
  application:get_env(telemetry, splay_seconds, 10).


forwarder_destinations() ->
  application:get_env(telemetry, forwarder_destinations, []).


%%--------------------------------------------------------------------
%% @doc
%% When resolving each destination in forwarder_destinations, send to
%% ALL resolved hosts or just a single one.  This is useful eg. for
%% sending metrics to all mesos masters.
%% @end
%%--------------------------------------------------------------------
forward_to_all_resolved_hosts() ->
  application:get_env(telemetry, forward_to_all_resolved_hosts, true).


%%--------------------------------------------------------------------
%% @doc
%% Determines whether we should retain aggregate metrics after passing
%% them along, or only incremental measurements.  This should be set to
%% false for anything that is not the final stage of an aggregation
%% pipeline, otherwise deplicate metrics will be submitted.
%% @end
%%--------------------------------------------------------------------
is_aggregator() ->
  application:get_env(telemetry, is_aggregator, false).


forward_metrics() ->
  OverridePath = application:get_env(telemetry, override_path, false),

  case OverridePath of
    false ->
      application:get_env(telemetry, forward_metrics, false);
    Path when is_list(Path) ->
      {ok, FD} = file:open(Path, [binary, raw, read]),
      {ok, JSON} = file:read(FD, 32768),
      DecodedList = jsx:decode(JSON),
      DecodedMap = maps:from_list(DecodedList),
      maps:get(<<"forward_metrics">>, DecodedMap, false)
  end.


receive_metrics() ->
  application:get_env(telemetry, receive_metrics, false).


enable_metric_database() ->
  application:get_env(telemetry, enable_metric_database, false).


opentsdb_endpoint() ->
  application:get_env(telemetry, opentsdb_endpoint, false).


rendered_metric_receiver_modules() ->
  application:get_env(telemetry, rendered_metric_receiver_modules, []).

