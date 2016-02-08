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
  forwarder_destinations/0,
  is_aggregator/0]).


interval_seconds() ->
  application:get_env(telemetry, interval_seconds, 6).


max_intervals() ->
  application:get_env(telemetry, max_intervals, 60).


forwarder_destinations() ->
  application:get_env(telemetry, forwarder_destinations, "localhost").


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
