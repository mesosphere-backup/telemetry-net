%%%-------------------------------------------------------------------
%%% @author Tyler Neely
%%% @copyright (C) 2016, Mesosphere
%%% @doc
%%%
%%% @end
%%% Created : 2. Feb 2016 11:44 PM
%%%-------------------------------------------------------------------
-module(telemetry).
-author("Tyler Neely").

%% API
-export([start/0,
         stop/0,
         counter/2,
         histogram/2,
         hdr_to_map/3
        ]).

start() ->
  application:ensure_all_started(telemetry).

stop() ->
  application:stop(telemetry).

counter(Name, Value) ->
  Now = os:system_time(seconds),
  telemetry_store:submit(Name, Now, counter, Value).

histogram(Name, Value) ->
  Now = os:system_time(seconds),
  telemetry_store:submit(Name, Now, histogram, Value).

hdr_to_map(Name, Time, HistoRef) ->
  #{
    name => Name,
    time => Time,
    min => hdr_histogram:min(HistoRef),
    mean => hdr_histogram:mean(HistoRef),
    median => hdr_histogram:median(HistoRef),
    max => hdr_histogram:max(HistoRef),
    stddev => hdr_histogram:stddev(HistoRef),
    p75 => hdr_histogram:percentile(HistoRef, 75.0),
    p90 => hdr_histogram:percentile(HistoRef, 90.0),
    p95 => hdr_histogram:percentile(HistoRef, 95.0),
    p99 => hdr_histogram:percentile(HistoRef, 99.0),
    p999 => hdr_histogram:percentile(HistoRef, 99.9),
    p9999 => hdr_histogram:percentile(HistoRef, 99.99),
    p99999 => hdr_histogram:percentile(HistoRef, 99.999),
    total_count => hdr_histogram:get_total_count(HistoRef)
  }.
