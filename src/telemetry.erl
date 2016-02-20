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
         add_gauge_func/2,
         remove_gauge_func/1,
         hdr_to_map/3,
         binary_metrics_to_summary/1
        ]).

-include("telemetry.hrl").

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

add_gauge_func(Name, Fun) ->
  telemetry_store:add_gauge_func(Name, Fun).

remove_gauge_func(Name) ->
  telemetry_store:remove_gauge_func(Name).

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


%% Converts orddicts that are {Time, Metric} -> Value to Metric -> Time -> Value
-spec(binary_metrics_to_summary(#binary_metrics{}) -> maps:map(atom(), histo_summary() | counter_summary())).
binary_metrics_to_summary(#binary_metrics{time_to_binary_histos = TimeToBinaryHistos,
                                          time_to_counters = TimeToCounters}) ->
  HistoExtractFun = fun binary_histo_to_summary/3,
  CounterExtractFun = fun (_Name, _Time, Value) -> Value end,

  Histograms = invert_time_name_to_value_orddict(TimeToBinaryHistos, HistoExtractFun),
  Counters = invert_time_name_to_value_orddict(TimeToCounters, CounterExtractFun),

  #{
    counters => Counters,
    histograms => Histograms
  }.


-spec(invert_time_name_to_value_orddict(time_to_binary_histos() | time_to_counters(), function()) -> 
                                        histo_summary() | counter_summary()).
invert_time_name_to_value_orddict(TimeNameToValueOrddict, ExtractFun) ->
  Orddict = orddict:fold(fun({Time, Name}, ValueIn, AccIn) ->
                             ValueOut = ExtractFun(Name, Time, ValueIn),
                             orddict:append(Name, {Time, ValueOut}, AccIn)
                         end, orddict:new(), TimeNameToValueOrddict),

  DictList = orddict:to_list(Orddict),

  lists:foldl(fun({Name, TimeSummaryList}, AccIn) ->
                  TimeSummaryMap = maps:from_list(TimeSummaryList),
                  maps:put(Name, TimeSummaryMap, AccIn)
              end, #{}, DictList).


-spec(binary_histo_to_summary(string(), integer(), time_to_binary_histos()) -> histo_summary()).
binary_histo_to_summary(Name, Time, BinaryHisto) ->
  {ok, HistoRef} = hdr_histogram:from_binary(BinaryHisto),
  Summary = hdr_to_map(Name, Time, HistoRef),
  hdr_histogram:close(HistoRef),
  Summary.
