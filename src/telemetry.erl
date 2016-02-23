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
         counter/3,
         counter/4,
         histogram/2,
         histogram/3,
         histogram/4,
         add_gauge_func/2,
         remove_gauge_func/1,
         hdr_to_map/1,
         binary_metrics_to_summary/1,
         metrics_to_summary/1
        ]).

-include("telemetry.hrl").

start() ->
  application:ensure_all_started(telemetry).

stop() ->
  application:stop(telemetry).

default_tags() ->
  {ok, HN} = inet:gethostname(),
  M = maps:new(),
  maps:put(hostname, HN, M).

-spec(counter(Name :: string(), Value :: float()) -> ok).
counter(Name, Value) ->
  Now = os:system_time(seconds),
  DefaultTags = default_tags(),
  telemetry_store:submit(#name_tags{name = Name, tags = DefaultTags},
                         Now, counter, Value).

-spec(counter(Name :: string(),
              Tags :: maps:map(string() | atom(), string() | atom()),
              Value :: float()) -> ok).
counter(Name, Tags, Value) ->
  Now = os:system_time(seconds),
  MergedTags = maps:merge(default_tags(), Tags),
  telemetry_store:submit(#name_tags{name = Name, tags = MergedTags},
                         Now, counter, Value).

-spec(counter(Name :: string(),
              Tags :: maps:map(string() | atom(), string() | atom()),
              AggregateTags :: list(list(string() | atom())),
              Value :: float()) -> ok).
counter(Name, Tags, AggregateTags, Value) ->
  Now = os:system_time(seconds),
  MergedDefaultTags = maps:merge(default_tags(), Tags),
  lists:map(fun(AggTagList) ->
                lists:map(fun(AggTags) ->
                              AT2 = lists:map(fun (Tag) ->
                                                  {Tag, aggregate}
                                              end, AggTags),
                              AggTagMap = maps:from_list(AT2),
                              MergedTags = maps:merge(MergedDefaultTags, AggTagMap),
                              telemetry_store:submit(#name_tags{name = Name, tags = MergedTags},
                                                     Now, counter, Value)
                          end, AggTagList)
            end, [[], AggregateTags]).
  

-spec(histogram(Name :: string(), Value :: float()) -> ok).
histogram(Name, Value) ->
  Now = os:system_time(seconds),
  DefaultTags = default_tags(),
  telemetry_store:submit(#name_tags{name = Name, tags = DefaultTags},
                         Now, histogram, Value).

-spec(histogram(Name :: string(),
                Tags :: maps:map(string() | atom(), string() | atom()),
                Value :: float()) -> ok).
histogram(Name, Tags, Value) ->
  Now = os:system_time(seconds),
  MergedTags = maps:merge(default_tags(), Tags),
  telemetry_store:submit(#name_tags{name = Name, tags = MergedTags},
                         Now, histogram, Value).

-spec(histogram(Name :: string(),
                Tags :: maps:map(string() | atom(), string() | atom()),
                AggregateTags :: list(list(string() | atom())),
                Value :: float()) -> ok).
histogram(Name, Tags, AggregateTags, Value) ->
  Now = os:system_time(seconds),
  MergedDefaultTags = maps:merge(default_tags(), Tags),
  lists:map(fun(AggTagList) ->
                lists:map(fun(AggTags) ->
                              AT2 = lists:map(fun (Tag) ->
                                                  {Tag, aggregate}
                                              end, AggTags),
                              AggTagMap = maps:from_list(AT2),
                              MergedTags = maps:merge(MergedDefaultTags, AggTagMap),
                              telemetry_store:submit(#name_tags{name = Name, tags = MergedTags},
                                                     Now, histogram, Value)
                          end, AggTagList)
            end, [[], AggregateTags]).
  

-spec(add_gauge_func(Name :: string() | atom(),
                     Fun :: fun()) -> ok).
add_gauge_func(Name, Fun) ->
  telemetry_store:add_gauge_func(Name, Fun).

-spec(remove_gauge_func(Name :: string() | atom()) -> ok).
remove_gauge_func(Name) ->
  telemetry_store:remove_gauge_func(Name).

hdr_to_map(HistoRef) ->
  #{
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
  HistoExtractFun = fun binary_histo_to_summary/1,
  CounterExtractFun = fun (Value) -> Value end,

  Histograms = invert_time_name_to_value_orddict(TimeToBinaryHistos, HistoExtractFun),
  Counters = invert_time_name_to_value_orddict(TimeToCounters, CounterExtractFun),

  #{
    counters => Counters,
    histograms => Histograms
  }.


%% Converts orddicts that are {Time, Metric} -> Value to Metric -> Time -> Value
-spec(metrics_to_summary(#metrics{}) -> maps:map(atom(), histo_summary() | counter_summary())).
metrics_to_summary(#metrics{time_to_histos = TimeToHistos,
                            time_to_counters = TimeToCounters}) ->
  HistoExtractFun = fun hdr_to_map/1,
  CounterExtractFun = fun (Value) -> Value end,

  Histograms = invert_time_name_to_value_orddict(TimeToHistos, HistoExtractFun),
  Counters = invert_time_name_to_value_orddict(TimeToCounters, CounterExtractFun),

  #{
    counters => Counters,
    histograms => Histograms
  }.


-spec(invert_time_name_to_value_orddict(time_to_binary_histos() | time_to_counters(), function()) ->
                                        histo_summary() | counter_summary()).
invert_time_name_to_value_orddict(TimeNameToValueOrddict, ExtractFun) ->
  Orddict = orddict:fold(fun({Time, Name}, ValueIn, AccIn) ->
                             ValueOut = ExtractFun(ValueIn),
                             orddict:append(Name, {Time, ValueOut}, AccIn)
                         end, orddict:new(), TimeNameToValueOrddict),

  DictList = orddict:to_list(Orddict),

  lists:foldl(fun({Name, TimeSummary}, AccIn) ->
                  TimeSummaryMap = maps:from_list(TimeSummary),
                  maps:put(Name, TimeSummaryMap, AccIn)
              end, #{}, DictList).


-spec(binary_histo_to_summary(time_to_binary_histos()) -> histo_summary()).
binary_histo_to_summary(BinaryHisto) ->
  {ok, HistoRef} = hdr_histogram:from_binary(BinaryHisto),
  Summary = hdr_to_map(HistoRef),
  hdr_histogram:close(HistoRef),
  Summary.
