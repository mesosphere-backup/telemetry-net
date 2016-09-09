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
         add_prepare_func/2,
         metrics_to_summary/1
        ]).

-include("telemetry.hrl").

-ifdef(TEST).
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-endif.


start() ->
  application:ensure_all_started(telemetry).

stop() ->
  application:stop(telemetry).

default_tags() ->
  {ok, HN} = inet:gethostname(),
  M = maps:new(),
  maps:put(hostname, HN, M).

-spec(counter(Name :: metric_name(), Value :: integer()) -> ok).
counter(Name, Value) ->
  Now = os:system_time(seconds),
  DefaultTags = default_tags(),
  telemetry_store:submit(#name_tags{name = Name, tags = DefaultTags},
                         Now, counter, Value).

%% @doc This is called by an external program to add Value to a counter
-spec(counter(Name :: metric_name(),
              Tags :: maps:map(string() | atom(), string() | atom()),
              Value :: integer()) -> ok).
counter(Name, Tags, Value) ->
  Now = os:system_time(seconds),
  MergedTags = maps:merge(default_tags(), Tags),
  telemetry_store:submit(#name_tags{name = Name, tags = MergedTags},
                         Now, counter, Value),
  ok.

%% @doc This is called by an external program to add Value to a counter
-spec(counter(Name :: metric_name(),
              Tags :: maps:map(string() | atom(), string() | atom()),
              AggregateTags :: list(list(string() | atom())),
              Value :: integer()) -> ok).
counter(Name, Tags, AggregateTags, Value) ->
  Now = os:system_time(seconds),
  MergedDefaultTags = maps:merge(default_tags(), Tags),
  lists:map(fun(AggTagList) ->
                lists:map(fun(AggTags) ->
                              AT2 = lists:map(fun (Tag) ->
                                                  {Tag, aggregate}
                                              end, AggTags),
                              AggTagMap = maps:from_list(AT2),
                              MergedTags = maps:merge(MergedDefaultTags, AggTagMap),                                                                                                                             telemetry_store:submit(#name_tags{name = Name, tags = MergedTags},
                                                     Now, counter, Value)
                          end, AggTagList)
            end, [[], AggregateTags]),
  ok.


-spec(histogram(Name :: metric_name(), Value :: number()) -> ok).
histogram(Name, Value) ->
  Now = os:system_time(seconds),
  DefaultTags = default_tags(),
  telemetry_store:submit(#name_tags{name = Name, tags = DefaultTags},
                         Now, histogram, Value).

-spec(histogram(Name :: metric_name(),
                Tags :: maps:map(string() | atom(), string() | atom()),
                Value :: number()) -> ok).
histogram(Name, Tags, Value) ->
  Now = os:system_time(seconds),
  MergedTags = maps:merge(default_tags(), Tags),
  telemetry_store:submit(#name_tags{name = Name, tags = MergedTags},
                         Now, histogram, Value).

%% @doc This is called by an external program to add Value to the histogram specificed by Name.
-spec(histogram(Name :: metric_name(),
                Tags :: maps:map(string() | atom(), string() | atom()),
                AggregateTags :: list(list(string() | atom())),
                Value :: number()) -> ok).
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
            end, [[], AggregateTags]),
  ok.

-spec(add_prepare_func(Name :: string() | atom(),
                       Fun :: fun((#metrics{}) -> #metrics{})) -> ok).
add_prepare_func(Name, Fun) ->
  telemetry_store:add_prepare_func(Name, Fun).

-spec(add_gauge_func(Name :: string() | atom(),
                     Fun :: fun()) -> ok).
add_gauge_func(Name, Fun) ->
  telemetry_store:add_gauge_func(Name, Fun).

-spec(remove_gauge_func(Name :: string() | atom()) -> ok).
remove_gauge_func(Name) ->
  telemetry_store:remove_gauge_func(Name).

%% Converts orddicts that are {Time, Metric} -> Value to Metric -> Time -> Value
-spec(metrics_to_summary(#metrics{}) -> maps:map(atom(), histo_summary() | counter_summary())).
metrics_to_summary(#metrics{time_to_histos = TimeToHistos,
                            time_to_counters = TimeToCounters}) ->
  HistoExtractFun = fun telemetry_histo:map_summary/1,
  CounterExtractFun = fun (Value) -> Value end,

  Histograms = invert_time_name_to_value_orddict(TimeToHistos, HistoExtractFun),
  Counters = invert_time_name_to_value_orddict(TimeToCounters, CounterExtractFun),

  #{
    counters => Counters,
    histograms => Histograms
  }.


-spec(invert_time_name_to_value_orddict(time_to_histos() | time_to_counters(), function()) ->
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

-ifdef(TEST).
prepare_test_() -> {setup,
                    fun() -> start(), ok end,
                    fun(_) -> stop() end,
                    ?_assertEqual(ok, add_prepare_func(foobar, fun(M) -> M end))}.
-endif.
