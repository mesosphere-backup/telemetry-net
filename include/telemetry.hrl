%%%-------------------------------------------------------------------
%%% @author Tyler Neely
%%% @copyright (C) 2016, Mesosphere
%%% @doc
%%%
%%% @end
%%% Created : 2. Feb 2016 11:44 PM
%%%-------------------------------------------------------------------

-type time_to_binary_histos() :: orddict:orddict({integer(), string()}, binary()).
-type time_to_histos() :: orddict:orddict({integer(), string()}, term()).
-type time_to_counters() :: orddict:orddict({integer(), string()}, integer()).
-type histo_summary() :: maps:map(string(), maps:map(integer(), maps:map(atom(), term()))).
-type counter_summary() :: maps:map(string(), maps:map(integer(), integer())).

-record(metrics, {
  time_to_histos = orddict:new(),
  time_to_counters = orddict:new(),
  dirty_histo_times = sets:new(),
  dirty_counter_times = sets:new()
  }).

-record(binary_metrics, {
  time_to_binary_histos = orddict:new(),
  time_to_counters = orddict:new(),
  dirty_histo_times = sets:new(),
  dirty_counter_times = sets:new(),
  is_aggregate = false
  }).
