%%%-------------------------------------------------------------------
%%% @author Tyler Neely
%%% @copyright (C) 2016, Mesosphere
%%% @doc
%%%
%%% @end
%%% Created : 2. Feb 2016 11:44 PM
%%%-------------------------------------------------------------------


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
