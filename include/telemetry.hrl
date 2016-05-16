%%%-------------------------------------------------------------------
%%% @author Tyler Neely
%%% @copyright (C) 2016, Mesosphere
%%% @doc
%%%
%%% @end
%%% Created : 2. Feb 2016 11:44 PM
%%%-------------------------------------------------------------------

-type time_to_histos() :: orddict:orddict({integer(), string()}, term()).
-type time_to_counters() :: orddict:orddict({integer(), string()}, integer()).
-type histo_summary() :: maps:map(string(), maps:map(integer(), maps:map(atom(), term()))).
-type counter_summary() :: maps:map(string(), maps:map(integer(), integer())).

-type metric_name() :: atom() | binary() | string().

-record(name_tags, {
  name :: string(),
  tags :: maps:map()
  }).


-record(metrics, {
  time_to_histos = orddict:new(),
  time_to_counters = orddict:new(),
  dirty_histos = sets:new(),
  dirty_counters = sets:new()
  }).
