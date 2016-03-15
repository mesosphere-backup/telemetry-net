%%%-------------------------------------------------------------------
%%% @author Tyler Neely
%%% @copyright (C) 2016, Mesosphere
%%% @doc
%%%
%%% @end
%%% Created : 14. March 2016 17:43 PM
%%%-------------------------------------------------------------------
-module(telemetry_histo).
-author("Tyler Neely").

%% API
-export([new/0,
         percentile/2,
         record/2,
         merge/2]).

-record(histo, {
  total = 0 :: integer(),
  values = orddict:new() :: orddict:orddict()
  }).

new() ->
  #histo{}.

percentile(#histo{total = 0}, _Pct) ->
  {error, empty_histo};
percentile(#histo{total = T, values = V}, Pct) ->
  Threshold = T * Pct,
  PctFun = fun(CompK, Count, AccIn) ->
               case AccIn of
                 {notfound, SoFar} ->
                   NewSoFar = Count + SoFar,
                   case NewSoFar >= Threshold of
                     true ->
                       {found, decompress(CompK)};
                     false ->
                       {notfound, NewSoFar}
                   end;
                 {found, _} ->
                   AccIn
               end
           end,
  orddict:fold(PctFun, {notfound, 0}, V).

record(#histo{total = Total, values = Values}, V) ->
  CompK = compress(V),
  NewValues = orddict:update_counter(CompK, 1, Values),
  #histo{total = Total + 1, values = NewValues}.

merge(#histo{total = T1, values = V1}, #histo{total = T2, values = V2}) ->
  MergeFun = fun(K, V1, V2) ->
                 V1 + V2
             end,
  NewValues = orddict:merge(MergeFun, V1, V2),
  #histo{total = T1 + T2, values = NewValues}.

compress(V) when V >= 0 -> 100 * math:log(1.0 + abs(V)) + 0.5;
compress(V) -> -1.0 * (100 * math:log(1.0 + abs(V)) + 0.5).

decompress(V) when V >= 0 -> math:exp(abs(V) / 100) - 1.0;
decompress(V) -> -1.0 * (math:exp(abs(V) / 100) - 1.0).

map_summary(H = #histo{total = Total}) ->
  #{
    min => percentile(H, 0),
    median => percentile(H, 50),
    max => percentile(H, 100),
    p75 => percentile(H, 75.0),
    p90 => percentile(H, 90.0),
    p95 => percentile(H, 95.0),
    p99 => percentile(H, 99.0),
    p999 => percentile(H, 99.9),
    p9999 => percentile(H, 99.99),
    p99999 => percentile(H, 99.999),
    total_count => Total
  }.

