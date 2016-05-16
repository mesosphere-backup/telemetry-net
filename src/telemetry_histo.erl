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
         merge/2,
         map_summary/1]).

-record(histo, {
  total = 0 :: integer(),
  values = orddict:new() :: orddict:orddict()
  }).

new() ->
  #histo{}.

percentile(#histo{total = 0}, _Pct) ->
  {error, empty_histo};
percentile(#histo{total = T, values = V}, Pct) when Pct >= 0 andalso Pct =< 1.0 ->
  Threshold = T * Pct,
  PctFun = fun(CompK, Count, _AccIn = {notfound, SoFar}) ->
                NewSoFar = Count + SoFar,
                case NewSoFar >= Threshold of
                  true ->
                    decompress(CompK);
                  false ->
                    {notfound, NewSoFar}
                end;
              (_CompK, _Count, AccIn)   ->
                AccIn
           end,
  orddict:fold(PctFun, {notfound, 0}, V).

record(#histo{total = Total, values = Values}, V) ->
  CompK = compress(V),
  NewValues = orddict:update_counter(CompK, 1, Values),
  #histo{total = Total + 1, values = NewValues}.

merge(#histo{total = T1, values = V1}, #histo{total = T2, values = V2}) ->
  MergeFun = fun(_K, Count1, Count2) ->
                 Count1 + Count2
             end,
  NewValues = orddict:merge(MergeFun, V1, V2),
  #histo{total = T1 + T2, values = NewValues}.

-spec(compress(V :: float()) -> integer()).
compress(V) when V >= 0 -> round(100 * math:log(1.0 + abs(V)) + 0.5);
compress(V) -> -1 * compress(-1 * V).

-spec(decompress(V :: integer()) -> float()).
decompress(V) when V >= 0 -> math:exp(abs(V) / 100) - 1.0;
decompress(V) -> -1.0 * decompress(-1 * V).

map_summary(H = #histo{total = Total}) ->
  #{
    min => percentile(H, 0),
    median => percentile(H, 0.50),
    max => percentile(H, 1.0),
    p75 => percentile(H, 0.75),
    p90 => percentile(H, 0.9),
    p95 => percentile(H, 0.95),
    p99 => percentile(H, 0.99),
    p999 => percentile(H, 0.999),
    p9999 => percentile(H, 0.9999),
    p99999 => percentile(H, 0.99999),
    total_count => Total
  }.

