%%%-------------------------------------------------------------------
%%% @author Tyler Neely
%%% @copyright (C) 2016, Mesosphere
%%% @doc
%%%
%%% @end
%%% Created : 2. Feb 2016 11:44 PM
%%%-------------------------------------------------------------------
-module(telemetry_metrics).
-author("Tyler Neely").


-export([update/3]).

-ifdef(TEST).
update(_Metric, _Value, _Type) ->
  ok.
-else.
update(Metric, Value, Type) ->
  case exometer:update(Metric, Value) of
    {error, not_found} ->
      ok = exometer:ensure(Metric, Type, []),
      ok = exometer:update(Metric, Value);
    _ -> ok
  end.
-endif.

