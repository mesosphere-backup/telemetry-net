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
         histogram/2
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

