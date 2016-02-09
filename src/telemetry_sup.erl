%%%-------------------------------------------------------------------
%%% @author Tyler Neely
%%% @copyright (C) 2016, Mesosphere
%%% @doc
%%%
%%% @end
%%% Created : 2. Feb 2016 11:44 PM
%%%-------------------------------------------------------------------
-module(telemetry_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).



%% ===================================================================
%% Helper functions
%% ===================================================================
maybe_add_forwarder(Children) ->
  case telemetry_config:forward_metrics() of
    true ->
      [?CHILD(telemetry_forwarder, worker) | Children];
    false ->
      Children
  end.


maybe_add_receiver(Children) ->
  case telemetry_config:receive_metrics() of
    true ->
      [?CHILD(telemetry_receiver, worker) | Children];
    false ->
      Children
  end.


%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
  Children = maybe_add_forwarder([]),
  Children2 = maybe_add_receiver(Children),
  %% always make sure telemetry_store is first in this list
  Children3 = [?CHILD(telemetry_store, worker) | Children2],
  {ok, {{one_for_one, 5, 10}, Children3}}.
