%%%-------------------------------------------------------------------
%%% @author Tyler Neely
%%% @copyright (C) 2016, Mesosphere
%%% @doc
%%%
%%% @end
%%% Created : 2. Feb 2016 11:44 PM
%%%-------------------------------------------------------------------

-module(telemetry_forwarder).
-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-include_lib("kernel/include/logger.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(SERVER, ?MODULE).

-record(state, {}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link() ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
  self() ! attempt_push,
  {ok, #state{}}.

handle_call(_Request, _From, State) ->
  {reply, ok, State}.

handle_cast(_Req, State) ->
  {noreply, State}.

handle_info(attempt_push, State) ->
  push(telemetry_config:forward_metrics()),
  erlang:send_after(splay_ms(), self(), attempt_push),
  {noreply, State};
handle_info(_Info, State) ->
  {noreply, State}.

try_submit(Metrics, Servers) ->
  Process = telemetry_receiver,
  Message = {push_metrics, Metrics},
  case  gen_server:multi_call(Servers, Process, Message) of
    {[], _BadReps} ->
      {error, no_successful_responses};
    {_GoodReps, _BadReps} ->
      ok
  end.

terminate(_Reason, _State = #state{}) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.


push(false) ->
  ok;
push(true) ->
  Metrics = telemetry_store:reap(),

  Destinations = telemetry_config:forwarder_destinations(),

  %% TODO(tyler) persist submissions for failed pushes, and retry them before sending
  %% new ones at each interval.
  %% Try to submit to the new endpoint first, then fall back to older one.
  case try_submit(Metrics, Destinations) of
    {error, no_successful_responses} ->
      ?LOG_WARNING("failed to submit metrics to any of ~p", [Destinations]);
    ok -> ok
  end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Returns the number of milliseconds until the next minute, plus some
%% randomness.  The randomness helps prevent thundering herd submission
%% once per minute while submitting metrics.
%% @end
%%--------------------------------------------------------------------
-spec(splay_ms() -> integer()).
splay_ms() ->
  MsPerMinute = telemetry_config:interval_seconds() * 1000,
  NextMinute = MsPerMinute - erlang:system_time(millisecond) rem MsPerMinute,

  SplayMS = telemetry_config:splay_seconds() * 1000,
  FlooredSplayMS = max(1, SplayMS),
  Splay = rand:uniform(FlooredSplayMS),

  NextMinute + Splay.
