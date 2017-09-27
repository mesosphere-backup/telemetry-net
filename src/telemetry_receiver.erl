%%%-------------------------------------------------------------------
%%% @author Tyler Neely
%%% @copyright (C) 2016, Mesosphere
%%% @doc
%%%
%%% @end
%%% Created : 2. Feb 2016 11:44 PM
%%%-------------------------------------------------------------------

-module(telemetry_receiver).
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
  {ok, #state{}}.

handle_call({push_metrics, Metrics}, _From, State) ->
  Res = telemetry_store:merge(Metrics),
  {reply, Res, State}.

handle_cast(_Req, State) ->
  {noreply, State}.

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, _State = #state{}) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.
