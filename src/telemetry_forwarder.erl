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

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
-spec(init(term()) ->
  {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term()} | ignore).
init([]) ->
  self() ! attempt_push,
  {ok, #state{}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
  State :: #state{}) ->
  {reply, Reply :: term(), NewState :: #state{}} |
  {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_call(_Request, _From, State) ->
  {reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_cast(_Req, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_info(attempt_push, State) ->
  Metrics = telemetry_store:reap(),

  Endpoints = telemetry_config:forwarder_destinations(),

  Destinations = get_destinations(Endpoints),

  DestinationAtomsV2 = lists:map(fun fmt_ip_v2/1, Destinations),
  DestinationAtomsV1 = lists:map(fun fmt_ip_v1/1, Destinations),

  %% TODO(tyler) persist submissions for failed pushes, and retry them before sending
  %% new ones at each interval.
  %% Try to submit to the new endpoint first, then fall back to older one.
  case try_submit(Metrics, [DestinationAtomsV2, DestinationAtomsV1]) of
    {error, no_successful_responses} ->
      DestinationAtoms = DestinationAtomsV1 ++ DestinationAtomsV2,
      lager:warning("failed to submit metrics to any of ~p", [DestinationAtoms]);
    ok -> ok
  end,

  erlang:send_after(splay_ms(), self(), attempt_push),

  {noreply, State};
handle_info(_Info, State) ->
  {noreply, State}.

try_submit(_Metrics, []) ->
  {error, no_successful_responses};
try_submit(Metrics, [DestinationGroup | OtherDestinationGroups]) ->
  {GoodReps, _BadReps} = gen_server:multi_call(DestinationGroup,
                                               telemetry_receiver,
                                               {push_metrics, Metrics}),
  case GoodReps of
    [] ->
      try_submit(Metrics, OtherDestinationGroups);
    _ ->
      ok
  end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
  State :: #state{}) -> term()).
terminate(_Reason, _State = #state{}) ->
  ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
  Extra :: term()) ->
  {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

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
  NextMinute = -1 * erlang:monotonic_time(milli_seconds) rem MsPerMinute,

  SplayMS = telemetry_config:splay_seconds() * 1000,
  FlooredSplayMS = max(1, SplayMS),
  Splay = rand:uniform(FlooredSplayMS),

  NextMinute + Splay.

-spec(fmt_ip_v1({integer(), integer(), integer(), integer()}) -> atom()).
fmt_ip_v1({A, B, C, D}) ->
  prefix_ip("networkagg@", {A, B, C, D}).

-spec(fmt_ip_v2({integer(), integer(), integer(), integer()}) -> atom()).
fmt_ip_v2({A, B, C, D}) ->
  prefix_ip("networking_api@", {A, B, C, D}).

-spec(prefix_ip(string(), {integer(), integer(), integer(), integer()}) -> atom()).
prefix_ip(Prefix, {A, B, C, D}) ->
  NodeList = io_lib:format("~p.~p.~p.~p", [A, B, C, D]),
  PrefixedStr = Prefix ++ NodeList,
  FlatStr = lists:flatten(PrefixedStr),
  list_to_atom(FlatStr).



%%--------------------------------------------------------------------
%% @private
%% @doc
%% If the first argument is true, return the full second argument.
%% Otherwise, return at most one element from the second argument.
%% @end
%%--------------------------------------------------------------------
-spec(take_first_or_all(boolean(), list(term())) -> list(term())).
take_first_or_all(true, R) ->
  R;
take_first_or_all(false, []) ->
  [];
take_first_or_all(false, [A | _]) ->
  [A].

-spec(get_destinations([string()]) -> [inet:ipv4_address()]).
get_destinations(Endpoints) ->
  lists:flatmap(fun name_to_ips/1, Endpoints).

name_to_ips(Name) ->
  UseAllEndpointsPerRecord = telemetry_config:forward_to_all_resolved_hosts(),
  case inet:getaddrs(Name, inet) of
    {ok, Records} ->
      take_first_or_all(UseAllEndpointsPerRecord, Records);
    {error, Error} ->
      lager:warning("Could not resolve name ~s: ~p", [Name, Error]),
      []
  end.


-ifdef(TEST).
prefix_test() ->
  Prefixed = 'abc@1.2.3.4',
  ?assertEqual(Prefixed, prefix_ip("abc@", {1, 2, 3, 4})).
-endif.
