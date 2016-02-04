%%%-------------------------------------------------------------------
%%% @author Tyler Neely
%%% @copyright (C) 2016, Mesosphere
%%% @doc
%%%
%%% @end
%%% Created : 2. Feb 2016 11:44 PM
%%%-------------------------------------------------------------------

-module(telemetry_store).
-behaviour(gen_server).

%% API
-export([start_link/0,
  submit/3,
  reap/0
  ]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).


-define(SERVER, ?MODULE).

-record(state, {
          time_to_histos = orddict:new(),
          time_to_counters = orddict:new(),
          dirty_times = sets:new()
         }).


%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec(submit(Name :: binary(), Type :: term(), Value :: term()) -> ok | {error, atom()}).
submit(Name, Type, Value) ->
  gen_server:cast(?SERVER, {submit, Name, Type, Value}).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec(reap() -> {ok, term()} | {error, atom()}).
reap() ->
  gen_server:call(?SERVER, reap).

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
handle_call(reap, _From, #state{time_to_histos = TimeToHistos,
                                time_to_counters = TimeToCounters,
                                dirty_times = DirtyTimes}) ->

  io:format("[call] reap in store~n"),
  RetHistos = orddict:filter(fun ({Time, _Name}, _V) ->
                                 sets:is_element(Time, DirtyTimes)
                             end, TimeToHistos),
  RetCounters = orddict:filter(fun ({Time, _Name}, _V) ->
                                   sets:is_element(Time, DirtyTimes)
                               end, TimeToCounters),
  RetState = #state{time_to_histos = RetHistos,
                    time_to_counters = RetCounters,
                    dirty_times = DirtyTimes},

  Now = os:system_time(seconds),
  CutoffTime = Now - (telemetry_config:interval_seconds() * telemetry_config:max_intervals()),

  TimeToHistos2 = orddict:filter(fun ({Time, _Name}, _V) ->
                                     Time =< CutoffTime
                                 end, TimeToHistos),
  TimeToCounters2 = orddict:filter(fun ({Time, _Name}, _V) ->
                                       Time =< CutoffTime
                                   end, TimeToCounters),
  {reply, RetState, #state{time_to_histos = TimeToHistos2,
                           time_to_counters = TimeToCounters2}};

handle_call(_Request, _From, State) ->
  io:format("[call] unhandled in store~n"),
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
handle_cast({submit, Time, Name, histogram, Value},
            State = #state{time_to_histos = TimeToHistos,
                           dirty_times = DirtyTimes}) ->

  NormalizedTime = Time - (Time rem telemetry_config:interval_seconds()),
  TimeToHistos2 = case orddict:is_key({NormalizedTime, Name}, TimeToHistos) of
                    true ->
                      TimeToHistos;
                    false ->
                      orddict:append({NormalizedTime, Name},
                                     hdr_histogram:open(1000000,3),
                                     TimeToHistos)
                  end,

  TimeToHistos3 = orddict:update({NormalizedTime, Name},
                                 fun (HistoRef) ->
                                     hdr_histogram:record(HistoRef, Value)
                                 end,
                                 TimeToHistos2),
  
  DirtyTimes2 = sets:add_element(NormalizedTime, DirtyTimes),

  {noreply, State#state{time_to_histos = TimeToHistos3,
                        dirty_times = DirtyTimes2}};

handle_cast({submit, Time, Name, counter, Value},
            State = #state{time_to_counters = TimeToCounters,
                           dirty_times = DirtyTimes}) ->

  NormalizedTime = Time - (Time rem telemetry_config:interval_seconds()),
  TimeToCounters2 = case orddict:is_key({NormalizedTime, Name}, TimeToCounters) of
                      true -> TimeToCounters;
                      false -> orddict:append({NormalizedTime, Name}, 0, TimeToCounters)
                    end,

  TimeToCounters3 = orddict:update_counter({NormalizedTime, Name}, Value, TimeToCounters2),

  DirtyTimes2 = sets:add_element(NormalizedTime, DirtyTimes),

  {noreply, State#state{time_to_counters = TimeToCounters3,
                        dirty_times = DirtyTimes2}}.

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
handle_info(_Info, State) ->
  {noreply, State}.

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

