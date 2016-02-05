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
  submit/4,
  reap/0,
  merge_binary/1
  ]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-include("telemetry.hrl").

-define(SERVER, ?MODULE).


%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Submit a metric to the store for aggregation.
%% @end
%%--------------------------------------------------------------------
-spec(submit(Name :: binary(), Time :: integer(), Type :: term(), Value :: term()) -> ok | {error, atom()}).
submit(Name, Time, Type, Value) ->
  gen_server:cast(?SERVER, {submit, Name, Time, Type, Value}).

%%--------------------------------------------------------------------
%% @doc
%% For all times which have had metrics submitted in the last interval,
%% collect the counters and hdr_histogram binary exports.
%% @end
%%--------------------------------------------------------------------
-spec(reap() -> {ok, term()} | {error, atom()}).
reap() ->
  gen_server:call(?SERVER, reap).

%%--------------------------------------------------------------------
%% @doc
%% Take counters and serialized hdr_histograms and merge them with our
%% state.
%% @end
%%--------------------------------------------------------------------
-spec(merge_binary(Metrics :: #binary_metrics{}) -> ok | {error, atom()}).
merge_binary(Metrics) ->
  gen_server:call(?SERVER, {merge_binary, Metrics}).

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
  {ok, State :: #metrics{}} | {ok, State :: #metrics{}, timeout() | hibernate} |
  {stop, Reason :: term()} | ignore).
init([]) ->
  {ok, #metrics{}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
  State :: #metrics{}) ->
  {reply, Reply :: #binary_metrics{}, NewState :: #metrics{}} |
  {reply, Reply :: #binary_metrics{}, NewState :: #metrics{}, timeout() | hibernate} |
  {noreply, NewState :: #metrics{}} |
  {noreply, NewState :: #metrics{}, timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewState :: #metrics{}} |
  {stop, Reason :: term(), NewState :: #metrics{}}).
handle_call(reap, _From, #metrics{time_to_histos = TimeToHistos,
                                time_to_counters = TimeToCounters,
                                dirty_times = DirtyTimes}) ->

  io:format("[call] reap in store~n"),
  RetHistos = orddict:filter(fun ({Time, _Name}, _V) ->
                                 sets:is_element(Time, DirtyTimes)
                             end, TimeToHistos),

	RetHistos2 = orddict:map(fun ({_Time, _Name}, [HistoRef]) ->
                               hdr_histogram:to_binary(HistoRef)
                           end, RetHistos),

  RetCounters = orddict:filter(fun ({Time, _Name}, _V) ->
                                   sets:is_element(Time, DirtyTimes)
                               end, TimeToCounters),

  RetState = #binary_metrics{time_to_binary_histos = RetHistos2,
                             time_to_counters = RetCounters,
                             dirty_times = DirtyTimes},

  Now = os:system_time(seconds),
  CutoffTime = Now - (telemetry_config:interval_seconds() *
                      telemetry_config:max_intervals()),

  TimeToHistos2 = orddict:filter(fun ({Time, _Name}, [HistoRef]) ->
                                     case Time =< CutoffTime of
                                       true ->
                                         true;
                                       false ->
                                         hdr_histogram:close(HistoRef),
                                         false
                                     end
                                 end, TimeToHistos),
  TimeToCounters2 = orddict:filter(fun ({Time, _Name}, _V) ->
                                       Time =< CutoffTime
                                   end, TimeToCounters),
  {reply, RetState, #metrics{time_to_histos = TimeToHistos2,
                           time_to_counters = TimeToCounters2}};

handle_call({merge_binary, #metrics{time_to_histos = TimeToBinaryHistosIn,
                             time_to_counters = TimeToCountersIn,
                             dirty_times = DirtyTimesIn}},
            _From,
            _State = #metrics{time_to_histos = TimeToHistos,
                              time_to_counters = TimeToCounters,
                              dirty_times = DirtyTimes}) ->
  MergedDirtyTimes = sets:union(DirtyTimesIn, DirtyTimes),
  MergedCounters = merge_counters(TimeToCountersIn, TimeToCounters),
  MergedHistos = merge_histos(TimeToBinaryHistosIn, TimeToHistos),
  MergedState = #metrics{time_to_histos = MergedHistos,
                         time_to_counters = MergedCounters,
                         dirty_times = MergedDirtyTimes},
  {reply, ok, MergedState};

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
-spec(handle_cast(Request :: term(), State :: #metrics{}) ->
  {noreply, NewState :: #metrics{}} |
  {noreply, NewState :: #metrics{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #metrics{}}).
handle_cast({submit, Name, Time, histogram, Value},
            State = #metrics{time_to_histos = TimeToHistos,
                           dirty_times = DirtyTimes}) ->

  NormalizedTime = Time - (round(Time) rem telemetry_config:interval_seconds()),
  TimeToHistos2 = case orddict:is_key({NormalizedTime, Name}, TimeToHistos) of
                    true ->
                      TimeToHistos;
                    false ->
                      %% This opens up a histogram with a max value of 1M,
                      %% which records up to 3 significant figures of a value.
                      {ok, HistoRef} = hdr_histogram:open(1000000, 3),
                      orddict:append({NormalizedTime, Name}, HistoRef, TimeToHistos)
                  end,

  orddict:update({NormalizedTime, Name},
                 fun ([HistoRef]) ->
                     hdr_histogram:record(HistoRef, Value)
                 end, TimeToHistos2),

  DirtyTimes2 = sets:add_element(NormalizedTime, DirtyTimes),

  {noreply, State#metrics{time_to_histos = TimeToHistos2,
                        dirty_times = DirtyTimes2}};

handle_cast({submit, Name, Time, counter, Value},
            State = #metrics{time_to_counters = TimeToCounters,
                           dirty_times = DirtyTimes}) ->

  NormalizedTime = Time - (round(Time) rem telemetry_config:interval_seconds()),

  TimeToCounters2 = orddict:update_counter({NormalizedTime, Name}, Value, TimeToCounters),

  DirtyTimes2 = sets:add_element(NormalizedTime, DirtyTimes),

  {noreply, State#metrics{time_to_counters = TimeToCounters2,
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
-spec(handle_info(Info :: timeout() | term(), State :: #metrics{}) ->
  {noreply, NewState :: #metrics{}} |
  {noreply, NewState :: #metrics{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #metrics{}}).
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
  State :: #metrics{}) -> term()).
terminate(_Reason, _State = #metrics{}) ->
  ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #metrics{},
  Extra :: term()) ->
  {ok, NewState :: #metrics{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Takes an orddict of {Time, Name} -> hdr_histogram exported binaries,
%% and merges it with an orddict of {Time, Name} -> hdr_histogram
%% local instances.
%% @end
%%--------------------------------------------------------------------
merge_histos(TimeToBinaryHistos, TimeToHistos) ->
  MergeFunc = fun (_K, HistoBinary, HistoRef) ->
                  {ok, HistoRefToMerge} = hdr_histogram:from_binary(HistoBinary),
                  hdr_histogram:add(HistoRef, HistoRefToMerge),
                  hdr_histogram:close(HistoRefToMerge),
                  HistoRef
              end,
  orddict:merge(MergeFunc, TimeToBinaryHistos, TimeToHistos).


merge_counters(TimeToCounters1, TimeToCounters2) ->
  MergeFunc = fun(_K, Counter1, Counter2) ->
                  Counter1 + Counter2
              end,
  orddict:merge(MergeFunc, TimeToCounters1, TimeToCounters2).
