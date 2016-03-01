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
  snapshot/0,
  reap/0,
  merge_binary/1,
  add_gauge_func/2,
  remove_gauge_func/1
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

-record(store, {
  metrics = #metrics{},
  metric_funs = maps:new()
  }).


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
%% Get a snapshot of current metrics.
%% @end
%%--------------------------------------------------------------------
-spec(snapshot() -> #binary_metrics{}).
snapshot() ->
  case ets:lookup(snapcache, last_snap) of
    [{last_snap, Cached}] ->
      Cached;
    _ ->
      gen_server:call(?SERVER, snapshot)
  end.

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
  gen_server:cast(?SERVER, {merge_binary, Metrics}).

%%--------------------------------------------------------------------
%% @doc
%% Register a fun of zero arity that returns a numerical value to be
%% called upon the creation of any metrics snapshot.
%% @end
%%--------------------------------------------------------------------
-spec(add_gauge_func(string(), fun()) -> ok | {error, atom()}).
add_gauge_func(Name, Fun) ->
  gen_server:call(?SERVER, {add_gauge_func, Name, Fun}).

%%--------------------------------------------------------------------
%% @doc
%% Remove a metrics function previously registered using add_gauge_func.
%% @end
%%--------------------------------------------------------------------
-spec(remove_gauge_func(string()) -> ok).
remove_gauge_func(Name) ->
  gen_server:call(?SERVER, {remove_gauge_func, Name}).

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
  {ok, State :: #store{}} | {ok, State :: #store{}, timeout() | hibernate} |
  {stop, Reason :: term()} | ignore).
init([]) ->
  snapcache = ets:new(snapcache, [named_table, set, {read_concurrency, true}]),
  {ok, #store{}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
  State :: #store{}) ->
  {reply, Reply :: #binary_metrics{}, NewState :: #store{}} |
  {reply, Reply :: #binary_metrics{}, NewState :: #store{}, timeout() | hibernate} |
  {noreply, NewState :: #store{}} |
  {noreply, NewState :: #store{}, timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewState :: #store{}} |
  {stop, Reason :: term(), NewState :: #store{}}).
handle_call(reap, _From, #store{metrics = Metrics, metric_funs = MetricFuns}) ->
  %% record function gauges
  Metrics2 = record_gauge_funcs(Metrics, MetricFuns),

  #metrics{time_to_histos = TimeToHistos,
           time_to_counters = TimeToCounters} = Metrics2,

  %% Create a snapshot of current metrics.
  ReapedState = export_metrics(Metrics2),

  %% Prune metrics that we should shed.
  Now = os:system_time(seconds),

  CutoffTime = Now - (telemetry_config:interval_seconds() *
                      telemetry_config:max_intervals()),

  TimeToHistos2 = orddict:filter(fun ({Time, _Name}, HistoRef) ->
                                     case Time >= CutoffTime of
                                       true ->
                                         true;
                                       false ->
                                         hdr_histogram:close(HistoRef),
                                         false
                                     end
                                 end, TimeToHistos),

  TimeToCounters2 = orddict:filter(fun ({Time, _Name}, _V) ->
                                       Time >= CutoffTime
                                   end, TimeToCounters),

  %% Only nodes in aggregator mode should retain non-partial metrics.
  IsAggregator = telemetry_config:is_aggregator(),
  RetMetrics = case IsAggregator of
                 true -> #metrics{time_to_histos = TimeToHistos2,
                                  time_to_counters = TimeToCounters2};
                 false -> #metrics{}
               end,
  RetState = #store{metrics = RetMetrics, metric_funs = MetricFuns},
  {reply, ReapedState, RetState};

handle_call(snapshot, _From, State = #store{metrics = Metrics}) ->
  ReapedState = export_metrics(Metrics),
  {reply, ReapedState, State};

handle_call({add_gauge_func, Name, Fun}, _From, State = #store{metric_funs = MetricFuns}) ->
  NewMetricFuns = maps:put(Name, Fun, MetricFuns),
  NewState = State#store{metric_funs = NewMetricFuns},
  {reply, ok, NewState};

handle_call({remove_gauge_func, Name}, _From, State = #store{metric_funs = MetricFuns}) ->
  NewMetricFuns = maps:remove(Name, MetricFuns),
  NewState = State#store{metric_funs = NewMetricFuns},
  {reply, ok, NewState};

handle_call(Request, _From, State) ->
  lager:warn("got unknown request in telemetry_store handle_call: ~p", [Request]),
  {reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #store{}) ->
  {noreply, NewState :: #store{}} |
  {noreply, NewState :: #store{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #store{}}).
handle_cast({submit, Name, Time, histogram, Value},
            State = #store{metrics = Metrics}) ->
  #metrics{time_to_histos = TimeToHistos,
           dirty_histos = DirtyHistos} = Metrics,
  NormalizedTime = Time - (round(Time) rem telemetry_config:interval_seconds()),
  TimeToHistos2 = case orddict:is_key({NormalizedTime, Name}, TimeToHistos) of
                    true ->
                      TimeToHistos;
                    false ->
                      %% This opens up a histogram with a max value of 1M,
                      %% which records up to 3 significant figures of a value.
                      MaxVal = telemetry_config:max_histo_value(),
                      {ok, HistoRef} = hdr_histogram:open(round(MaxVal), 3),
                      orddict:store({NormalizedTime, Name}, HistoRef, TimeToHistos)
                  end,

  orddict:update({NormalizedTime, Name},
                 fun (HistoRef) ->
                     ok = hdr_histogram:record(HistoRef, Value)
                 end, TimeToHistos2),

  DirtyHistos2 = sets:add_element({NormalizedTime, Name}, DirtyHistos),

  RetMetrics = Metrics#metrics{time_to_histos = TimeToHistos2,
                               dirty_histos = DirtyHistos2},

  RetState = State#store{metrics = RetMetrics},

  {noreply, RetState};

handle_cast({merge_binary, #binary_metrics{time_to_binary_histos = TimeToBinaryHistosIn,
                                           time_to_counters = TimeToCountersIn,
                                           dirty_histos = DirtyHistosIn,
                                           dirty_counters = DirtyCountersIn}},
            _State = #store{metrics = Metrics, metric_funs = MetricFuns}) ->
  #metrics{time_to_histos = TimeToHistos,
           time_to_counters = TimeToCounters,
           dirty_histos = DirtyHistos,
           dirty_counters = DirtyCounters} = Metrics,
  MergedDirtyHistos = sets:union(DirtyHistosIn, DirtyHistos),
  MergedDirtyCounters = sets:union(DirtyCountersIn, DirtyCounters),
  MergedCounters = merge_counters(TimeToCountersIn, TimeToCounters),
  MergedHistos = merge_histos(TimeToBinaryHistosIn, TimeToHistos),
  MergedMetrics = #metrics{time_to_histos = MergedHistos,
                           time_to_counters = MergedCounters,
                           dirty_histos = MergedDirtyHistos,
                           dirty_counters = MergedDirtyCounters},
  MergedState = #store{metrics = MergedMetrics, metric_funs = MetricFuns},

  submit_to_opentsdb(MergedMetrics#metrics{dirty_histos = DirtyHistosIn,
                                           dirty_counters = DirtyCountersIn}),

  {noreply, MergedState};


handle_cast({submit, Name, Time, counter, Value},
            State = #store{metrics = Metrics}) ->
  #metrics{time_to_counters = TimeToCounters,
           dirty_counters = DirtyCounters} = Metrics,

  NormalizedTime = Time - (round(Time) rem telemetry_config:interval_seconds()),

  TimeToCounters2 = orddict:update_counter({NormalizedTime, Name}, Value, TimeToCounters),

  DirtyCounters2 = sets:add_element({NormalizedTime, Name}, DirtyCounters),

  RetMetrics = Metrics#metrics{time_to_counters = TimeToCounters2,
                               dirty_counters = DirtyCounters2},

  RetState = State#store{metrics = RetMetrics},

  {noreply, RetState}.

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
terminate(_Reason, _State = #store{}) ->
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
  %% Make sure the non-binary orddict has all keys present, or
  %% the binary will take precedence.
  InitializedOrddict = orddict:fold(fun (K, _V, AccIn) ->
                                        case orddict:is_key(K, AccIn) of
                                          false ->
                                            {ok, HistoRef} = hdr_histogram:open(1000000, 3),
                                            orddict:store(K, HistoRef, AccIn);
                                          true ->
                                            AccIn
                                        end
                                    end, TimeToHistos, TimeToBinaryHistos),
  MergeFunc = fun (_K, HistoBinary, HistoRef) ->
                  {ok, HistoRefToMerge} = hdr_histogram:from_binary(HistoBinary),
                  hdr_histogram:add(HistoRef, HistoRefToMerge),
                  hdr_histogram:close(HistoRefToMerge),
                  HistoRef
              end,
  orddict:merge(MergeFunc, TimeToBinaryHistos, InitializedOrddict).


merge_counters(TimeToCounters1, TimeToCounters2) ->
  MergeFunc = fun(_K, Counter1, Counter2) ->
                  Counter1 + Counter2
              end,
  orddict:merge(MergeFunc, TimeToCounters1, TimeToCounters2).


record_gauge_funcs(Metrics = #metrics{time_to_counters = TimeToCounters,
                                      dirty_counters = DirtyCounters},
                   MetricFuns) ->
  Now = os:system_time(seconds),
  NormalizedTime = Now - (round(Now) rem telemetry_config:interval_seconds()),

  {RetCounters2, DirtyCounters2} = maps:fold(fun (Name, Fun, {AccIn, AccDirtyIn}) ->
                                                 Value = Fun(),
                                                 AccCounter = orddict:store({NormalizedTime, Name}, Value, AccIn),
                                                 AccDirty = sets:add_element({NormalizedTime, Name}, AccDirtyIn),
                                                 {AccCounter, AccDirty}
                                             end, {TimeToCounters, DirtyCounters}, MetricFuns),

  Metrics#metrics{time_to_counters = RetCounters2,
                  dirty_counters = DirtyCounters2}.


-spec(export_metrics(#metrics{}) -> #binary_metrics{}).
export_metrics(#metrics{time_to_histos = TimeToHistos,
                        time_to_counters = TimeToCounters,
                        dirty_histos = DirtyHistos,
                        dirty_counters = DirtyCounters}) ->

  RetHistos = orddict:filter(fun ({Time, Name}, _V) ->
                                 sets:is_element({Time, Name}, DirtyHistos)
                             end, TimeToHistos),

  RetHistos2 = orddict:map(fun ({_Time, _Name}, HistoRef) ->
                               hdr_histogram:to_binary(HistoRef)
                           end, RetHistos),

  RetCounters = orddict:filter(fun ({Time, Name}, _V) ->
                                   sets:is_element({Time, Name}, DirtyCounters)
                               end, TimeToCounters),
  IsAggregator = telemetry_config:is_aggregator(),

  ExportedMetrics = #binary_metrics{time_to_binary_histos = RetHistos2,
                                    time_to_counters = RetCounters,
                                    dirty_histos = DirtyHistos,
                                    dirty_counters = DirtyCounters,
                                    is_aggregate = IsAggregator},
  true = ets:insert(snapcache, {last_snap, ExportedMetrics}),
  ExportedMetrics.



submit_to_opentsdb(#metrics{time_to_histos = TimeToHistos,
                            time_to_counters = TimeToCounters,
                            dirty_histos = DirtyHistos,
                            dirty_counters = DirtyCounters}) ->
  %% TODO(tyler) rip out this filthy hack
  Now = os:system_time(seconds),
  NormalizedTime = Now - (round(Now) rem telemetry_config:interval_seconds()),
  Gate = NormalizedTime - telemetry_config:interval_seconds(),

  Counters = orddict:filter(fun (K, _V) ->
                                K > Gate
                            end, TimeToCounters),
  Histos = orddict:filter(fun (K, _V) ->
                              K > Gate
                          end, TimeToHistos),
  Summary = telemetry:metrics_to_summary(#metrics{time_to_histos = Histos,
                                                  time_to_counters = Counters}),
  #{counters := CounterSummary, histograms := HistoSummary} = Summary,
  submit_counters_to_opentsdb(CounterSummary),
  submit_histos_to_opentsdb(HistoSummary),

  ok.


submit_counters_to_opentsdb(Summary) ->
  Metrics = maps:fold(fun (#name_tags{name = Name, tags = Tags}, TimeValue, AccIn) ->
                          maps:fold(fun (Time, Value, SubAccIn) ->
                                        [{Name, Time, Value, Tags} | SubAccIn]
                                    end, AccIn, TimeValue)
                     end, [], Summary),
  gen_opentsdb:put_metric_batch(Metrics).


submit_histos_to_opentsdb(Summary) ->
  Metrics = maps:fold(fun (#name_tags{name = Name, tags = Tags}, TimeValue, AccIn) ->
                          maps:fold(fun (Time, HistoSummary, SubAccIn) ->
                                        maps:fold(fun (SubHistoName, Value, SubSubAccIn) ->
                                                      [{Name, Time, Value, Tags#{histo => SubHistoName}} | SubSubAccIn]
                                                  end, SubAccIn, HistoSummary)
                                    end, AccIn, TimeValue)
                      end, [], Summary),
  gen_opentsdb:put_metric_batch(Metrics).
