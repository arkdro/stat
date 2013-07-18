%%%-------------------------------------------------------------------
%%% @author <arkdro@gmail.com>
%%% @doc
%%% receive gauges {K :: atom(), V :: pos_integer()},
%%% store them to the appropriate ets (based on K),
%%% periodically send statistics for accumulated data and clear ets
%%%
%%% @end
%%% Created :  5 May 2013 by  <arkdro@gmail.com>
%%%-------------------------------------------------------------------
-module(spastat_srv).

-behaviour(gen_server).

%% API
-export([
         funs_status/1,
         start_link/0
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE). 
-define(INTERVAL, 60 * 1000).

-record(state, {
          %% list of funs to be called on flush
          funs = [] :: [{reference(), function()}],
          inc, %% ets for incremented keys
          rel %% ets that contains correspondence between tags and ets tables
         }).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

funs_status(L) ->
    gen_server:cast(?SERVER, {funs_status, L}).

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
init([]) ->
    Rel = ets:new(?MODULE, []),
    Inc = create_inc_table(),
    erlang:send_after(?INTERVAL, self(), flush),
    {ok, #state{rel=Rel, inc=Inc}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast({add_fun, F, Ref}, #state{funs=L} = State) ->
    New = State#state{funs = [{Ref, F} | L]},
    {noreply, New};

handle_cast({funs_status, L}, #state{funs=Funs} = State) ->
    OkRefs = [Ref || {Ref, ok} <- L],
    S = sets:from_list(OkRefs),
    NewFuns = [X || {Ref, _} = X <- Funs, sets:is_element(Ref, S)],
    {noreply, State#state{funs=NewFuns}};

handle_cast({gauge, K, V}, State) ->
    gauge(State, K, V),
    {noreply, State};

handle_cast({timing, K, Dur}, State) ->
    gauge(State, K, Dur),
    {noreply, State};

handle_cast({inc, K}, State) ->
    inc(State, K),
    {noreply, State};

handle_cast(_Msg, State) ->
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
handle_info(flush, State) ->
    New = flush_stat(State),
    erlang:send_after(?INTERVAL, self(), flush),
    {noreply, New};

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
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

gauge(#state{rel=Rel}, Key, Val) ->
    Tab = ensure_table(Rel, Key),
    ets:insert(Tab, {Val}).

inc(#state{inc=Tab}, Key) ->
    case ets:lookup(Tab, Key) of
        [{Key, Val}] ->
            ets:insert(Tab, {Key, Val + 1});
        [] ->
            ets:insert(Tab, {Key, 1})
    end.

ensure_table(Rel, Key) ->
    case ets:lookup(Rel, Key) of
        [{_, Tab}] ->
            Tab;
        [] ->
            Tab = ets:new(?MODULE, [duplicate_bag]),
            ets:insert(Rel, {Key, Tab}),
            Tab
    end.

flush_stat(State) ->
    flush_stat_gauge(State),
    flush_stat_inc(State).

flush_stat_inc(#state{inc=Tab, funs=Funs} = State) ->
    Pid = spastat_flusher:get_pid(),
    ets:give_away(Tab, Pid, {flush_inc, Tab, Funs}),
    New = create_inc_table(),
    State#state{inc=New}.

flush_stat_gauge(#state{rel=Rel, funs=Funs}) ->
    Flusher = spastat_flusher:get_pid(),
    [transfer_one_tab(Flusher, X, Funs) || X <- ets:tab2list(Rel)],
    ets:delete_all_objects(Rel).

transfer_one_tab(Pid, {Key, Tab}, Funs) ->
    ets:give_away(Tab, Pid, {flush, Key, Tab, Funs}).

create_inc_table() ->
    ets:new(?MODULE, []).

