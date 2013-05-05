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
-module(stat_srv).

-behaviour(gen_server).

%% API
-export([
         gauge/2,
         start_link/0
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE). 
-define(INTERVAL, 1000).

-record(state, {
          rel %% ets that contains correspondence between tags and ets tables
         }).

%%%===================================================================
%%% API
%%%===================================================================

gauge(K, V) ->
    gen_server:cast(?SERVER, {gauge, K, V}).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
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
init([]) ->
    Rel = ets:new(?MODULE, []),
    erlang:send_after(?INTERVAL, self(), flush),
    {ok, #state{rel=Rel}}.

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
handle_cast({gauge, K, V}, State) ->
    gauge(State, K, V),
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
    flush_stat(State),
    erlang:send_after(?INTERVAL, self(), flush),
    {noreply, State};

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

ensure_table(Rel, Key) ->
    case ets:lookup(Rel, Key) of
        [{_, Tab}] ->
            Tab;
        [] ->
            Tab = ets:new(?MODULE, [duplicate_bag]),
            ets:insert(Rel, {Key, Tab}),
            Tab
    end.

flush_stat(#state{rel=Rel}) ->
    ets:foldl(fun flush_one_key/2, true, Rel).

flush_one_key({Key, Tab}, Acc) ->
    Size = ets:info(Tab, size),
    {Sum, Min, Max} = sum_one_tab(Tab),
    Avg = get_average(Size, Sum),
    Props = [{tab, Key},
             {size, Size},
             {min, Min},
             {max, Max},
             {sum, Sum},
             {avg, Avg}],
    error_logger:info_report({flush_tab, Props}),
    ets:delete_all_objects(Tab),
    Acc.

sum_one_tab(Tab) ->
    F = fun({X}, {Sum, Min, Max}) ->
                NewSum = Sum + X,
                NewMin = erlang:min(Min, X),
                NewMax = erlang:max(Max, X),
                {NewSum, NewMin, NewMax}
        end,
    ets:foldl(F, {0, 0, 0}, Tab).

get_average(0, _) ->
    infinity;
get_average(Size, Sum) ->
    Sum / Size.

