%%%-------------------------------------------------------------------
%%% @author <arkdro@gmail.com>
%%% @doc
%%% flush statistics to somewhere
%%%
%%% @end
%%% Created :  9 May 2013 by  <arkdro@gmail.com>
%%%-------------------------------------------------------------------
-module(stat_flusher).

-behaviour(gen_server).

%% API
-export([
         get_pid/0,
         start_link/0
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE). 
-define(INTERVAL, 60 * 1000).

-record(state, {
          rel %% ets that contains correspondence between tags and ets tables
         }).

%%%===================================================================
%%% API
%%%===================================================================

get_pid() ->
    gen_server:call(?SERVER, get_pid).

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
handle_call(get_pid, _From, State) ->
    {reply, self(), State};

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
handle_info({'ETS-TRANSFER', Tab, _FromPid, {flush, Key, Tab}}, State) ->
    flush_tab(Key, Tab),
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

flush_tab(Key, Tab) ->
    Size = ets:info(Tab, size),
    {Sum, Min, Max} = sum_one_tab(Size, Tab),
    Avg = get_average(Size, Sum),
    {Med, Perc25, Perc75, Perc90} = get_percentiles(Tab),
    Props = [{tab, Key},
             {size, Size},
             {min, Min},
             {max, Max},
             {pct25, Perc25},
             {med, Med},
             {pct75, Perc75},
             {pct90, Perc90},
             {sum, Sum},
             {avg, Avg}],
    error_logger:info_report({flush_tab, Props}),
    ets:delete(Tab).

sum_one_tab(0, _) ->
    {undefined, undefined, undefined};
sum_one_tab(_, Tab) ->
    Min0 = ets:first(Tab),
    F = fun({X}, {Sum, Min, Max}) ->
                NewSum = Sum + X,
                NewMin = erlang:min(Min, X),
                NewMax = erlang:max(Max, X),
                {NewSum, NewMin, NewMax}
        end,
    ets:foldl(F, {0, Min0, 0}, Tab).

get_average(0, _) ->
    undefined;
get_average(Size, Sum) ->
    Sum / Size.

get_percentiles(Tab) ->
    Data = get_data(Tab),
    Med = get_median(Data),
    Perc25 = get_percentile(25, Data),
    Perc75 = get_percentile(75, Data),
    Perc90 = get_percentile(90, Data),
    {Med, Perc25, Perc75, Perc90}.

get_median(Data) ->
    get_percentile(50, Data).

get_percentile(_Perc, {_Size, []}) ->
    undefined;
get_percentile(Perc, {Size, Sorted}) ->
    %% do not bother with exact math
    Num = round(Size * Perc / 100 + 0.5),
    lists:nth(Num, Sorted).

get_data(Tab) ->
    L = [X || {X} <- ets:tab2list(Tab)],
    Sorted = lists:sort(L),
    Size = ets:info(Tab, size),
    {Size, Sorted}.

