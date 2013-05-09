%%%-------------------------------------------------------------------
%%% @author <arkdro@gmail.com>
%%% @doc
%%% flush statistics to somewhere
%%%
%%% @end
%%% Created :  9 May 2013 by  <arkdro@gmail.com>
%%%-------------------------------------------------------------------
-module(stat_flush_util).

%% API
-export([
         flush_tab/2
        ]).

-define(PERCENTILES, [25, 50, 75, 90, 98]).

-record(acc,{
          size = 0  :: integer(),
          sum       :: undefined | integer(),
          min       :: undefined | integer(),
          max       :: undefined | integer(),
          avg       :: undefined | integer(),
          perc = [] :: [{integer(), term()}] %% list of percentiles
         }).

%%%===================================================================
%%% API
%%%===================================================================

-spec flush_tab(term(), non_neg_integer()) -> true.

flush_tab(Key, Tab) ->
    Res = get_numbers(Tab),
    Props = create_result(Key, Res),
    error_logger:info_report({flush_tab, Props}),
    ets:delete(Tab).

%%%===================================================================
%%% Internal functions
%%%===================================================================

create_result(Key, Res) ->
    Tags = record_info(fields, acc),
    Data = tuple_to_list(Res),
    Props = lists:zip(Tags, Data),
    [{key, Key} | Props].

get_average(0, _) ->
    undefined;
get_average(Size, Sum) ->
    Sum / Size.

get_numbers(Tab) ->
    Data = get_data(Tab),
    collect_numbers(Data).

collect_numbers({Size, Sorted}) ->
    Min = get_min(Sorted),
    Idx = get_percentile_indices(Size),
    Res = collect_numbers(0, Sorted, Idx, #acc{size=Size, min=Min}),
    Avg = get_average(Size, Res#acc.sum),
    Res#acc{avg=Avg}.

collect_numbers(_I, [], Idx, Res) ->
    move_percentiles(Idx, Res);
collect_numbers(I, [H|T], PercIdx, #acc{sum=Sum} = Acc) ->
    NewPercIdx = fill_percentile(I, H, PercIdx),
    NewAcc = Acc#acc{max=H, sum=Sum+H},
    collect_numbers(I+1, T, NewPercIdx, NewAcc).

move_percentiles({_, Idx}, Res) ->
    L = dict:to_list(Idx),
    Res#acc{perc=lists:sort(L)}.

fill_percentile(_I, _Val, {[], _Idx} = PercIdx) ->
    PercIdx;
fill_percentile(I, Val, {[H | T], IdxData}) when I =< H ->
    NewData = dict:store(H, Val, IdxData),
    {T, NewData};
fill_percentile(_, _, PercIdx) ->
    PercIdx.

get_percentile_indices(Size) ->
    F = fun(X, Acc) ->
                Idx = round(Size * X / 100 + 0.5),
                dict:store(X, Idx, Acc)
                end,
    {?PERCENTILES, lists:foldl(F, dict:new(), ?PERCENTILES)}.

get_min([]) ->
    undefined;
get_min([H | _]) ->
    H.

get_data(Tab) ->
    L = [X || {X} <- ets:tab2list(Tab)],
    Sorted = lists:sort(L),
    Size = ets:info(Tab, size),
    {Size, Sorted}.

