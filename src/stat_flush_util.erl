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
    InitSum = init_sum(Sorted),
    Idx = get_percentile_indices(Size),
    Res = collect_numbers(1, Sorted, Idx, dict:new(),
                          #acc{size=Size, min=Min, sum=InitSum}),
    Avg = get_average(Size, Res#acc.sum),
    Res#acc{avg=Avg}.

collect_numbers(_I, [], Idx, Perc, Res) ->
    move_percentiles(Perc, Res);
collect_numbers(I, [H|T], Idx, PercAcc, #acc{sum=Sum} = Acc) ->
    {NewPercIdx, NewPercAcc} = fill_percentile(I, H, Idx, PercAcc),
    NewAcc = Acc#acc{max=H, sum=Sum+H},
    collect_numbers(I+1, T, NewPercIdx, NewPercAcc, NewAcc).

move_percentiles(Idx, Res) ->
    L = dict:to_list(Idx),
    Res#acc{perc=lists:sort(L)}.

fill_percentile(_I, _Val, {[], []}, PercAcc) ->
    {{[], []}, PercAcc};
fill_percentile(I, Val, {[H | T] = IdxPercList, [H2 | T2] = IdxList}, IdxData) when I =< H2 ->
    NewData = dict:store(H, Val, IdxData),
    {{IdxPercList, IdxList}, NewData};
fill_percentile(I, Val, {[H | T], [H2 | T2]}, IdxData) ->
    fill_percentile(I, Val, {T, T2}, IdxData).

get_percentile_indices(Size) ->
    {?PERCENTILES, [round(Size * X / 100 + 0.5) || X <- ?PERCENTILES]}.

init_sum([]) ->
    undefined;
init_sum(_) ->
    0.

get_min([]) ->
    undefined;
get_min([H | _]) ->
    H.

get_data(Tab) ->
    L = [X || {X} <- ets:tab2list(Tab)],
    Sorted = lists:sort(L),
    Size = ets:info(Tab, size),
    {Size, Sorted}.

