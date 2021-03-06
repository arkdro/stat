%%%-------------------------------------------------------------------
%%% @author <arkdro@gmail.com>
%%% @doc
%%% flush statistics to somewhere
%%%
%%% @end
%%% Created :  9 May 2013 by  <arkdro@gmail.com>
%%%-------------------------------------------------------------------
-module(spastat_flush_util).

%% API
-export([
         flush_inc_tab/2,
         flush_tab/3
        ]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

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

-spec flush_tab(term(), atom() | non_neg_integer(), [function()]) -> true.

flush_tab(Key, Tab, Funs) ->
    Res = get_numbers(Tab),
    Props = create_result(Key, Res),
    error_logger:info_report({flush_tab, Props}),
    ResFuns = [{Ref, catch X(Props)} || {Ref, X} <- Funs],
    spastat_srv:funs_status(ResFuns),
    ets:delete(Tab).

-spec flush_inc_tab(atom() | non_neg_integer(), [function()]) -> true.

flush_inc_tab(Tab, Funs) ->
    case ets:info(Tab, size) of
        0 ->
            skip;
        _ ->
            flush_inc_tab2(Tab, Funs)
    end,
    ets:delete(Tab).

%%%===================================================================
%%% Internal functions
%%%===================================================================

flush_inc_tab2(Tab, Funs) ->
    F = fun(X, Acc) ->
                flush_one_inc_item(X, Acc, Funs)
        end,
    ets:foldl(F, true, Tab).

flush_one_inc_item({K, V}, Acc, Funs) ->
    Props = [{key, K},
             {size, V}],
    error_logger:info_report({flush_inc_tab, Props}),
    ResFuns = [{Ref, catch X(Props)} || {Ref, X} <- Funs],
    spastat_srv:funs_status(ResFuns),
    Acc.

create_result(Key, Res) ->
    Tags = record_info(fields, acc),
    [_ | Data] = tuple_to_list(Res),
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
    Max = get_max(Sorted),
    Sum = lists:foldl(fun(X, Acc) -> X + Acc end, 0, Sorted),
    P = [{X, get_percentile(Size, Sorted, X)} || X <- ?PERCENTILES],
    Avg = get_average(Size, Sum),
    #acc{
          size = Size,
          avg = Avg,
          min = Min,
          max = Max,
          sum = Sum,
          perc = P
        }.

get_percentile(0, _, _) ->
    undefined;
get_percentile(Size, List, Perc) ->
    N = round(Size * Perc / 100 + 0.5),
    lists:nth(N, List).

get_max([]) ->
    undefined;
get_max(Sorted) ->
    lists:last(Sorted).

get_min([]) ->
    undefined;
get_min([H | _]) ->
    H.

get_data(Tab) ->
    L = [X || {X} <- ets:tab2list(Tab)],
    Sorted = lists:sort(L),
    Size = ets:info(Tab, size),
    {Size, Sorted}.

%%%===================================================================
%%% Unit tests
%%%===================================================================

-ifdef(TEST).

util_test_() ->
    [{"Percentiles tests",
      [
       ?_test(
          begin
              ?assertMatch(1, get_min([1,2,5,61]))
          end),
       ?_test(
          begin
              ?assertMatch(6, get_percentile(10, lists:seq(1, 10), 50))
          end)
      ]},
     {"Collect numbers tests",
      [
       ?_test(
          begin
              Size = 20,
              Sorted = [X*3+1 || X <- lists:seq(1, Size)],
              %% [4,7,10,13,16,19, 22,25,28,31,34, 37,40,43,46,49, 52,55,58, 61]
              Res = collect_numbers({20, Sorted}),
              Exp = #acc{
                size = 20,
                sum = 650,
                min = 4,
                max = 61,
                avg = 32.5,
                perc = [{25, 19}, {50, 34}, {75, 49}, {90, 58}, {98, 61}]
                },
              ?assertMatch(Exp, Res)
          end)
      ]}
    ].

-endif.

