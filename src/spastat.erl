-module(spastat).

%% API for spastat_srv
-export([
         inc/1, %% k
         timing/2, %% k, T1
         gauge/2
        ]).

-define(SERVER, spastat_srv).

gauge(K, V) ->
    gen_server:cast(?SERVER, {gauge, K, V}).

inc(K) ->
    gen_server:cast(?SERVER, {inc, K}).

timing(K, T1) ->
    T2 = os:timestamp(),
    gen_server:cast(?SERVER, {timing, K, timer:now_diff(T2, T1)}).

