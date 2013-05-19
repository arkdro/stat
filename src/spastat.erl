-module(spastat).

%% API for spastat_srv
-export([
         inc/1, %% k
         gauge/2
        ]).

-define(SERVER, spastat_srv).

gauge(K, V) ->
    gen_server:cast(?SERVER, {gauge, K, V}).

inc(K) ->
    gen_server:cast(?SERVER, {inc, K}).

