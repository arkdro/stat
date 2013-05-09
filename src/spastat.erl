-module(spastat).

%% API for stat_srv
-export([
         gauge/2,
        ]).

-define(SERVER, stat_srv).

gauge(K, V) ->
    gen_server:cast(?SERVER, {gauge, K, V}).

