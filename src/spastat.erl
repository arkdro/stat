-module(spastat).

%% API for spastat_srv
-export([
         gauge/2
        ]).

-define(SERVER, spastat_srv).

gauge(K, V) ->
    gen_server:cast(?SERVER, {gauge, K, V}).

