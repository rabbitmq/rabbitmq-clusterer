%% The contents of this file are subject to the Mozilla Public License 
%% Version 1.1 (the "License"); you may not use this file except in 
%% compliance with the License. You may obtain a copy of the License at 
%% http://www.mozilla.org/MPL/1.1/ 
%%
%% Software distributed under the License is distributed on an "AS IS" 
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the 
%% License for the specific language governing rights and limitations 
%% under the License. 
%%
%% The Original Code is RabbitMQ. 
%%
%% The Initial Developer of the Original Code is Pivotal Software, Inc. 
%% Portions created by the Initial Developer are Copyright (C) 2013-2015 
%% Pivotal Software, Inc. All Rights Reserved.

-module(rabbit_clusterer_sup).

-behaviour(supervisor2).

-export([start_link/0, init/1]).

start_link() ->
    supervisor2:start_link(?MODULE, []).

init([]) ->
    {ok, {{one_for_all, 0, 1},
          [{rabbit_clusterer_comms_sup,
            {rabbit_clusterer_comms_sup, start_link, []},
            intrinsic, infinity, supervisor, [rabbit_clusterer_comms_sup]},
           {rabbit_clusterer_coordinator,
            {rabbit_clusterer_coordinator, start_link, []},
            intrinsic, 16#ffffffff, worker, [rabbit_clusterer_coordinator]}]}}.
