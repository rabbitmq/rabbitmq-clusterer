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

-module(rabbit_clusterer_comms_sup).

-behaviour(supervisor).

-export([start_link/0, start_comms/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

start_comms() ->
    {ok, _Pid, Token} = supervisor:start_child(?SERVER, []),
    {ok, Token}.

%%----------------------------------------------------------------------------

init([]) ->
    {ok, {{simple_one_for_one, 10, 10},
          [{comms, {rabbit_clusterer_comms, start_link, []},
            temporary, 16#ffffffff, worker, [rabbit_clusterer_comms]}]}}.
