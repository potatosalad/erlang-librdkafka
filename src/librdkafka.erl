%% -*- mode: erlang; tab-width: 4; indent-tabs-mode: 1; st-rulers: [70] -*-
%% vim: ts=4 sw=4 ft=erlang noet
%%%-------------------------------------------------------------------
%%% @author Andrew Bennett <potatosaladx@gmail.com>
%%% @copyright 2018, Andrew Bennett
%%% @doc
%%%
%%% @end
%%% Created :  02 February 2018 by Andrew Bennett <potatosaladx@gmail.com>
%%%-------------------------------------------------------------------
-module(librdkafka).

%% Public API
% -export([kafka_config/1]).
% -export([topic_config/1]).
%% Internal API
-export([priv_dir/0]).

%% Types

%%%===================================================================
%%% Public API Functions
%%%===================================================================

% kafka_config(Config0) ->
% 	Config1 = librdkafka_config:transform_kafka_config(Config0),
% 	librdkafka_nif:kafka_config(Config1).

% topic_config(Config0) ->
% 	Config1 = librdkafka_config:transform_topic_config(Config0),
% 	librdkafka_nif:topic_config(Config1).

%%%===================================================================
%%% Internal API Functions
%%%===================================================================

-spec priv_dir() -> file:filename_all().
priv_dir() ->
	case code:priv_dir(?MODULE) of
		{error, bad_name} ->
			case code:which(?MODULE) of
				Filename when is_list(Filename) ->
					filename:join([filename:dirname(Filename), "../priv"]);
				_ ->
					"../priv"
			end;
		Dir ->
			Dir
	end.

%%%-------------------------------------------------------------------
%%% Internal functions
%%%-------------------------------------------------------------------
