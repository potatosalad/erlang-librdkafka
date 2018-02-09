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
-module(librdkafka_nif).

%% NIF
-export([kafka_config_dump/1]).
-export([topic_config_dump/1]).
-export([kafka_config_fetch/2]).
-export([topic_config_fetch/2]).
-export([consumer_new/4]).
-export([consumer_forward/2]).
-export([consumer_select/1]).
-export([consumer_poll/1]).
-export([queue_forward/2]).
-export([queue_select/1]).
-export([queue_poll/1]).

-on_load(init/0).

%%%===================================================================
%%% NIF Functions
%%%===================================================================

kafka_config_dump(_Ref) ->
	erlang:nif_error({nif_not_loaded, ?MODULE}).

topic_config_dump(_Ref) ->
	erlang:nif_error({nif_not_loaded, ?MODULE}).

kafka_config_fetch(_Ref, _Name) ->
	erlang:nif_error({nif_not_loaded, ?MODULE}).

topic_config_fetch(_Ref, _Name) ->
	erlang:nif_error({nif_not_loaded, ?MODULE}).

consumer_new(_GroupId, _Topics, _KafkaConfig, _TopicConfig) ->
	erlang:nif_error({nif_not_loaded, ?MODULE}).

consumer_forward(_ConsumerRef, _ToPid) ->
	erlang:nif_error({nif_not_loaded, ?MODULE}).

consumer_select(_ConsumerRef) ->
	erlang:nif_error({nif_not_loaded, ?MODULE}).

consumer_poll(_ConsumerRef) ->
	erlang:nif_error({nif_not_loaded, ?MODULE}).

queue_forward(_QueueRef, _ToPid) ->
	erlang:nif_error({nif_not_loaded, ?MODULE}).

queue_select(_QueueRef) ->
	erlang:nif_error({nif_not_loaded, ?MODULE}).

queue_poll(_QueueRef) ->
	erlang:nif_error({nif_not_loaded, ?MODULE}).

%%%-------------------------------------------------------------------
%%% Internal functions
%%%-------------------------------------------------------------------

%% @private
init() ->
	SoName = filename:join(librdkafka:priv_dir(), ?MODULE_STRING),
	erlang:load_nif(SoName, 0).
