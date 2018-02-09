%% -*- mode: erlang; tab-width: 4; indent-tabs-mode: 1; st-rulers: [70] -*-
%% vim: ts=4 sw=4 ft=erlang noet
%%%-------------------------------------------------------------------
%%% @author Andrew Bennett <potatosaladx@gmail.com>
%%% @copyright 2018, Andrew Bennett
%%% @doc
%%%
%%% @end
%%% Created :  06 February 2018 by Andrew Bennett <potatosaladx@gmail.com>
%%%-------------------------------------------------------------------
-module(librdkafka_queue).
-behaviour(gen_statem).

%% API
-export([start_link/5]).
%% gen_statem callbacks
-export([callback_mode/0]).
-export([init/1]).
-export([handle_event/4]).

start_link(Consumer, Topic, Partition, Offset, Queue) ->
	gen_statem:start_link(?MODULE, {Consumer, Topic, Partition, Offset, Queue}, []).

%%%===================================================================
%%% gen_statem callbacks
%%%===================================================================

callback_mode() ->
	[handle_event_function, state_enter].

init({Consumer, Topic, Partition, Offset, Queue}) ->
	% Consumer = librdkafka_nif:consumer_new(GroupId, Topics, KafkaConfig, TopicConfig),
	% ok = librdkafka_nif:consumer_select(Consumer),
	Data = #{
		consumer => Consumer,
		topic => Topic,
		partition => Partition,
		offset => Offset,
		queue => Queue
	},
	{ok, unforwarded, Data}.

%% State Enter Events
handle_event(enter, unforwarded, unforwarded, _Data) ->
	Actions = [{state_timeout, 1000, die}],
	{keep_state_and_data, Actions};
handle_event(enter, unforwarded, forwarded, _Data = #{ queue := Queue }) ->
	ok = librdkafka_nif:queue_select(Queue),
	keep_state_and_data;
%% Info Events
handle_event(info, {select, Queue, Consumer, ready_input}, forwarded, _Data = #{ consumer := Consumer, queue := Queue }) ->
	ok = librdkafka_nif:queue_poll(Queue),
	ok = librdkafka_nif:queue_select(Queue),
	keep_state_and_data;
handle_event(info, {forward, Queue}, unforwarded, Data = #{ queue := Queue }) ->
	io:format("forwarded queue = ~p~n", [Queue]),
	{next_state, forwarded, Data};
handle_event(info, Info, State, _Data) ->
	io:format("State = ~p, Info = ~p~n", [State, Info]),
	keep_state_and_data.

%%%-------------------------------------------------------------------
%%% Internal functions
%%%-------------------------------------------------------------------
