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
-module(librdkafka_c).
-behaviour(gen_statem).

%% API
-export([start_link/0]).
%% gen_statem callbacks
-export([callback_mode/0]).
-export([init/1]).
-export([handle_event/4]).

start_link() ->
	gen_statem:start_link(?MODULE, {<<"dcm-test-group">>, [<<"dcm_test">>], [{<<"bootstrap.servers">>, <<"kafka:9092">>}], []}, []).

%%%===================================================================
%%% gen_statem callbacks
%%%===================================================================

callback_mode() ->
	[handle_event_function, state_enter].

init({GroupId, Topics, KafkaConfig, TopicConfig}) ->
	Consumer = librdkafka_nif:consumer_new(GroupId, Topics, KafkaConfig, TopicConfig),
	ok = librdkafka_nif:consumer_select(Consumer),
	Data = #{
		consumer => Consumer,
		queues => #{}
	},
	{ok, nil, Data}.

%% State Enter Events
handle_event(enter, nil, nil, _Data) ->
	keep_state_and_data;
%% Info Events
handle_event(info, {select, Consumer, undefined, ready_input}, nil, _Data = #{ consumer := Consumer }) ->
	ok = librdkafka_nif:consumer_poll(Consumer),
	ok = librdkafka_nif:consumer_select(Consumer),
	keep_state_and_data;
handle_event(info, {select, Queue, Consumer, ready_input}, nil, _Data = #{ consumer := Consumer, queues := Queues }) ->
	[] = maps:get(Queue, Queues),
	ok = librdkafka_nif:queue_poll(Queue),
	ok = librdkafka_nif:queue_select(Queue),
	keep_state_and_data;
handle_event(info, {kafka, Consumer, {rebalance, assign_partitions, Partitions}}, nil, Data0 = #{ consumer := Consumer, queues := Queues0 }) ->
	Queues1 = lists:foldl(fun ({Topic, Partition, Offset, Queue}, Queues) when is_reference(Queue) ->
		io:format("rebalance:assign_partitions ~p~n", [{Topic, Partition, Offset, Queue}]),
		ok = librdkafka_nif:queue_select(Queue),
		maps:put(Queue, [], Queues)
	end, Queues0, Partitions),
	Data1 = Data0#{ queues := Queues1 },
	{keep_state, Data1};
handle_event(info, Info, nil, _Data) ->
	io:format("Info = ~p~n", [Info]),
	keep_state_and_data.

%%%-------------------------------------------------------------------
%%% Internal functions
%%%-------------------------------------------------------------------
