%% -*- mode: erlang; tab-width: 4; indent-tabs-mode: 1; st-rulers: [70] -*-
%% vim: ts=4 sw=4 ft=erlang noet
%%%-------------------------------------------------------------------
%%% @author Andrew Bennett <potatosaladx@gmail.com>
%%% @copyright 2018, Andrew Bennett
%%% @doc See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
%%%
%%% @end
%%% Created :  05 February 2018 by Andrew Bennett <potatosaladx@gmail.com>
%%%-------------------------------------------------------------------
-module(librdkafka_config).

%% Public API
-export([transform_kafka_config/1]).
-export([transform_topic_config/1]).

%%%===================================================================
%%% Public API Functions
%%%===================================================================

transform_kafka_config(Config) when is_list(Config) ->
	[transform_kafka_config(K, V) || {K, V} <- Config].

transform_topic_config(Config) when is_list(Config) ->
	[transform_topic_config(K, V) || {K, V} <- Config].

%%%-------------------------------------------------------------------
%%% Internal functions
%%%-------------------------------------------------------------------

%% @private
to_binary(V) when is_binary(V) ->
	V;
to_binary(V) when is_list(V) ->
	erlang:list_to_binary(V);
to_binary(V) when is_atom(V) ->
	erlang:atom_to_binary(V, utf8);
to_binary(V) when is_integer(V) ->
	erlang:integer_to_binary(V);
to_binary(V) when is_float(V) ->
	float_to_bin(V).

%% @private
float_to_bin(Value) ->
	erlang:float_to_binary(Value, [{decimals, 8}, compact]).

%% @private
transform_kafka_config(debug, V) ->
	{<<"debug">>, to_binary(V)};
transform_kafka_config(client_id, V) ->
	{<<"client.id">>, to_binary(V)};
transform_kafka_config(bootstrap_servers, V) ->
	{<<"bootstrap.servers">>, to_binary(V)};
transform_kafka_config(message_max_bytes, V) ->
	{<<"message.max.bytes">>, to_binary(V)};
transform_kafka_config(message_copy_max_bytes, V) ->
	{<<"message.copy.max.bytes">>, to_binary(V)};
transform_kafka_config(receive_message_max_bytes, V) ->
	{<<"receive.message.max.bytes">>, to_binary(V)};
transform_kafka_config(max_in_flight, V) ->
	{<<"max.in.flight">>, to_binary(V)};
transform_kafka_config(metadata_request_timeout_ms, V) ->
	{<<"metadata.request.timeout.ms">>, to_binary(V)};
transform_kafka_config(topic_metadata_refresh_interval_ms, V) ->
	{<<"topic.metadata.refresh.interval.ms">>, to_binary(V)};
transform_kafka_config(metadata_max_age_ms, V) ->
	{<<"metadata.max.age.ms">>, to_binary(V)};
transform_kafka_config(topic_metadata_refresh_fast_interval_ms, V) ->
	{<<"topic.metadata.refresh.fast.interval.ms">>, to_binary(V)};
transform_kafka_config(topic_metadata_refresh_sparse, V) ->
	{<<"topic.metadata.refresh.sparse">>, to_binary(V)};
transform_kafka_config(topic_blacklist, V) ->
	{<<"topic.blacklist">>, to_binary(V)};
transform_kafka_config(socket_timeout_ms, V) ->
	{<<"socket.timeout.ms">>, to_binary(V)};
transform_kafka_config(socket_send_buffer_bytes, V) ->
	{<<"socket.send.buffer.bytes">>, to_binary(V)};
transform_kafka_config(socket_receive_buffer_bytes, V) ->
	{<<"socket.receive.buffer.bytes">>, to_binary(V)};
transform_kafka_config(socket_keepalive_enable, V) ->
	{<<"socket.keepalive.enable">>, to_binary(V)};
transform_kafka_config(socket_nagle_disable, V) ->
	{<<"socket.nagle.disable">>, to_binary(V)};
transform_kafka_config(socket_max_fails, V) ->
	{<<"socket.max.fails">>, to_binary(V)};
transform_kafka_config(broker_address_ttl, V) ->
	{<<"broker.address.ttl">>, to_binary(V)};
transform_kafka_config(broker_address_family, V) ->
	{<<"broker.address.family">>, to_binary(V)};
transform_kafka_config(reconnect_backoff_jitter_ms, V) ->
	{<<"reconnect.backoff.jitter.ms">>, to_binary(V)};
transform_kafka_config(statistics_interval_ms, V) ->
	{<<"statistics.interval.ms">>, to_binary(V)};
transform_kafka_config(log_level, V) ->
	{<<"log_level">>, to_binary(V)};
transform_kafka_config(log_connection_close, V) ->
	{<<"log.connection.close">>, to_binary(V)};
transform_kafka_config(api_version_request, V) ->
	{<<"api.version.request">>, to_binary(V)};
transform_kafka_config(api_version_fallback_ms, V) ->
	{<<"api.version.fallback.ms">>, to_binary(V)};
transform_kafka_config(broker_version_fallback, V) ->
	{<<"broker.version.fallback">>, to_binary(V)};
transform_kafka_config(security_protocol, V) ->
	{<<"security.protocol">>, to_binary(V)};
transform_kafka_config(ssl_cipher_suites, V) ->
	{<<"ssl.cipher.suites">>, to_binary(V)};
transform_kafka_config(ssl_key_location, V) ->
	{<<"ssl.key.location">>, to_binary(V)};
transform_kafka_config(ssl_key_password, V) ->
	{<<"ssl.key.password">>, to_binary(V)};
transform_kafka_config(ssl_certificate_location, V) ->
	{<<"ssl.certificate.location">>, to_binary(V)};
transform_kafka_config(ssl_ca_location, V) ->
	{<<"ssl.ca.location">>, to_binary(V)};
transform_kafka_config(ssl_crl_location, V) ->
	{<<"ssl.crl.location">>, to_binary(V)};
transform_kafka_config(sasl_mechanisms, V) ->
	{<<"sasl.mechanisms">>, to_binary(V)};
transform_kafka_config(sasl_kerberos_service_name, V) ->
	{<<"sasl.kerberos.service.name">>, to_binary(V)};
transform_kafka_config(sasl_kerberos_principal, V) ->
	{<<"sasl.kerberos.principal">>, to_binary(V)};
transform_kafka_config(sasl_kerberos_kinit_cmd, V) ->
	{<<"sasl.kerberos.kinit.cmd">>, to_binary(V)};
transform_kafka_config(sasl_kerberos_keytab, V) ->
	{<<"sasl.kerberos.keytab">>, to_binary(V)};
transform_kafka_config(sasl_kerberos_min_time_before_relogin, V) ->
	{<<"sasl.kerberos.min.time.before.relogin">>, to_binary(V)};
transform_kafka_config(sasl_username, V) ->
	{<<"sasl.username">>, to_binary(V)};
transform_kafka_config(sasl_password, V) ->
	{<<"sasl.password">>, to_binary(V)};
transform_kafka_config(session_timeout_ms, V) ->
	{<<"session.timeout.ms">>, to_binary(V)};
transform_kafka_config(partition_assignment_strategy, V) ->
	{<<"partition.assignment.strategy">>, to_binary(V)};
transform_kafka_config(heartbeat_interval_ms, V) ->
	{<<"heartbeat.interval.ms">>, to_binary(V)};
transform_kafka_config(coordinator_query_interval_ms, V) ->
	{<<"coordinator.query.interval.ms">>, to_binary(V)};
transform_kafka_config(auto_commit_interval_ms, V) ->
	{<<"auto.commit.interval.ms">>, to_binary(V)};
transform_kafka_config(queued_min_messages, V) ->
	{<<"queued.min.messages">>, to_binary(V)};
transform_kafka_config(queued_max_messages_kbytes, V) ->
	{<<"queued.max.messages.kbytes">>, to_binary(V)};
transform_kafka_config(fetch_wait_max_ms, V) ->
	{<<"fetch.wait.max.ms">>, to_binary(V)};
transform_kafka_config(fetch_message_max_bytes, V) ->
	{<<"fetch.message.max.bytes">>, to_binary(V)};
transform_kafka_config(fetch_min_bytes, V) ->
	{<<"fetch.min.bytes">>, to_binary(V)};
transform_kafka_config(fetch_error_backoff_ms, V) ->
	{<<"fetch.error.backoff.ms">>, to_binary(V)};
transform_kafka_config(offset_store_method, V) ->
	{<<"offset.store.method">>, to_binary(V)};
transform_kafka_config(check_crcs, V) ->
	{<<"check.crcs">>, to_binary(V)};
transform_kafka_config(queue_buffering_max_messages, V) ->
	{<<"queue.buffering.max.messages">>, to_binary(V)};
transform_kafka_config(queue_buffering_max_kbytes, V) ->
	{<<"queue.buffering.max.kbytes">>, to_binary(V)};
transform_kafka_config(queue_buffering_max_ms, V) ->
	{<<"queue.buffering.max.ms">>, to_binary(V)};
transform_kafka_config(message_send_max_retries, V) ->
	{<<"message.send.max.retries">>, to_binary(V)};
transform_kafka_config(retry_backoff_ms, V) ->
	{<<"retry.backoff.ms">>, to_binary(V)};
transform_kafka_config(compression_codec, V) ->
	{<<"compression.codec">>, to_binary(V)};
transform_kafka_config(batch_num_messages, V) ->
	{<<"batch.num.messages">>, to_binary(V)};
transform_kafka_config(delivery_report_only_error, V) ->
	{<<"delivery.report.only.error">>, to_binary(V)};
transform_kafka_config(K, V) ->
	throw({error, {options, {K, V}}}).

%% @private
transform_topic_config(request_required_acks, V) ->
	{<<"request.required.acks">>, to_binary(V)};
transform_topic_config(request_timeout_ms, V) ->
	{<<"request.timeout.ms">>, to_binary(V)};
transform_topic_config(message_timeout_ms, V) ->
	{<<"message.timeout.ms">>, to_binary(V)};
transform_topic_config(compression_codec, V) ->
	{<<"compression.codec">>, to_binary(V)};
transform_topic_config(auto_commit_interval_ms, V) ->
	{<<"auto.commit.interval.ms">>, to_binary(V)};
transform_topic_config(auto_offset_reset, V) ->
	{<<"auto.offset.reset">>, to_binary(V)};
transform_topic_config(offset_store_path, V) ->
	{<<"offset.store.path">>, to_binary(V)};
transform_topic_config(offset_store_sync_interval_ms, V) ->
	{<<"offset.store.sync.interval.ms">>, to_binary(V)};
transform_topic_config(offset_store_method, V) ->
	{<<"offset.store.method">>, to_binary(V)};
transform_topic_config(K, V) ->
	throw({error, {options, {K, V}}}).
