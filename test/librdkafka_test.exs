defmodule LibrdkafkaTest do
  use ExUnit.Case
  doctest Librdkafka

  test "greets the world" do
    assert Librdkafka.hello() == :world
  end
end
