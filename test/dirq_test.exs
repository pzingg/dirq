defmodule DirqTest do
  use ExUnit.Case
  doctest Dirq

  test "greets the world" do
    assert Dirq.hello() == :world
  end
end
