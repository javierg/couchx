defmodule CouchxTest do
  use ExUnit.Case
  doctest Couchx

  test "greets the world" do
    assert Couchx.hello() == :world
  end
end
