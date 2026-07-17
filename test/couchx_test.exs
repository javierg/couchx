defmodule CouchxTest do
  use ExUnit.Case
  doctest Couchx

  test "module is available" do
    assert Code.ensure_loaded?(Couchx)
  end
end
