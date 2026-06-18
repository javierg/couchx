defmodule Couchx.CouchId do
  @moduledoc """
  Helper module to return doc information
  """

  def base_id(id), do: String.replace(id, ~r{^([^/]+/)}, "")
  def underscore_school_id(school_id), do: String.replace(school_id, "/", "_")
end
