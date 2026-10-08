defmodule Couchx.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {Registry, keys: :unique, name: Couchx.Pool.Registry}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: Couchx.Supervisor)
  end
end
