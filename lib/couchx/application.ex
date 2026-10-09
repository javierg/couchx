defmodule Couchx.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {Registry, keys: :unique, name: Couchx.Pool.Registry},
      {Registry, keys: :unique, name: Couchx.DynamicRepo.Registry},
      {DynamicSupervisor, strategy: :one_for_one, name: Couchx.DynamicRepoSupervisor},
      Couchx.DynamicRepo.Janitor
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: Couchx.Supervisor)
  end
end
