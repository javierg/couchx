defmodule Couchx.Pool do
  @moduledoc """
  Per-repo HTTP connection pool, backed by a dedicated `Finch` instance.

  Requests run in the caller's process through Req, using this repo's Finch
  pool. There are no worker GenServers: `pool_size` (default 10) is the
  number of HTTP connections to CouchDB, and callers beyond that wait for a
  free connection for up to `pool_timeout` milliseconds (default 5_000).

  The pool supervisor pid is what Ecto stores as the repo `:pid`. Its
  connection settings (base URL, headers, Finch name) are kept in
  `Couchx.Pool.Registry`, so resolving a pool never messages a process.

  When the config has a `:name`, the pool is registered in `CouchxRegistry`
  under that name.

  Config options:

    * `:pool_size` - connections in the pool, default `10`
    * `:pool_count` - number of Finch pool shards, default `1`
    * `:connect_timeout` - TCP connect timeout in ms, default `30_000`
  """

  use Supervisor

  @default_pool_size 10
  @default_connect_timeout 30_000
  @registry Couchx.Pool.Registry

  def start_link(config) do
    Supervisor.start_link(__MODULE__, config, name_opts(config[:name]))
  end

  def child_spec(config) do
    %{
      id: config[:id] || CouchxAdapter,
      start: {__MODULE__, :start_link, [config]},
      restart: :permanent,
      shutdown: :infinity,
      type: :supervisor
    }
  end

  @impl true
  def init(config) do
    finch = finch_name(config)

    conn = %{
      finch: finch,
      base_url: base_url(config),
      headers: headers(config)
    }

    {:ok, _} = Registry.register(@registry, self(), conn)

    finch_pool = [
      size: pool_size(config),
      count: config[:pool_count] || 1,
      conn_opts: [transport_opts: [timeout: config[:connect_timeout] || @default_connect_timeout]]
    ]

    # Finch is a supervisor but its child_spec doesn't say so; declaring it
    # makes shutdown wait for in-flight connections to close.
    finch_spec =
      Supervisor.child_spec({Finch, name: finch, pools: %{default: finch_pool}},
        type: :supervisor,
        shutdown: :infinity
      )

    children = [finch_spec]

    Supervisor.init(children, strategy: :one_for_one)
  end

  def pool_size(config) do
    case config[:pool_size] do
      size when is_integer(size) and size > 0 ->
        size

      nil ->
        @default_pool_size

      other ->
        raise ArgumentError,
              "Couchx :pool_size must be a positive integer, got: #{inspect(other)}"
    end
  end

  @doc """
  Returns the connection settings (`:finch`, `:base_url`, `:headers`) for a
  pool given as a pid, registered name or `{:via, ...}` tuple.

  Exits with `:noproc` if no pool is running there, like `GenServer.call/3`.
  """
  def lookup(server) do
    with pid when is_pid(pid) <- GenServer.whereis(server),
         [{^pid, conn}] <- Registry.lookup(@registry, pid) do
      conn
    else
      _ -> exit({:noproc, {__MODULE__, :lookup, [server]}})
    end
  end

  defp finch_name(config) do
    case config[:name] || config[:repo] do
      name when is_atom(name) and not is_nil(name) -> Module.concat([__MODULE__, Finch, name])
      _ -> Module.concat([__MODULE__, Finch, "anon#{System.unique_integer([:positive])}"])
    end
  end

  defp base_url(config) do
    base = "#{config[:protocol]}://#{config[:hostname]}:#{config[:port]}"
    database = config[:database] || ""

    String.trim_trailing(base, "/") <> "/" <> String.trim_leading(to_string(database), "/")
  end

  defp headers(config) do
    credentials = Base.encode64("#{config[:username]}:#{config[:password]}")

    [
      {"Content-Type", "application/json"},
      {"Authorization", "Basic #{credentials}"}
    ]
  end

  defp name_opts(nil), do: []
  defp name_opts(name), do: [name: {:via, Registry, {CouchxRegistry, name}}]
end
