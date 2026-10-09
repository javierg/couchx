defmodule Couchx.DynamicRepo do
  @moduledoc """
  Runs repo calls against a dynamically started repo.

  A dynamic repo is identified by its name together with the options it's
  started with (credentials, database, ...). The first `run/1` or
  `with_dynamic_repo/3` for a combination starts a repo under
  `Couchx.DynamicRepoSupervisor` and leaves it running, so later calls with
  the same name and options reuse its connection pool. Calls with the same
  name but different options get their own repo, so each database can use its
  own credentials.

  A repo with no running callbacks is stopped after it has been idle for
  `:dynamic_repo_idle_timeout` (default 5 minutes, `:infinity` to disable):

      config :couchx, dynamic_repo_idle_timeout: :timer.minutes(10)

  Stop repos earlier with `stop_repo/1` (every repo with that name) or
  `stop_repo/2` (only the one started with those options).
  """

  @registry Couchx.DynamicRepo.Registry

  @doc false
  def repo_key(module, name, opts) do
    digest = :crypto.hash(:sha256, :erlang.term_to_binary(Enum.sort(opts)))
    {module, to_string(name), digest}
  end

  @doc false
  def with_repo(module, name, opts, callback) do
    repo = checkout(module, name, opts)
    default_dynamic_repo = module.get_dynamic_repo()

    try do
      module.put_dynamic_repo(repo)
      callback.()
    after
      module.put_dynamic_repo(default_dynamic_repo)
      Couchx.DynamicRepo.Janitor.checkin(repo)
    end
  end

  defp checkout(module, name, opts) do
    pid = ensure_started(module, name, opts)

    case Couchx.DynamicRepo.Janitor.checkout(pid) do
      :ok ->
        if Process.alive?(pid) do
          pid
        else
          Couchx.DynamicRepo.Janitor.checkin(pid)
          checkout(module, name, opts)
        end

      :closing ->
        await_down(pid)
        checkout(module, name, opts)
    end
  end

  defp await_down(pid) do
    ref = Process.monitor(pid)

    receive do
      {:DOWN, ^ref, _, _, _} -> :ok
    end
  end

  @doc false
  def ensure_started(module, name, opts) do
    key = repo_key(module, name, opts)

    case Registry.lookup(@registry, key) do
      [{pid, _}] -> pid
      [] -> start(module, key, opts)
    end
  end

  @doc false
  def stop(module, name) do
    name = to_string(name)

    @registry
    |> Registry.select([{{{module, name, :_}, :"$1", :_}, [], [:"$1"]}])
    |> Enum.each(&terminate/1)
  end

  @doc false
  def stop(module, name, opts) do
    with [{pid, _}] <- Registry.lookup(@registry, repo_key(module, name, opts)) do
      terminate(pid)
    end

    :ok
  end

  defp start(module, key, opts) do
    via = {:via, Registry, {@registry, key}}
    spec = Supervisor.child_spec({module, [name: via] ++ opts}, restart: :transient)

    case DynamicSupervisor.start_child(Couchx.DynamicRepoSupervisor, spec) do
      {:ok, pid} -> pid
      {:error, {:already_started, pid}} -> pid
    end
  end

  defp terminate(pid), do: DynamicSupervisor.terminate_child(Couchx.DynamicRepoSupervisor, pid)

  defmacro __using__(otp_app: otp_app, name: name) do
    quote location: :keep do
      @otp_app unquote(otp_app)
      @repo_name unquote(name)

      def default_options(_), do: [returning: true]

      def run(callback) do
        config = fetch_config()
        credentials = [username: config[:username], password: config[:password]]

        with_dynamic_repo(@repo_name, credentials, callback)
      end

      def with_dynamic_repo(name, opts, callback) do
        Couchx.DynamicRepo.with_repo(__MODULE__, name, opts, callback)
      end

      def stop_repo(name), do: Couchx.DynamicRepo.stop(__MODULE__, name)

      def stop_repo(name, opts), do: Couchx.DynamicRepo.stop(__MODULE__, name, opts)

      defp fetch_config do
        Application.get_env(@otp_app, __MODULE__)
      end
    end
  end
end
