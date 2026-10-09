defmodule Couchx.DynamicRepo.Janitor do
  @moduledoc """
  Stops dynamic repos that haven't been used for a while.

  Every dynamic repo has an entry `{pid, last_used, active}` in an ETS table.
  Callers bump `active` while their callback runs. A repo with no active
  callers that has been idle longer than the timeout is stopped.

  Configure with:

      config :couchx, dynamic_repo_idle_timeout: :timer.minutes(5)

  Set it to `:infinity` to keep repos running until `stop_repo` is called.
  """

  use GenServer

  @table __MODULE__
  @default_idle_timeout :timer.minutes(5)
  @max_sweep_interval :timer.minutes(1)

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc false
  def checkout(pid) do
    :ets.update_counter(@table, pid, {3, 1}, {pid, now(), 0})
    :ok
  rescue
    ArgumentError -> :closing
  end

  @doc false
  def checkin(pid) do
    :ets.update_element(@table, pid, {2, now()})
    :ets.update_counter(@table, pid, {3, -1})
    :ok
  rescue
    ArgumentError -> :ok
  end

  @doc "Stops idle repos now. Returns the stopped pids."
  def sweep, do: GenServer.call(__MODULE__, :sweep, :infinity)

  def idle_timeout do
    Application.get_env(:couchx, :dynamic_repo_idle_timeout, @default_idle_timeout)
  end

  @impl true
  def init(_opts) do
    :ets.new(@table, [:named_table, :public, :set, write_concurrency: true])
    schedule()
    {:ok, nil}
  end

  @impl true
  def handle_call(:sweep, _from, state), do: {:reply, do_sweep(), state}

  @impl true
  def handle_info(:sweep, state) do
    do_sweep()
    schedule()
    {:noreply, state}
  end

  defp do_sweep do
    drop_dead()

    case idle_timeout() do
      :infinity -> []
      timeout -> stop_idle(now() - timeout)
    end
  end

  defp stop_idle(cutoff) do
    spec = [
      {{:"$1", :"$2", 0}, [{:<, :"$2", cutoff}], [{{:"$1", :"$2", :closing}}]}
    ]

    :ets.select_replace(@table, spec)

    pids = :ets.select(@table, [{{:"$1", :_, :closing}, [], [:"$1"]}])

    for pid <- pids do
      DynamicSupervisor.terminate_child(Couchx.DynamicRepoSupervisor, pid)
      :ets.delete(@table, pid)
    end

    pids
  end

  defp drop_dead do
    for [pid] <- :ets.match(@table, {:"$1", :_, :_}), not Process.alive?(pid) do
      :ets.delete(@table, pid)
    end
  end

  defp schedule do
    case idle_timeout() do
      :infinity -> Process.send_after(self(), :sweep, @max_sweep_interval)
      timeout -> Process.send_after(self(), :sweep, timeout |> div(2) |> max(10) |> min(@max_sweep_interval))
    end
  end

  defp now, do: System.monotonic_time(:millisecond)
end
