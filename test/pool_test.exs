defmodule Couchx.PoolTest do
  use ExUnit.Case, async: false

  setup do
    bypass = Bypass.open()

    config = [
      protocol: "http",
      hostname: "127.0.0.1",
      port: bypass.port,
      database: "",
      username: "user",
      password: "pass"
    ]

    {:ok, bypass: bypass, config: config}
  end

  defp start_pool(config), do: start_supervised!({Couchx.Pool, config})

  defp in_flight_counter do
    {:ok, agent} = Agent.start_link(fn -> {0, 0} end)
    agent
  end

  defp enter(agent), do: Agent.update(agent, fn {now, max} -> {now + 1, max(max, now + 1)} end)
  defp leave(agent), do: Agent.update(agent, fn {now, max} -> {now - 1, max} end)
  defp max_in_flight(agent), do: Agent.get(agent, fn {_, max} -> max end)

  test "defaults pool size" do
    assert Couchx.Pool.pool_size([]) == 10
  end

  test "rejects invalid pool size", %{config: config} do
    assert_raise ArgumentError, fn -> Couchx.Pool.pool_size(config ++ [pool_size: 0]) end
  end

  test "starts a Finch pool and exposes connection settings", %{config: config} do
    pool = start_pool(config)

    assert [{_finch, _, :supervisor, [Finch]}] = Supervisor.which_children(pool)

    conn = Couchx.Pool.lookup(pool)
    assert conn.base_url == "http://127.0.0.1:#{config[:port]}/"
    assert {"Authorization", "Basic " <> _} = List.keyfind(conn.headers, "Authorization", 0)
  end

  test "registers the pool under its name", %{config: config} do
    start_supervised!({Registry, keys: :unique, name: CouchxRegistry})
    pool = start_pool(config ++ [name: :named_pool])

    assert [{^pool, _}] = Registry.lookup(CouchxRegistry, :named_pool)
    assert Couchx.Pool.lookup({:via, Registry, {CouchxRegistry, :named_pool}}).finch
  end

  test "reuses the Finch name for the same repo name", %{config: config} do
    start_supervised!({Registry, keys: :unique, name: CouchxRegistry})

    first = start_supervised!({Couchx.Pool, config ++ [name: :reused]}, id: :first)
    first_finch = Couchx.Pool.lookup(first).finch
    stop_supervised!(:first)
    second = start_supervised!({Couchx.Pool, config ++ [name: :reused]}, id: :second)

    assert first_finch == Couchx.Pool.lookup(second).finch
  end

  test "lookup exits with :noproc for something that isn't a pool" do
    assert {:noproc, _} = catch_exit(Couchx.Pool.lookup(self()))
    assert {:noproc, _} = catch_exit(Couchx.DbConnection.info(:no_such_pool))
  end

  test "never runs more than pool_size requests at once", %{bypass: bypass, config: config} do
    pool = start_pool(config ++ [pool_size: 2])
    counter = in_flight_counter()

    Bypass.expect(bypass, "GET", "/db", fn conn ->
      enter(counter)
      Process.sleep(100)
      leave(counter)
      Plug.Conn.resp(conn, 200, ~s({"ok":true}))
    end)

    results =
      1..6
      |> Enum.map(fn _ -> Task.async(fn -> Couchx.DbConnection.raw_request(pool, :get, "db") end) end)
      |> Task.await_many(5_000)

    assert Enum.all?(results, &match?({:ok, _}, &1))
    assert max_in_flight(counter) == 2
  end

  test "a slow request doesn't block others while connections are free",
       %{bypass: bypass, config: config} do
    pool = start_pool(config ++ [pool_size: 2])
    test_pid = self()

    Bypass.expect(bypass, "GET", "/slow", fn conn ->
      send(test_pid, :slow_in_flight)
      Process.sleep(300)
      Plug.Conn.resp(conn, 200, ~s({"ok":true}))
    end)

    Bypass.expect(bypass, "GET", "/fast", fn conn ->
      Plug.Conn.resp(conn, 200, ~s({"ok":true}))
    end)

    slow = Task.async(fn -> Couchx.DbConnection.raw_request(pool, :get, "slow") end)
    assert_receive :slow_in_flight, 1_000

    {elapsed, result} = :timer.tc(fn -> Couchx.DbConnection.raw_request(pool, :get, "fast") end)

    assert {:ok, _} = result
    assert elapsed < 200_000, "fast request waited behind the slow one"
    assert {:ok, _} = Task.await(slow)
  end

  test "call_timeout exits with {:timeout, _} and does not retry", %{bypass: bypass, config: config} do
    pool = start_pool(config)
    counter = in_flight_counter()

    Bypass.stub(bypass, "GET", "/slow", fn conn ->
      # Bypass kills the handler when the client hangs up; trap so it finishes.
      Process.flag(:trap_exit, true)
      enter(counter)
      Process.sleep(200)
      Plug.Conn.resp(conn, 200, ~s({"ok":true}))
    end)

    assert {:timeout, _} =
             catch_exit(Couchx.DbConnection.raw_request(pool, :get, "slow", call_timeout: 50))

    Process.sleep(250)
    assert max_in_flight(counter) == 1
  end

  test "pool_timeout exits with {:timeout, _} when all connections are busy",
       %{bypass: bypass, config: config} do
    pool = start_pool(config ++ [pool_size: 1])
    test_pid = self()

    Bypass.stub(bypass, "GET", "/slow", fn conn ->
      send(test_pid, :busy)
      Process.sleep(300)
      Plug.Conn.resp(conn, 200, ~s({"ok":true}))
    end)

    busy = Task.async(fn -> Couchx.DbConnection.raw_request(pool, :get, "slow") end)
    assert_receive :busy, 1_000

    assert {:timeout, _} =
             catch_exit(Couchx.DbConnection.raw_request(pool, :get, "slow", pool_timeout: 50))

    assert {:ok, _} = Task.await(busy)
  end

  test "no worker processes sit between caller and Finch", %{bypass: bypass, config: config} do
    pool = start_pool(config)
    Bypass.expect(bypass, "GET", "/db", fn conn -> Plug.Conn.resp(conn, 200, ~s({"ok":true})) end)

    assert {:ok, _} = Couchx.DbConnection.raw_request(pool, :get, "db")
    assert [{_finch, _, :supervisor, [Finch]}] = Supervisor.which_children(pool)
  end

  test "requests don't emit Req deprecation warnings", %{bypass: bypass, config: config} do
    pool = start_pool(config)
    Bypass.expect(bypass, "GET", "/db", fn conn -> Plug.Conn.resp(conn, 200, ~s({"ok":true})) end)

    stderr =
      ExUnit.CaptureIO.capture_io(:stderr, fn ->
        assert {:ok, _} = Couchx.DbConnection.raw_request(pool, :get, "db", pool_timeout: 1_000)
      end)

    refute stderr =~ "deprecated"
  end

  defp trickle_server(chunks, interval) do
    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, port} = :inet.port(listen)

    spawn_link(fn ->
      {:ok, socket} = :gen_tcp.accept(listen)
      {:ok, _request} = :gen_tcp.recv(socket, 0)
      :gen_tcp.send(socket, "HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n")

      Enum.reduce_while(1..chunks, :ok, fn _, _ ->
        Process.sleep(interval)

        case :gen_tcp.send(socket, "1\r\n \r\n") do
          :ok -> {:cont, :ok}
          {:error, _} -> {:halt, :ok}
        end
      end)

      :gen_tcp.close(socket)
      :gen_tcp.close(listen)
    end)

    port
  end

  test "call_timeout is a deadline for the whole response, not per chunk", %{config: config} do
    port = trickle_server(10, 40)
    pool = start_pool(Keyword.put(config, :port, port))

    {elapsed, result} =
      :timer.tc(fn ->
        catch_exit(Couchx.DbConnection.raw_request(pool, :get, "trickle", call_timeout: 150))
      end)

    assert {:timeout, _} = result
    assert elapsed < 350_000, "request ran past call_timeout while data kept arriving"
  end

  test "Finch still reports pool checkout timeouts with the message we match on" do
    finch_pool = Path.join(Mix.Project.deps_paths()[:finch], "lib/finch/http1/pool.ex")
    assert File.read!(finch_pool) =~ "unable to provide a connection"
  end

  test "adapter ensure_all_started starts :couchx" do
    assert {:ok, _} = Couchx.Adapter.ensure_all_started(nil, :temporary)
    assert Process.whereis(Couchx.Pool.Registry)
  end
end
