defmodule Couchx.DynamicRepoTest do
  use ExUnit.Case, async: false

  defmodule Repo do
    use Ecto.Repo, otp_app: :couchx, adapter: Couchx.Adapter
    use Couchx.DynamicRepo, otp_app: :couchx, name: :couchx_dynamic_test
  end

  defmodule SlowRepo do
    use Ecto.Repo, otp_app: :couchx, adapter: Couchx.Adapter
    use Couchx.DynamicRepo, otp_app: :couchx, name: :couchx_dynamic_slow

    def init(_type, config) do
      Process.sleep(100)
      {:ok, config}
    end
  end

  setup do
    bypass = Bypass.open()
    start_supervised!({Registry, keys: :unique, name: CouchxRegistry})

    Application.put_env(:couchx, Repo,
      protocol: "http",
      hostname: "127.0.0.1",
      port: bypass.port,
      database: "",
      username: "user",
      password: "pass"
    )

    on_exit(fn ->
      Repo.stop_repo(:couchx_dynamic_test)
      Repo.stop_repo(:couchx_dynamic_other)
      Application.delete_env(:couchx, Repo)
    end)

    {:ok, bypass: bypass}
  end

  defp current_repo_pid, do: Repo.get_dynamic_repo()

  test "keeps the repo running between runs and reuses it" do
    first = Repo.run(&current_repo_pid/0)
    second = Repo.run(&current_repo_pid/0)

    assert is_pid(first)
    assert first == second
    assert Process.alive?(first)
  end

  test "restores the previous dynamic repo after the callback, even on error" do
    before = Repo.get_dynamic_repo()

    assert_raise RuntimeError, fn -> Repo.run(fn -> raise "boom" end) end
    assert Repo.get_dynamic_repo() == before
    assert Process.alive?(Repo.run(&current_repo_pid/0))
  end

  test "accepts string names and keeps repos separate" do
    a = Repo.with_dynamic_repo(:couchx_dynamic_test, [], &current_repo_pid/0)
    b = Repo.with_dynamic_repo("couchx_dynamic_other", [], &current_repo_pid/0)

    assert a != b
  end

  test "same name with different credentials or database gets its own repo" do
    a = Repo.with_dynamic_repo(:couchx_dynamic_test, [username: "a", password: "x"], &current_repo_pid/0)
    b = Repo.with_dynamic_repo(:couchx_dynamic_test, [username: "b", password: "y"], &current_repo_pid/0)
    c = Repo.with_dynamic_repo(:couchx_dynamic_test, [username: "a", password: "x", database: "other"], &current_repo_pid/0)
    again = Repo.with_dynamic_repo(:couchx_dynamic_test, [password: "x", username: "a"], &current_repo_pid/0)

    assert length(Enum.uniq([a, b, c])) == 3
    assert again == a
  end

  test "each repo sends its own credentials", %{bypass: bypass} do
    Bypass.expect(bypass, "GET", "/", fn conn ->
      [auth] = Plug.Conn.get_req_header(conn, "authorization")

      conn
      |> Plug.Conn.put_resp_content_type("application/json")
      |> Plug.Conn.resp(200, Jason.encode!(%{auth: auth}))
    end)

    info = fn ->
      %{pid: pool} = Ecto.Repo.Registry.lookup(Repo.get_dynamic_repo())
      {:ok, %{"auth" => auth}} = Couchx.DbConnection.info(pool)
      auth
    end

    assert Repo.with_dynamic_repo(:couchx_dynamic_test, [username: "a", password: "x"], info) ==
             "Basic " <> Base.encode64("a:x")

    assert Repo.with_dynamic_repo(:couchx_dynamic_test, [username: "b", password: "y"], info) ==
             "Basic " <> Base.encode64("b:y")
  end

  test "stop_repo/2 stops only the repo started with those options" do
    a = Repo.with_dynamic_repo(:couchx_dynamic_test, [username: "a"], &current_repo_pid/0)
    b = Repo.with_dynamic_repo(:couchx_dynamic_test, [username: "b"], &current_repo_pid/0)
    ref = Process.monitor(a)

    assert :ok = Repo.stop_repo(:couchx_dynamic_test, username: "a")
    assert_receive {:DOWN, ^ref, _, _, _}
    assert Process.alive?(b)
  end

  test "stop_repo/1 stops every repo with that name" do
    a = Repo.with_dynamic_repo(:couchx_dynamic_test, [username: "a"], &current_repo_pid/0)
    b = Repo.with_dynamic_repo("couchx_dynamic_test", [username: "b"], &current_repo_pid/0)
    refs = Enum.map([a, b], &Process.monitor/1)

    assert :ok = Repo.stop_repo(:couchx_dynamic_test)

    for ref <- refs, do: assert_receive({:DOWN, ^ref, _, _, _})
  end

  test "concurrent callers only get the repo once Ecto has registered it", %{bypass: _bypass} do
    config = Application.get_env(:couchx, Repo)
    Application.put_env(:couchx, SlowRepo, config)

    on_exit(fn ->
      SlowRepo.stop_repo(:couchx_dynamic_slow)
      Application.delete_env(:couchx, SlowRepo)
    end)

    results =
      1..10
      |> Enum.map(fn _ ->
        Task.async(fn ->
          SlowRepo.run(fn ->
            %{pid: pool} = Ecto.Repo.Registry.lookup(SlowRepo.get_dynamic_repo())
            {SlowRepo.get_dynamic_repo(), is_pid(pool)}
          end)
        end)
      end)
      |> Task.await_many()

    assert [{repo, true}] = Enum.uniq(results)
    assert is_pid(repo)
  end

  test "concurrent first runs start the repo once" do
    pids =
      1..10
      |> Enum.map(fn _ -> Task.async(fn -> Repo.run(&current_repo_pid/0) end) end)
      |> Task.await_many()

    assert [_one] = Enum.uniq(pids)
  end

  test "stop_repo stops it and the next run starts a fresh one" do
    first = Repo.run(&current_repo_pid/0)
    ref = Process.monitor(first)

    assert :ok = Repo.stop_repo(:couchx_dynamic_test)
    assert_receive {:DOWN, ^ref, _, _, _}

    second = Repo.run(&current_repo_pid/0)
    assert second != first
  end

  describe "idle timeout" do
    setup do
      Application.put_env(:couchx, :dynamic_repo_idle_timeout, 50)
      on_exit(fn -> Application.delete_env(:couchx, :dynamic_repo_idle_timeout) end)
    end

    test "stops a repo that has been idle longer than the timeout" do
      repo = Repo.run(&current_repo_pid/0)
      ref = Process.monitor(repo)

      Process.sleep(80)
      Couchx.DynamicRepo.Janitor.sweep()
      assert_receive {:DOWN, ^ref, _, _, _}

      assert Repo.run(&current_repo_pid/0) != repo
    end

    test "keeps a repo that was used recently" do
      repo = Repo.run(&current_repo_pid/0)

      assert Couchx.DynamicRepo.Janitor.sweep() == []
      assert Process.alive?(repo)
    end

    test "never stops a repo while a callback is running" do
      parent = self()

      task =
        Task.async(fn ->
          Repo.run(fn ->
            send(parent, {:repo, current_repo_pid()})
            receive do: (:done -> :ok)
          end)
        end)

      assert_receive {:repo, repo}
      Process.sleep(80)

      assert Couchx.DynamicRepo.Janitor.sweep() == []
      assert Process.alive?(repo)

      send(task.pid, :done)
      Task.await(task)
    end

    test "the scheduled sweep stops idle repos" do
      repo = Repo.run(&current_repo_pid/0)
      ref = Process.monitor(repo)

      Process.sleep(80)
      send(Couchx.DynamicRepo.Janitor, :sweep)
      assert_receive {:DOWN, ^ref, _, _, _}, 1_000
    end

    test ":infinity disables it" do
      Application.put_env(:couchx, :dynamic_repo_idle_timeout, :infinity)
      repo = Repo.run(&current_repo_pid/0)

      Process.sleep(80)
      assert Couchx.DynamicRepo.Janitor.sweep() == []
      assert Process.alive?(repo)
    end
  end

  test "stop_repo is a no-op for a repo that isn't running" do
    assert :ok = Repo.stop_repo(:couchx_not_running)
  end

  test "repo calls inside run go through the dynamic repo's pool", %{bypass: bypass} do
    Bypass.expect_once(bypass, "GET", "/", fn conn ->
      conn
      |> Plug.Conn.put_resp_content_type("application/json")
      |> Plug.Conn.resp(200, ~s({"couchdb":"Welcome"}))
    end)

    assert {:ok, %{"couchdb" => "Welcome"}} =
             Repo.run(fn ->
               %{pid: pool} = Ecto.Repo.Registry.lookup(Repo.get_dynamic_repo())
               Couchx.DbConnection.info(pool)
             end)
  end
end
