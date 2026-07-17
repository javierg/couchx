defmodule Couchx.DbConnectionTest do
  use ExUnit.Case, async: false

  setup do
    bypass = Bypass.open()

    start_supervised!({Registry, keys: :unique, name: CouchxRegistry})

    name = :"couchx_test_#{System.unique_integer([:positive])}"

    {:ok, pid} =
      Couchx.DbConnection.start_link(
        name: name,
        protocol: "http",
        hostname: "127.0.0.1",
        port: bypass.port,
        database: "",
        username: "user",
        password: "pass"
      )

    {:ok, bypass: bypass, conn: pid}
  end

  describe "raw_request/4 with Req" do
    test "does not forward CouchDB options like include_docs/module to Req", %{
      bypass: bypass,
      conn: conn
    } do
      Bypass.expect_once(bypass, "GET", "/mydb/_all_docs", fn conn ->
        assert conn.query_string =~ "include_docs=true"

        conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, ~s({"rows":[]}))
      end)

      assert {:ok, %{"rows" => []}} =
               Couchx.DbConnection.raw_request(conn, :get, "mydb/_all_docs",
                 include_docs: true,
                 module: __MODULE__
               )
    end

    test "joins URLs without a double slash when database is empty", %{
      bypass: bypass,
      conn: conn
    } do
      Bypass.expect_once(bypass, "GET", "/mydb/_all_docs", fn conn ->
        # Path must be /mydb/... not //mydb/...
        assert conn.request_path == "/mydb/_all_docs"

        conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, ~s({"rows":[]}))
      end)

      assert {:ok, %{"rows" => []}} =
               Couchx.DbConnection.raw_request(conn, :get, "/mydb/_all_docs", [])
    end

    test "moves view options such as reduce into the query string on POST", %{
      bypass: bypass,
      conn: conn
    } do
      Bypass.expect_once(bypass, "POST", "/_design/demo/_view/demo", fn conn ->
        assert conn.query_string =~ "reduce=true"
        {:ok, body, conn} = Plug.Conn.read_body(conn)
        assert body == "{}"

        conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, ~s({"rows":[{"key":null,"value":2}]}))
      end)

      assert {:ok, %{"rows" => [%{"value" => 2}]}} =
               Couchx.DbConnection.raw_request(conn, :post, "_design/demo/_view/demo",
                 reduce: true,
                 method: :post,
                 body: %{}
               )
    end

    test "maps legacy recv_timeout to receive_timeout without crashing", %{
      bypass: bypass,
      conn: conn
    } do
      Bypass.expect_once(bypass, "PUT", "/_replicator/doc", fn conn ->
        conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, ~s({"ok":true,"id":"doc","rev":"1-abc"}))
      end)

      assert {:ok, %{"ok" => true}} =
               Couchx.DbConnection.raw_request(conn, :put, "_replicator/doc",
                 body: %{source: "a", target: "b"},
                 recv_timeout: 1_000
               )
    end

    test "does not create atoms from query-string keys", %{bypass: bypass, conn: conn} do
      key = "couchx_query_#{System.unique_integer([:positive])}"

      assert_raise ArgumentError, fn -> String.to_existing_atom(key) end

      Bypass.expect_once(bypass, "GET", "/_all_docs", fn conn ->
        assert URI.decode_query(conn.query_string) == %{key => "value"}

        conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, ~s({"rows":[]}))
      end)

      assert {:ok, %{"rows" => []}} =
               Couchx.DbConnection.raw_request(conn, :get, "_all_docs?#{key}=value")

      assert_raise ArgumentError, fn -> String.to_existing_atom(key) end
    end

    test "explicit options replace duplicate parameters embedded in the path", %{
      bypass: bypass,
      conn: conn
    } do
      Bypass.expect_once(bypass, "GET", "/_all_docs", fn conn ->
        assert URI.decode_query(conn.query_string) == %{"include_docs" => "true"}

        conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, ~s({"rows":[]}))
      end)

      assert {:ok, %{"rows" => []}} =
               Couchx.DbConnection.raw_request(
                 conn,
                 :get,
                 "_all_docs?include_docs=false",
                 include_docs: true
               )
    end
  end

  describe "get/4 with Req" do
    test "encodes CouchDB query options in the query string only", %{
      bypass: bypass,
      conn: conn
    } do
      Bypass.expect_once(bypass, "GET", "/_design/demo/_view/demo", fn conn ->
        assert conn.query_string =~ "reduce=true"
        refute conn.query_string =~ "module="

        conn
        |> Plug.Conn.put_resp_content_type("application/json")
        |> Plug.Conn.resp(200, ~s({"rows":[]}))
      end)

      assert {:ok, %{"rows" => []}} =
               Couchx.DbConnection.get(conn, "_design/demo/_view/demo", %{
                 reduce: true,
                 module: __MODULE__
               })
    end
  end

  test "adapter starts Req dependencies using the Ecto callback contract" do
    assert {:ok, applications} = Couchx.Adapter.ensure_all_started([], :temporary)
    assert is_list(applications)
  end
end
