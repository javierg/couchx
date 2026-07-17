defmodule Couchx.DbConnection do
  use GenServer, restart: :transient

  require Logger

  @default_call_timeout 30_000

  # Options that belong to CouchDB / the adapter, not to Req.
  @non_req_options [
    :body,
    :call_timeout,
    :descending,
    :endkey,
    :group,
    :group_level,
    :include_docs,
    :key,
    :keys,
    :limit,
    :method,
    :module,
    :query_str,
    :reduce,
    :skip,
    :stable,
    :stale,
    :startkey,
    :timeout,
    :update_seq
  ]

  def start_link(args) do
    config = build_config(args)
    name = process_name(args[:name])

    GenServer.start_link(__MODULE__, config, name: name)
  end

  def init(args) do
    {:ok, args}
  end

  def terminate(reason, _state) when reason in [:normal, :shutdown] do
    Logger.info("Couchx database connection stopped", reason: inspect(reason))
  end

  def terminate({:shutdown, _detail} = reason, _state) do
    Logger.info("Couchx database connection stopped", reason: inspect(reason))
  end

  def terminate(reason, _state) do
    Logger.warning("Couchx database connection terminated", reason: inspect(reason))
  end

  def info(server), do: GenServer.call(server, :info, @default_call_timeout)

  def insert(server, resource, body, options \\ []) do
    GenServer.call(server, {:insert, resource, body, options}, call_timeout(options))
  end

  def bulk_docs(server, docs, options \\ []) do
    GenServer.call(server, {:bulk_docs, docs, options}, call_timeout(options))
  end

  def get(server, resource, query \\ nil, options \\ []) do
    GenServer.call(server, {:get, resource, query, options}, call_timeout(options))
  end

  def all_docs(server, keys, options \\ []) do
    GenServer.call(server, {:all_docs, keys, options}, call_timeout(options))
  end

  def delete(server, resource, rev) do
    GenServer.call(server, {:delete, resource, rev}, @default_call_timeout)
  end

  def delete(server, :index, name, id) do
    id = if id, do: id, else: name
    GenServer.call(server, {:delete_index, name, id}, @default_call_timeout)
  end

  def create_db(server, name) do
    GenServer.call(server, {:create_db, name}, @default_call_timeout)
  end

  def delete_db(server, name) do
    GenServer.call(server, {:delete_db, name}, @default_call_timeout)
  end

  def create_admin(server, name, password) do
    GenServer.call(server, {:create_admin, name, password}, @default_call_timeout)
  end

  def delete_admin(server, name) do
    GenServer.call(server, {:delete_admin, name}, @default_call_timeout)
  end

  def find(server, query, options \\ []) do
    GenServer.call(server, {:find, query, options}, call_timeout(options))
  end

  def index(server, doc) do
    GenServer.call(server, {:index, doc}, @default_call_timeout)
  end

  def raw_request(server, method, path, options \\ []) do
    GenServer.call(server, {:raw_request, method, path, options}, call_timeout(options))
  end

  def handle_call({:index, doc}, _from, state) do
    headers = state[:base_headers]
    url = join_url(state[:base_url], "_index")
    body = Jason.encode!(doc)

    request(:post, url, body, headers: headers, options: [])
    |> call_response(state)
  end

  def handle_call({:delete_admin, name}, _from, state) do
    url = join_url(state[:base_url], "_users/org.couchdb.user:#{name}")
    opts = [headers: state[:base_headers], options: state[:options]]
    user_doc = request(:get, url, opts)

    request(:delete, "#{url}?rev=#{user_doc["_rev"]}", opts)
    |> call_response(state)
  end

  def handle_call({:create_admin, name, password}, _from, state) do
    opts = [headers: state[:base_headers], options: state[:options]]

    create_role(state[:base_url], name, name, opts)

    create_admin_user(state[:base_url], name, password, opts)
    |> call_response(state)
    |> call_response(state)
  end

  def handle_call({:create_db, name}, _from, state) do
    url = join_url(state[:base_url], name)
    opts = [headers: state[:base_headers], options: state[:options]]

    request(:put, url, [], opts)
    |> call_response(state)
  end

  def handle_call({:delete, doc_id, rev}, _from, state) do
    url = join_url(state[:base_url], "#{doc_id}?rev=#{rev}")
    opts = [headers: state[:base_headers], options: state[:options]]

    request(:delete, url, opts)
    |> call_response(state)
  end

  def handle_call({:delete_db, name}, _from, state) do
    url = join_url(state[:base_url], name)
    opts = [headers: state[:base_headers], options: state[:options]]

    request(:delete, url, opts)
    |> call_response(state)
  end

  def handle_call(:info, _from, state) do
    request(:get, state[:base_url], headers: state[:base_headers], options: state[:options])
    |> call_response(state)
  end

  def handle_call({:all_docs, keys, options}, _from, state) do
    headers = state[:base_headers]
    with_docs = options[:include_docs] || false
    url = join_url(state[:base_url], "_all_docs?include_docs=#{with_docs}")
    body = Jason.encode!(%{keys: keys})

    request(:post, url, body, headers: headers, options: [])
    |> call_response(state)
  end

  def handle_call({:bulk_docs, docs, options}, _from, state) do
    headers = state[:base_headers]
    url = join_url(state[:base_url], "_bulk_docs")
    body = Jason.encode!(%{docs: docs})

    request(:post, url, body, headers: headers, options: options)
    |> call_response(state)
  end

  def handle_call({:insert, resource, body, options}, _from, state) do
    headers = state[:base_headers]
    url = join_url(state[:base_url], resource)

    request(:put, url, body, headers: headers, options: options)
    |> call_response(state)
  end

  def handle_call({:get, resource, query, options}, _from, state) do
    headers = state[:base_headers]
    {path, query} = split_resource_query(resource, query)
    query_str = build_query_str(query)
    url = join_url(state[:base_url], "#{path}#{query_str}")

    request(:get, url, headers: headers, options: options)
    |> call_response(state)
  end

  def handle_call({:raw_request, method, path, options}, _from, state) do
    {path, embedded_query} = split_path_and_query(path)
    query = merge_query(embedded_query, options)
    query_str = build_query_str(query)
    url = join_url(state[:base_url], "#{path}#{query_str}")
    req_options = state[:options] ++ options

    case method do
      :get ->
        request(method, url, headers: state[:base_headers], options: req_options)

      :delete ->
        request(:delete, url, headers: state[:base_headers], options: [])

      _ ->
        body = Jason.encode!(options[:body] || %{})
        request(method, url, body, headers: state[:base_headers], options: req_options)
    end
    |> call_response(state)
  end

  def handle_call({:find, query, options}, _from, state) do
    headers = state[:base_headers]
    query_str = build_query_str(options[:query_str])
    url = join_url(state[:base_url], "_find#{query_str}")
    body = Jason.encode!(query)

    request(:post, url, body, headers: headers, options: options)
    |> call_response(state)
  end

  def handle_call({:delete_index, name, id}, _from, state) do
    headers = state[:base_headers]
    url = join_url(state[:base_url], "_index/_design/#{id}/json/#{name}")

    request(:delete, url, headers: headers, options: [])
    |> call_response(state)
  end

  defp request(method, url, extras) when method in [:get, :delete] do
    headers = extras[:headers] || []

    options =
      extras
      |> Keyword.get(:options, [])
      |> prepare_req_options()

    [method: method, url: url, headers: headers]
    |> Keyword.merge(options)
    |> Req.request!()
    |> then(& &1.body)
  end

  defp request(method, url, body, extras) when method in [:post, :put] do
    headers = extras[:headers] || []

    options =
      extras
      |> Keyword.get(:options, [])
      |> prepare_req_options()

    [method: method, url: url, headers: headers, body: body]
    |> Keyword.merge(options)
    |> Req.request!()
    |> then(& &1.body)
  end

  defp prepare_req_options(options) when is_list(options) do
    options
    |> maybe_map_recv_timeout()
    |> maybe_map_connect_timeout()
    |> Keyword.drop(@non_req_options)
  end

  defp prepare_req_options(_), do: []

  defp maybe_map_recv_timeout(options) do
    case Keyword.pop(options, :recv_timeout) do
      {nil, options} -> options
      {timeout, options} -> Keyword.put_new(options, :receive_timeout, timeout)
    end
  end

  defp maybe_map_connect_timeout(options) do
    case Keyword.get(options, :timeout) do
      nil ->
        options

      timeout ->
        Keyword.update(options, :connect_options, [timeout: timeout], fn
          connect_options when is_list(connect_options) ->
            Keyword.put_new(connect_options, :timeout, timeout)

          connect_options ->
            connect_options
        end)
    end
  end

  defp call_timeout(options) when is_list(options) do
    options[:call_timeout] || options[:timeout] || @default_call_timeout
  end

  defp call_timeout(_options), do: @default_call_timeout

  defp call_response(%{"error" => error, "reason" => reason}, state) do
    {:reply, {:error, "#{error} :: #{reason}"}, state}
  end

  defp call_response(response, state), do: {:reply, {:ok, response}, state}

  defp build_query_str(nil), do: ""
  defp build_query_str([]), do: ""
  defp build_query_str(query) when query == %{}, do: ""

  defp build_query_str(query) when is_map(query) do
    query
    |> Enum.to_list()
    |> build_query_str()
  end

  defp build_query_str(query) when is_list(query) do
    query =
      Enum.reject(query, fn
        {key, _value} ->
          to_string(key) in [
            "body",
            "call_timeout",
            "method",
            "module",
            "query_str",
            "recv_timeout",
            "timeout"
          ]

        _other ->
          true
      end)

    case query do
      [] -> ""
      query -> "?#{URI.encode_query(query)}"
    end
  end

  defp build_query_str(_), do: ""

  # Pull CouchDB view/query options out of raw_request opts into the query string.
  defp merge_query(embedded_query, options) when is_list(options) do
    couch_query =
      options
      |> Keyword.take([
        :descending,
        :endkey,
        :group,
        :group_level,
        :include_docs,
        :key,
        :keys,
        :limit,
        :reduce,
        :skip,
        :stable,
        :stale,
        :startkey,
        :update_seq
      ])

    embedded_query
    |> merge_query_values(options[:query_str])
    |> merge_query_values(couch_query)
  end

  defp merge_query(embedded_query, _), do: embedded_query

  defp split_resource_query(resource, query) when is_binary(resource) do
    {path, embedded} = split_path_and_query(resource)
    {path, merge_query_values(embedded, query)}
  end

  defp split_resource_query(resource, query), do: {to_string(resource), query}

  defp merge_query_values(existing, nil), do: existing

  defp merge_query_values(existing, query) when is_list(query) or is_map(query) do
    existing
    |> query_map()
    |> Map.merge(query_map(query))
    |> Enum.to_list()
  end

  defp merge_query_values(existing, _), do: existing

  defp query_map(query) when is_map(query) do
    Map.new(query, fn {key, value} -> {to_string(key), value} end)
  end

  defp query_map(query) when is_list(query) do
    Map.new(query, fn {key, value} -> {to_string(key), value} end)
  end

  defp split_path_and_query(path) when is_binary(path) do
    case String.split(path, "?", parts: 2) do
      [path] ->
        {path, []}

      [path, query] ->
        {path, URI.decode_query(query)}
    end
  end

  defp split_path_and_query(path), do: {to_string(path), []}

  defp join_url(base, path) do
    base = String.trim_trailing(to_string(base), "/")
    path = to_string(path) |> String.trim_leading("/")
    "#{base}/#{path}"
  end

  defp build_config(args) do
    %{
      base_url: base_url(args),
      base_headers: fetch_headers(args),
      options: []
    }
  end

  defp base_url(args) do
    database = args[:database] || ""

    "#{args[:protocol]}://#{args[:hostname]}:#{args[:port]}"
    |> join_url(database)
  end

  defp fetch_headers(config) do
    credentials =
      "#{config[:username]}:#{config[:password]}"
      |> Base.encode64()

    [
      {"Content-Type", "application/json"},
      {"Authorization", "Basic #{credentials}"}
    ]
  end

  defp create_admin_user(base_url, name, password, opts) do
    url = join_url(base_url, "_users/org.couchdb.user:#{name}")

    body =
      name
      |> user_doc(password)
      |> Jason.encode!()

    request(:put, url, body, opts)
  end

  defp create_role(base_url, db_name, name, opts) do
    roles = %{members: %{names: [], roles: []}, admins: %{names: [name], roles: []}}
    request(:put, join_url(base_url, "#{db_name}/_security"), Jason.encode!(roles), opts)
  end

  defp user_doc(name, password) do
    %{
      name: name,
      password: password,
      roles: [],
      type: "user"
    }
  end

  defp process_name(nil), do: __MODULE__

  defp process_name(name) do
    {:via, Registry, {CouchxRegistry, name}}
  end
end
