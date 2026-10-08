defmodule Couchx.DbConnection do
  @moduledoc """
  CouchDB request functions.

  `server` is a `Couchx.Pool` (pid, name or `{:via, ...}` tuple), usually the
  repo `:pid`. Requests run in the caller's process through Req, using the
  pool's Finch connections. No GenServer sits in between.

  Timeouts per call:

    * `:call_timeout` (or legacy `:timeout`) - how long to wait for response
      data from CouchDB, default 30_000 ms. It's applied as Req's
      `:receive_timeout`. Exits with `{:timeout, _}` when exceeded.
    * `:recv_timeout` / `:receive_timeout` - overrides the above directly.
    * `:pool_timeout` - time to wait for a free connection, default 5_000 ms.
      Exits with `{:timeout, _}` when exceeded.

  Requests are not retried unless you pass Req's `:retry` option.
  """

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

  # Req options a caller may pass through. Anything else is dropped, so
  # CouchDB options never reach Req and a caller cannot override `:finch`.
  @req_passthrough [
    :receive_timeout,
    :pool_timeout,
    :retry,
    :max_retries,
    :retry_delay,
    :retry_log_level
  ]

  def info(server), do: run(server, :info, [])

  def insert(server, resource, body, options \\ []) do
    run(server, {:insert, resource, body, options}, options)
  end

  def bulk_docs(server, docs, options \\ []) do
    run(server, {:bulk_docs, docs, options}, options)
  end

  def get(server, resource, query \\ nil, options \\ []) do
    run(server, {:get, resource, query, options}, options)
  end

  def all_docs(server, keys, options \\ []) do
    run(server, {:all_docs, keys, options}, options)
  end

  def delete(server, resource, rev), do: run(server, {:delete, resource, rev}, [])

  def delete(server, :index, name, id) do
    run(server, {:delete_index, name, id || name}, [])
  end

  def create_db(server, name), do: run(server, {:create_db, name}, [])

  def delete_db(server, name), do: run(server, {:delete_db, name}, [])

  def create_admin(server, name, password) do
    run(server, {:create_admin, name, password}, [])
  end

  def delete_admin(server, name), do: run(server, {:delete_admin, name}, [])

  def find(server, query, options \\ []) do
    run(server, {:find, query, options}, options)
  end

  def index(server, doc), do: run(server, {:index, doc}, [])

  def raw_request(server, method, path, options \\ []) do
    run(server, {:raw_request, method, path, options}, options)
  end

  defp run(server, message, options) do
    conn = Couchx.Pool.lookup(server)
    timeout = call_timeout(options)
    state = Map.put(conn, :timeout, timeout)

    try do
      message
      |> perform(state)
      |> call_response()
    rescue
      error ->
        if timeout_error?(error),
          do: exit({:timeout, {__MODULE__, :run, [server, message, timeout]}}),
          else: reraise(error, __STACKTRACE__)
    end
  end

  defp timeout_error?(%Req.TransportError{reason: :timeout}), do: true

  # Finch re-raises NimblePool's checkout timeout as a plain RuntimeError, so
  # the message is the only thing to match on. Kept loose (substring) so minor
  # rewording upstream doesn't break it; the pool_timeout test guards it.
  defp timeout_error?(%RuntimeError{message: message}),
    do: String.contains?(message, "unable to provide a connection")

  defp timeout_error?(_), do: false

  defp perform({:index, doc}, state) do
    request(:post, url(state, "_index"), Jason.encode!(doc), state, [])
  end

  defp perform({:delete_admin, name}, state) do
    url = url(state, "_users/org.couchdb.user:#{name}")
    user_doc = request(:get, url, state, [])

    request(:delete, "#{url}?rev=#{user_doc["_rev"]}", state, [])
  end

  defp perform({:create_admin, name, password}, state) do
    create_role(state, name, name)
    create_admin_user(state, name, password)
  end

  defp perform({:create_db, name}, state) do
    request(:put, url(state, name), [], state, [])
  end

  defp perform({:delete, doc_id, rev}, state) do
    request(:delete, url(state, "#{doc_id}?rev=#{rev}"), state, [])
  end

  defp perform({:delete_db, name}, state) do
    request(:delete, url(state, name), state, [])
  end

  defp perform(:info, state) do
    request(:get, state.base_url, state, [])
  end

  defp perform({:all_docs, keys, options}, state) do
    with_docs = options[:include_docs] || false
    url = url(state, "_all_docs?include_docs=#{with_docs}")

    request(:post, url, Jason.encode!(%{keys: keys}), state, [])
  end

  defp perform({:bulk_docs, docs, options}, state) do
    request(:post, url(state, "_bulk_docs"), Jason.encode!(%{docs: docs}), state, options)
  end

  defp perform({:insert, resource, body, options}, state) do
    request(:put, url(state, resource), body, state, options)
  end

  defp perform({:get, resource, query, options}, state) do
    {path, query} = split_resource_query(resource, query)
    query_str = build_query_str(query)

    request(:get, url(state, "#{path}#{query_str}"), state, options)
  end

  defp perform({:raw_request, method, path, options}, state) do
    {path, embedded_query} = split_path_and_query(path)
    query = merge_query(embedded_query, options)
    url = url(state, "#{path}#{build_query_str(query)}")

    case method do
      :get -> request(:get, url, state, options)
      :delete -> request(:delete, url, state, [])
      _ -> request(method, url, Jason.encode!(options[:body] || %{}), state, options)
    end
  end

  defp perform({:find, query, options}, state) do
    query_str = build_query_str(options[:query_str])

    request(:post, url(state, "_find#{query_str}"), Jason.encode!(query), state, options)
  end

  defp perform({:delete_index, name, id}, state) do
    request(:delete, url(state, "_index/_design/#{id}/json/#{name}"), state, [])
  end

  defp request(method, url, state, options) when method in [:get, :delete] do
    send_request([method: method, url: url], state, options)
  end

  defp request(method, url, body, state, options) when method in [:post, :put] do
    send_request([method: method, url: url, body: body], state, options)
  end

  defp send_request(base, state, options) do
    {pool_timeout, req_options} = Keyword.pop(prepare_req_options(options), :pool_timeout)

    base
    |> Keyword.merge(headers: state.headers, receive_timeout: state.timeout, retry: false)
    |> Keyword.put(:finch, finch_options(state.finch, pool_timeout))
    |> Keyword.merge(req_options)
    |> Req.request!()
    |> then(& &1.body)
  end

  defp finch_options(finch, nil), do: [name: finch]
  defp finch_options(finch, pool_timeout), do: [name: finch, pool_timeout: pool_timeout]

  defp url(state, path), do: join_url(state.base_url, path)

  defp prepare_req_options(options) when is_list(options) do
    options
    |> maybe_map_recv_timeout()
    |> Keyword.drop(@non_req_options)
    |> Keyword.take(@req_passthrough)
  end

  defp prepare_req_options(_), do: []

  defp maybe_map_recv_timeout(options) do
    case Keyword.pop(options, :recv_timeout) do
      {nil, options} -> options
      {timeout, options} -> Keyword.put_new(options, :receive_timeout, timeout)
    end
  end

  defp call_timeout(options) when is_list(options) do
    options[:call_timeout] || options[:timeout] || @default_call_timeout
  end

  defp call_timeout(_options), do: @default_call_timeout

  defp call_response(%{"error" => error, "reason" => reason}) do
    {:error, "#{error} :: #{reason}"}
  end

  defp call_response(response), do: {:ok, response}

  defp create_admin_user(state, name, password) do
    body = name |> user_doc(password) |> Jason.encode!()
    request(:put, url(state, "_users/org.couchdb.user:#{name}"), body, state, [])
  end

  defp create_role(state, db_name, name) do
    roles = %{members: %{names: [], roles: []}, admins: %{names: [name], roles: []}}
    request(:put, url(state, "#{db_name}/_security"), Jason.encode!(roles), state, [])
  end

  defp user_doc(name, password) do
    %{name: name, password: password, roles: [], type: "user"}
  end

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
end
