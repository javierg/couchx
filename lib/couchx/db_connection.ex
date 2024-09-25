defmodule Couchx.DbConnection do
  use GenServer, restart: :transient

  def start_link(args) do
    config = build_config(args)
    name = process_name(args[:name])

    GenServer.start_link(__MODULE__, config, name: name)
  end

  def init(args) do
    {:ok, args}
  end

  def terminate(reason, _state) do
    IO.inspect reason
  end

  def info(server), do: GenServer.call(server, :info)

  def insert(server, resource, body, options \\ []) do
    GenServer.call(server, {:insert, resource, body, options})
  end

  def bulk_docs(server, docs, options \\ []) do
    GenServer.call(server, {:bulk_docs, docs, options})
  end

  def get(server, resource, query \\ nil, options \\ []) do
    GenServer.call(server, {:get, resource, query, options})
  end

  def all_docs(server, keys, options \\ []) do
    GenServer.call(server, {:all_docs, keys, options})
  end

  def delete(server, resource, rev) do
    GenServer.call(server, {:delete, resource, rev})
  end

  def delete(server, :index, name, id) do
    id = if id, do: id, else: name
    GenServer.call(server, {:delete_index, name, id})
  end

  def create_db(server, name) do
    GenServer.call(server, {:create_db, name})
  end

  def delete_db(server, name) do
    GenServer.call(server, {:delete_db, name})
  end

  def create_admin(server, name, password) do
    GenServer.call(server, {:create_admin, name, password})
  end

  def delete_admin(server, name) do
    GenServer.call(server, {:delete_admin, name})
  end

  def find(server, query, options \\ []) do
    GenServer.call(server, {:find, query, options})
  end

  def index(server, doc) do
    GenServer.call(server, {:index, doc})
  end

  def raw_request(server, method, path, options \\ []) do
    timeout = options[:timeout] || 5_000

    GenServer.call(server, {:raw_request, method, path, options}, timeout)
  end

  def handle_call({:index, doc}, _from, state) do
    headers = state[:base_headers]
    url     = "#{state[:base_url]}/_index"
    body    = Jason.encode!(doc)

    request(:post, url, body, [headers: headers, options: []])
    |> call_response(state)
  end

  def handle_call({:delete_admin, name}, _from, state) do
    url      = "#{state[:base_url]}/_users/org.couchdb.user:#{name}"
    opts     = [headers: state[:base_headers], options: state[:options]]
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
    url  =  "#{state[:base_url]}/#{name}"
    opts = [headers: state[:base_headers], options: state[:options]]

    request(:put, url, [], opts)
    |> call_response(state)
  end

  def handle_call({:delete, doc_id, rev}, _from, state) do
    url  =  "#{state[:base_url]}/#{doc_id}?rev=#{rev}"
    opts = [headers: state[:base_headers], options: state[:options]]

    request(:delete, url, opts)
    |> call_response(state)
  end

  def handle_call({:delete_db, name}, _from, state) do
    url  =  "#{state[:base_url]}/#{name}"
    opts = [headers: state[:base_headers], options: state[:options]]

    request(:delete, url, opts)
    |> call_response(state)
  end

  def handle_call(:info, _from, state) do
    request(:get, state[:base_url], [headers: state[:base_headers], options: state[:options]])
    |> call_response(state)
  end

  def handle_call({:all_docs, keys, options}, _from, state) do
    headers   = state[:base_headers]
    with_docs = options[:include_docs] || false
    url       = state[:base_url] <> "/_all_docs?include_docs=#{with_docs}"
    body      = Jason.encode!(%{keys: keys})

    request(:post, url, body, [headers: headers, options: []])
    |> call_response(state)

  end

  def handle_call({:bulk_docs, docs, options}, _from, state) do
    headers = state[:base_headers]
    url     = state[:base_url] <> "/_bulk_docs"
    body    = Jason.encode!(%{docs: docs})

    request(:post, url, body, [headers: headers, options: options])
    |> call_response(state)
  end

  def handle_call({:insert, resource, body, options}, _from, state) do
    headers  = state[:base_headers]
    url      = state[:base_url] <> "/#{resource}"

    request(:put, url, body, [headers: headers, options: options])
    |> call_response(state)
  end

  def handle_call({:get, resource, query, options}, _from, state) do
    headers   = state[:base_headers]
    query_str = build_query_str(query)
    url       = "#{state[:base_url]}/#{resource}#{query_str}"

    request(:get, url, [headers: headers, options: options])
    |> call_response(state)
  end

  def handle_call({:raw_request, method, path, options}, _from, state) do
    query_str = build_query_str(options[:query_str])
    url = "#{state[:base_url]}/#{path}#{query_str}"
    req_options = state[:options] ++ options

    case method do
      :get ->
        request(method, url, headers: state[:base_headers], options: req_options)
      :delete ->
        request(:delete, url, headers: state[:base_headers], options: [])
      _ ->
        body = Jason.encode!(options[:body])
        request(method, url, body, headers: state[:base_headers], options: req_options)
    end
    |> call_response(state)

  end

  def handle_call({:find, query, options}, _from, state) do
    headers   = state[:base_headers]
    query_str = build_query_str(options[:query_str])
    url       = "#{state[:base_url]}/_find#{query_str}"
    body      = Jason.encode!(query)

    request(:post, url, body, [headers: headers, options: options])
    |> call_response(state)
  end

  def handle_call({:delete_index, name, id}, _from, state) do
    headers   = state[:base_headers]
    url       = "#{state[:base_url]}/_index/_design/#{id}/json/#{name}"

    request(:delete, url, [headers: headers, options: []])
    |> call_response(state)
  end

  defp request(:delete, url, opts) do
    headers = opts[:headers]
    options = opts[:options] || []

    HTTPoison.delete!(url, headers, options)
    |> decode_response
  end

  defp request(:get, url, opts) do
    headers = opts[:headers]
    options = opts[:options] || []

    HTTPoison.get!(url, headers, options)
    |> decode_response
  end

  defp request(:put, url, body, extras) do
    headers = extras[:headers]
    options = extras[:options] || []

    HTTPoison.put!(url, body, headers, options)
    |> decode_response
  end

  defp request(:post, url, body, extras) do
    headers = extras[:headers]
    options = extras[:options] || []

    HTTPoison.post!(url, body, headers, options)
    |> decode_response
  end

  defp call_response(%{"error" => error, "reason" => reason}, state) do
    {:reply, {:error, "#{error} :: #{reason}"}, state}
  end

  defp call_response(response, state), do: {:reply, {:ok, response}, state}

  defp build_query_str(nil), do: ""
  defp build_query_str(query) do
    "?#{URI.encode_query(query)}"
  end

  defp build_config(args) do
    %{
      base_url: base_url(args),
      base_headers: fetch_headers(args),
      options: []
    }
  end

  defp base_url(args) do
    "#{args[:protocol]}://#{args[:hostname]}:#{args[:port]}/#{args[:database]}"
  end

  defp fetch_headers(config) do
    credentials = "#{config[:username]}:#{config[:password]}"
                    |> Base.encode64()

    [
      {"Content-Type", "application/json"},
      {"Authorization", "Basic #{credentials}"}
    ]
  end

  defp decode_response(%{body: response}) do
    Jason.decode!(response)
  end

  defp create_admin_user(base_url, name, password, opts) do
    url  = "#{base_url}/_users/org.couchdb.user:#{name}"
    body = name
           |> user_doc(password)
           |> Jason.encode!

    request(:put, url, body, opts)
  end

  defp create_role(base_url, db_name, name, opts) do
    roles = %{members: %{ names: [], roles: [] }, admins: %{ names: [name], roles: [] } }
    request(:put, "#{base_url}/#{db_name}/_security", Jason.encode!(roles), opts)
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
