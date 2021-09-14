defmodule Couchx.Adapter do
  @moduledoc """
  Adapter to get basic query functionality into `Ecto` with `CouchDB`.

  ## Configuration

  It uses the same as Ecto pattern to config the Dbs with this format:

  ```
  config :my_app, MyRepo,
    username: "username",
    password: "password",
    database: "db_name",
    hostname: "localhost",
    protocol: "http",
    port: 5984
  ```

  ## Usage

  Couchx supports 1 main repo and many dynamic supervised repos.
  A dynamic repo will allow you to have multiple db connections in your application.
  To achieve this, you will need to setup a `DynamicSupervisor` and a `Registry` in the application like:

  ```
    def start(_type, _args) do
      children = [
        {DynamicSupervisor, strategy: :one_for_one, name: CouchxSupervisor}
        {Registry, keys: :unique, name: CouchxRegistry},
        ...
      ]
      ...
    end
  ```

  The Restry name is tied up to the code so it must be called `CouchxRegistry`.

  The main Repo is configured as any other Ecto Repo, so you can start it in the application just adding it to the children list.

  ```
    def start(_type, _args) do
      children = [
        MyDb.Repo
      ]
      ...
    end
  ```

  ### Dynamic Repo queries

  The dynamic repos are implemente with a Macro that you can get into your repo as:

  ```
    use CouchxDyncamicTepo, otp_app: :my_app, name: :my_repo
  ```

  This is used to setup a `run` function, with a callback as argument.
  To execute actions in a dynamic repo we follow this pattern:

  ```
    MyDynamicRepo.run( ->
      MyDynamicRepo.get(MyStruct, doc_id)
    end)
  ```

  Any Repo call inside the callback function will be run in a dynamically supervised connection.
  """
  import Couchx.CouchId

  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Schema
  @behaviour Ecto.Adapter.Queryable

  @query_map [
    ==: "$eq",
    or: "$or",
    and: "$and",
    in: "$in"
  ]

  defmacro __before_compile__(_env), do: :ok

  def init(config) do
    config           = put_conn_id(config)
    log              = Keyword.get(config, :log, :debug)
    telemetry_prefix = Keyword.fetch!(config, :telemetry_prefix)
    telemetry        = {config[:repo], log, telemetry_prefix ++ [:query]}
    spec             = couchdb_supervisor_spec(config)

    {:ok, spec, %{telemetry: telemetry, opts: [returning: true], config: config}}
  end

  def ensure_all_started(_repo, _type), do: HTTPoison.start
  def checkout(_adapter, _config, result), do: result

  def dumpers({:map, _}, type), do: [&Ecto.Type.embedded_dump(type, &1, :json)]
  def dumpers(_primitive, type), do: [type]

  def loaders({:map, _}, type), do: [&Ecto.Type.embedded_load(type, &1, :json)]
  def loaders(_primitive, type), do: [type]

  def autogenerate(:id), do: nil
  def autogenerate(:binary_id) do
    Ecto.UUID.cast!(Ecto.UUID.bingenerate)
  end

  def checked_out?(arg), do: arg

  def insert_all(_, _, _, _, _, _, _, _), do: nil

  def prepare(:all, query) do
    %{wheres: wheres} = query
    keys = Enum.map(wheres, &parse_where/1)
    {:nocache, {System.unique_integer([:positive]), keys}}
  end

  def prepare(:delete_all, _query) do
    {:nocache, {System.unique_integer([:positive]), [:delete]}}
  end

  def insert(meta, repo, fields, _on_conflict, returning, _options) do
    data = Enum.into(fields, %{})
           |> build_id(repo)
    url  = URI.encode_www_form(data._id)
    body = Jason.encode!(data)

    {:ok, response} = Couchx.DbConnection.insert(meta[:pid], url, body)
    response = Map.merge(data, %{_id: response["id"], _rev: response["rev"]})
    values = Enum.map(returning, fn(k)-> Map.get(response, k) end)

    {:ok, Enum.zip(returning, values)}
  end

  def execute(:view, meta, design, view, key, query_opts) do
    opts = query_opts
           |> Enum.into(%{})
           |> Map.merge(%{key: key})

    Couchx.DbConnection.get(meta[:pid], "_design/#{design}/_view/#{view}", opts)
    |> parse_view_response(opts[:include_docs])
  end

  def execute(meta, query_meta, query_cache, params, _opts) do
    {_, {_, keys}}        = query_cache
    %{select: select}     = query_meta
    {all_fields, module}  = fetch_fields(query_meta.sources)
    namespace             = build_namespace(module)

    fields_meta = fields_meta(select[:from])

    fields = case select[:postprocess] do
      {:map, keyfields} ->
        Keyword.keys(keyfields)
        |> Enum.map(&Atom.to_string/1)
      _-> all_fields
    end

    case do_query(meta[:pid], keys, namespace, params) do
      {:ok, %{"rows" => []}} ->
        {0, []}
      {:ok, %{"rows" => rows}} ->
        Enum.map(rows, fn(row)->
          row
          |> Map.get("doc")
          |> Map.take(fields)
          |> Map.values
        end) |> execute_response
      {:ok, response} ->
        process_docs(response, fields, fields_meta)
        |> execute_response
      [] ->
        {0, []}
      {:error, reason} ->
        raise Couchx.DbError, message: "#{reason}"
    end
  end

  def create_admin(server, name, password) do
    Couchx.DbConnection.create_admin(server, name, password)
  end

  def create_db(server, name) do
    Couchx.DbConnection.create_db(server, name)
  end

  def delete_admin(server, name) do
    Couchx.DbConnection.delete_admin(server, name)
  end

  def delete_db(server, name) do
    Couchx.DbConnection.delete_db(server, name)
  end

  defp parse_where([]), do: []
  defp parse_where(%Ecto.Query.BooleanExpr{expr: expr}) do
    {condition, _, fields} = expr
    build_query_condition(condition, fields)
  end

  defp build_query_condition(_, [{{_, [], [{_, [], [_]}, key]}, [], []}, value]) do
    %{ key => value }
  end

  defp build_query_condition(condition, fields) do
    %{ @query_map[condition] => build_query(fields) }
  end

  defp build_query(fields) do
    Enum.map(fields, &build_field_condition/1)
  end

  defp build_field_condition({:^, [], [0]}), do: :primary_key
  defp build_field_condition({{_, _, [{_, _, [0]}, key]}, _, _}), do: %{key => :empty}
  defp build_field_condition({expr, _, [{{_, _, [_, key]}, _, _}, value]}) do
    %{ key => %{ @query_map[expr] => value } }
  end

  defp execute_response([]), do: []
  defp execute_response(values) when is_list(values) do
    [item | _] = values
    if is_list(item) do
      {length(values), values}
    else
      {1, [values]}
    end
  end

  defp fetch_fields({{resource, nil, _}}) do
    module = ["Elixir", ".", resource]
               |> Enum.map(&Inflex.singularize/1)
               |> Enum.map(&String.capitalize/1)
               |> Enum.join
               |> String.to_existing_atom

    fetch_fields({{resource, module, nil}})
  end

  defp fetch_fields({{_resource, module, _}}) do
    fields = module.__struct__
               |> Map.keys
               |> Kernel.--([:__struct__, :__meta__])
               |> Enum.map(&Atom.to_string/1)

    {fields, module}
  end

  defp do_query(server, [%{_id: {:^, [], [0, _total]}}], namespace, ids) when is_list(ids) do
    do_query(server, [%{_id: ids}], namespace, [])
  end

  defp do_query(server, [%{_id: ids}], namespace, []) when is_list(ids) do
    doc_ids = Enum.map(ids, &namespace_id(namespace, &1))
              |> Enum.map(&URI.decode_www_form/1)

    Couchx.DbConnection.all_docs(server, doc_ids, include_docs: true)
    |> sanitize_collection
  end

  defp do_query(server, [%{"$eq" => [%{_id: :empty}, :primary_key]}], namespace, [id | _]) do
    Couchx.DbConnection.get(server, namespace_id(namespace, id))
  end

  defp do_query(server, [%{_id: {:^, [], [0]}}], namespace, [id | _]) do
    Couchx.DbConnection.get(server, namespace_id(namespace, id))
  end

  defp do_query(server, [:delete], namespace, []) do
    {:ok, %{"rows" => rows}} = Couchx.DbConnection.get(server, "_all_docs", [limit: 100, include_docs: true, startkey: Jason.encode!(namespace), endkey: Jason.encode!("#{namespace}/{}")])
    docs = Enum.map(rows, fn(%{"doc" => doc})-> %{_id: doc["_id"], _rev: doc["_rev"], _deleted: true} end)
    Couchx.DbConnection.bulk_docs(server, docs)
  end

  defp do_query(server, [], namespace, []) do
    {:ok, %{"rows" => rows}} = Couchx.DbConnection.get(server, "_all_docs", [include_docs: true, limit: 100, include_docs: true, startkey: Jason.encode!(namespace), endkey: Jason.encode!("#{namespace}/{}")])
    Enum.map(rows, &Map.get(&1, "doc"))
  end

  defp do_query(_, _, _, _), do: {:error, :not_implemented}

  defp build_namespace(module) do
    module
      |> to_string
      |> String.split(".")
      |> List.last
      |> Macro.underscore
  end

  defp namespace_id(namespace, id) do
    if Regex.match?(~r{^#{namespace}/}, id) do
      URI.encode_www_form(id)
    else
      namespace
        |> Kernel.<>("/#{id}")
        |> URI.encode_www_form
    end
  end

  defp build_id(data, %{schema: resource}) do
    resource
      |> to_string
      |> String.split(".")
      |> List.last
      |> Macro.underscore
      |> Kernel.<>("/#{base_id(data._id)}")
      |> update_data_id(data)
  end

  defp update_data_id(id, data) do
    Map.put(data, :_id, id)
  end

  defp parse_view_response({:ok, %{"rows" => rows}}, true) do
    rows
    |> Enum.map(&Map.get(&1, "doc"))
    |> Enum.map(&build_structs/1)
  end
  defp parse_view_response({:ok, %{"rows" => rows}}, _), do: rows

  defp build_structs(map) do
    doc_type = Map.get(map, "_id")
               |> String.replace(~r{(/.+)}, "")
               |> Macro.camelize

    module = :"Elixir.SDB.#{doc_type}" # TODO: pass module name in view execute

    doc = Enum.reduce(map, %{}, &keys_to_atoms/2)
    struct(module, doc)
  end

  defp keys_to_atoms({key, value}, acc) do
    Map.put(acc, String.to_atom(key), value)
  end

  defp put_conn_id(config), do: config ++ [id: config[:name]]

  defp couchdb_supervisor_spec(config) do
    {
      config[:id],
      {
        DynamicSupervisor,
        :start_child,
        [
          CouchxSupervisor,
          {Couchx.DbConnection, config}
        ]
      },
      :permanent,
      :infinity,
      :worker,
      [config[:id]]
    }
  end

  defp fields_meta({_, {_, _, _, fields_meta}}), do: fields_meta
  defp fields_meta(_), do: nil

  defp process_docs(rows, fields, meta) when is_list(rows) do
    Enum.map(rows, &process_docs(&1, fields, meta))
  end

  defp process_docs(doc, fields, nil) do
    doc
    |> Map.take(fields)
    |> Map.values
  end

  # TODO: move to process docs module to be imported
  defp process_docs(doc, fields, meta) do
    template = doc_template(meta)
    doc = Map.take(doc, fields)

    template
    |> Map.merge(doc)
    |> Map.values
  end

  defp doc_template(fields) do
    fields
    |> Enum.reduce(%{}, fn({key, type}, acc)->
         Map.put(acc, "#{key}", default_value(type))
       end)
  end

  defp default_value(:string), do: ""
  defp default_value(:integer), do: 0
  defp default_value(:boolean), do: false
  defp default_value({:array, _}), do: []
  defp default_value(:map), do: %{}
  defp default_value({:map, _}), do: %{}
  defp default_value(:binary_id), do: ""

  # Pending implementation

  def delete(meta, _meta_schema, params, _opts) do
    doc_id = URI.encode_www_form(params[:_id])
    Couchx.DbConnection.get(meta[:pid], doc_id)
    |> find_to_delete(meta[:pid], doc_id)
  end

  def insert_all(_a, _b, _c, _d, _e, _f, _g) do
  end

  def update(meta, _repo, fields, identity, returning, _) do
    data            = for {key, val} <- fields, into: %{}, do: {Atom.to_string(key), val}
    doc_id          = URI.encode_www_form(identity[:_id])
    {:ok, response} = Couchx.DbConnection.get(meta[:pid], doc_id)
    values          = Map.merge(response, data)
    body            = Jason.encode!(values)
    {:ok, response} = Couchx.DbConnection.insert(meta[:pid], doc_id, body)
    values          = fetch_insert_values(response, values, returning)

    {:ok, Enum.zip(returning, values)}
  end

  def update!(meta, repo, fields, identity, returning, a) do
    {:ok, values} = update(meta, repo, fields, identity, returning, a)
    values
  end

  def stream(_a, _b, _c, _d, _e) do
  end

  defp fetch_insert_values(%{"ok" => true}, response, returning) do
    data = for {key, val} <- response, into: %{}, do: {String.to_atom(key), val}

    Enum.map(returning, fn(k)->
      Map.get(data, k)
    end)
  end

  defp fetch_insert_values(_, _, _) do
    raise "Fail to save document"
  end

  defp find_to_delete({:ok, doc}, pid, doc_id) do
    Couchx.DbConnection.delete(pid, doc_id, doc["_rev"])
    |> handle_delete_response
  end

  defp find_to_delete({:error, error}, _, _) do
    raise error
  end

  defp handle_delete_response({:ok, _}) do
    {:ok, []}
  end

  defp handle_delete_response({:error, error}) do
    raise error
  end

  defp sanitize_collection({:ok, %{"rows" => rows}}) do
    rows = Enum.filter(rows, &Map.get(&1, "doc"))
    {:ok, %{"rows" => rows}}
  end

  defp sanitize_collection({:error, error}) do
    {:error, error}
  end
end
