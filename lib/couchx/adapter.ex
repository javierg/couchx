defmodule Couchx.Adapter do
  alias Couchx.PrepareQuery
  alias Couchx.QueryHandler
  alias Couchx.Constraint
  alias Couchx.DocumentState

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
  To achieve this, you will need to setup a `Registry` in the application like:

  ```
    def start(_type, _args) do
      children = [
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


  ## Migrations

  Couchx comes with a Mango index generator.

  ### Example

      $ mix couchx.gen.mango_index -r MyApp.Repo -n my-mango-index -f username,email

  This will create a file under `priv/my_app/repo/index/my-mango-index.exs` with contents as:

  ````
  defmodule MyApp.Repo.Index.MyMangoIndex do
    use Couchx.MangoIndex, repo_name: MyApp.Repo

    def up do
      create_index "my-mango-index" do
        %{
           fields: ["username", "email"],
         }
      end
    end

    def down do
      drop_index "my-mango-index"
    end
  end

  ````

  The Map inside the `create_index` block will be added to the `index` json object, so any structure that can go there can be added here.
  Currently only supported methods are

  ### create_index(String.t(), (-> Map.t()))

    - name: ID and Name for the index to be created in CouchDB, this will be used as `id` for the document persisted.
    - fun: A block that returns a formated Map for the index to be created, it will be parsed as JSON to the body of the index document.

  ### drop_index(String.t())

    - name: Id and Name for the index document to be deleted

  ### Examples

      $ mix couchx.mango_index

      Will add all indexes store under `priv/my_app/repo/index/` paths

      $ mix couchx.mango_index.down -r MyApp.Repo -n my-mango-index,my-other-index

      It will call down function on the Migration files

      ```
        priv/my_app/repo/index/my-mango-index.exs
        priv/my_app/repo/index/my-other-index.exs
      ```

      Removing the documents from the database.
  """
  import Couchx.CouchId

  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Schema
  @behaviour Ecto.Adapter.Queryable

  @encodable_keys ~w[key keys startkey endkey start_key end_key]a

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

  def prepare(:all, query) do
    prepared_query = PrepareQuery.call(query)
    {:nocache, {System.unique_integer([:positive]), prepared_query}}
  end

  def prepare(:delete_all, _query) do
    {:nocache, {System.unique_integer([:positive]), [:delete]}}
  end

  def insert(meta, repo, fields, _on_conflict, returning, _options) do
    constraints = Constraint.call(meta[:pid], repo, fields)

    constraints
    |> DocumentState.merge_constraints()
    |> do_insert(repo, constraints, fields, returning, meta)
  end

  def insert_all(meta, _repo, _fields, data, _on_conflict, schema, _returning, _opts) do
    docs = Enum.map(data, &Enum.into(&1, %{}))

    {:ok, res} = Couchx.DbConnection.bulk_docs(meta[:pid], docs)
    {:ok, Enum.map(res, &parse_bulk_response(&1, data, schema))}
  end

  def parse_bulk_response(%{"error" => _error, "id" => doc_id}, data, schema) do
    parse_bulk_response(%{"rev" => nil, "id" => doc_id}, data, schema)
  end

  def parse_bulk_response(%{"rev" => rev, "id" => doc_id}, data, schema) do
    fillers = Enum.map(schema, fn(_)-> nil end)
    doc_template = Enum.zip(schema, fillers)
    response_data = Enum.find(data, fn([{:_id, id} | _]) -> id == doc_id end) ++ [_rev: rev]
    doc = Keyword.merge(doc_template, response_data)

    Enum.map(schema, fn(key)-> Keyword.get(doc, key) end)
  end

  def execute(:view, meta, design, view, key, query_opts) do
    query_opts = query_opts ++ [key: Jason.encode!(key)]
    execute(:view, meta, design, view, query_opts)
  end

  def execute(:view, meta, design, view, query_opts) do
    opts = prepare_view_options(query_opts)
    Couchx.DbConnection.get(meta[:pid], "_design/#{design}/_view/#{view}", opts)
    |> parse_view_response(opts[:include_docs], query_opts[:module])
  end

  def execute(:find, meta, selector, fields, opts) do
    query = %{selector: selector, fields: fields}
    Couchx.DbConnection.find(meta[:pid], query, opts)
    |> parse_view_response(opts[:include_docs], opts[:module])
  end

  def execute(:request, meta, method, path, opts) do
    Couchx.DbConnection.raw_request(meta[:pid], method, path, opts)
    |> parse_view_response(opts[:include_docs], opts[:module])
  end

  def execute(meta, query_meta, query_cache, params, _opts) do
    {_, {_, query}}        = query_cache
    %{select: select}      = query_meta
    keys                   = fetch_query_keys(query_cache)
    query_options          = query[:options] || %{}
    {all_fields, module}   = fetch_fields(query_meta.sources)
    namespace              = build_namespace(module)

    fields_meta = fields_meta(select[:from])

    fields = case select[:postprocess] do
      {:map, keyfields} ->
        Keyword.keys(keyfields)
        |> Enum.map(&Atom.to_string/1)
      _-> all_fields
    end

    query = if select[:take] do
      %{fields: select[:take]}
    else
      %{}
    end

    do_query(meta[:pid], keys, namespace, params, Map.merge(query, query_options))
    |> QueryHandler.query_results(fields, fields_meta)
  end

  defp fetch_query_keys({_, {_, query}})
    when query == [:delete], do: [:delete]

  defp fetch_query_keys({_, {_, query}}), do: query[:keys]

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

  defp do_query(server, [%{_id: {:^, [], [0, _total]}}], namespace, ids, select) when is_list(ids) do
    do_query(server, [%{_id: ids}], namespace, [], select)
  end

  defp do_query(server, [%{_id: ids}], namespace, [], _select) when is_list(ids) do
    doc_ids = Enum.map(ids, &namespace_id(namespace, &1))
              |> Enum.map(&URI.decode_www_form/1)

    Couchx.DbConnection.all_docs(server, doc_ids, include_docs: true)
    |> sanitize_collection
  end

  defp do_query(server, [%{"$eq" => [%{_id: :empty}, :primary_key]}], namespace, [id | _], select) do
    do_query(server, [%{_id: id}], namespace, [], select)
  end

  defp do_query(server, [%{_id: {:^, [], [0]}}], namespace, [id | _], _select) do
    do_query(server, [%{_id: id}], namespace, [], [])
  end

  defp do_query(server, [%{_id: id}], namespace, [], []) do
    Couchx.DbConnection.get(server, namespace_id(namespace, id))
  end

  defp do_query(server, [%{_id: id}], namespace, [], select) when is_list(select) do
    namespaced_id = unencoded_namespace_id(namespace, id)
    query = select_query(%{_id: namespaced_id}, select)

    Couchx.DbConnection.find(server, query)
  end

  defp do_query(server, [%{_id: id}], namespace, [], _) do
    Couchx.DbConnection.get(server, namespace_id(namespace, id))
  end

  defp do_query(server, [:delete], namespace, [], _select) do
    {:ok, %{"rows" => rows}} = Couchx.DbConnection.get(server, "_all_docs", [limit: 100, include_docs: true, startkey: Jason.encode!(namespace), endkey: Jason.encode!("#{namespace}/{}")])
    docs = Enum.map(rows, fn(%{"doc" => doc})-> %{_id: doc["_id"], _rev: doc["_rev"], _deleted: true} end)
    Couchx.DbConnection.bulk_docs(server, docs)
  end

  defp do_query(server, [], namespace, [], query_options) do
    limit = query_options[:limit] || 100
    orders= query_options[:sort]

    opts =  [
      include_docs: true,
      limit: limit,
      include_docs: true,
      startkey: Jason.encode!(namespace),
      endkey: Jason.encode!("#{namespace}/{}")
    ]

    descending = if orders do
      [default_order | _] = orders

      default_order
      |> Map.values
      |> List.flatten
      |> List.first
      |> Kernel.==(:desc)
    end

    opts = if descending do
      startkey = opts[:startkey]
      endkey = opts[:endkey]

      Keyword.replace(opts, :startkey, endkey)
      |> Keyword.replace(:endkey, startkey)
      |> Kernel.++([descending: true])
    else
      opts
    end

    {:ok, %{"rows" => rows}} = Couchx.DbConnection.get(server, "_all_docs", opts)
    Enum.map(rows, &Map.get(&1, "doc"))
  end

  defp do_query(server, properties, namespace, values, query_options) when is_list(properties) do
    selector = extract_properties(namespace, properties, values)
    query_options = extract_options_properties(namespace, query_options, values)
    query = select_query(selector, query_options)
    Couchx.DbConnection.find(server, query)
  end

  defp extract_options_properties(namespace, query_options, values) do
    extract_properties(namespace, query_options, values)
    |> Map.drop([:type])
  end

  defp extract_properties(namespace, [%{"$and" => properties}], values) do
    extract_properties(namespace, properties, values)
  end

  defp extract_properties(namespace, properties, values) do
    Enum.reduce(properties, %{type: namespace}, &process_property(&1, &2, values))
  end

  defp select_query(selector, options) do
    %{selector: selector}
    |> Map.merge(options)
  end

  defp process_property({key, selector}, acc, values)
    when is_map(selector) do
      with [operator] <- Map.keys(selector),
           false <- operator == "$eq" do
        %{key => process_selector(selector, values)}
      else
        true ->
          case selector do
            %{"$eq" => {_, [], [value_index]}} ->
              value = Enum.fetch!(values, value_index)
              Map.put(acc, key, value)
            %{"$eq" => value} ->
              Map.put(acc, key, value)
        _ ->
            {:error, "unsupported selector"}
          end
      end
  end

  defp process_property({key, {:^, [], [value_index]}}, acc, values) do
    value = Enum.fetch!(values, value_index)
    Map.put(acc, key, value)
  end

  defp process_property({key, value}, acc, _values) do
    case key do
      "$or" ->
        Map.put(acc, "$and", [%{key => value}])
      _ ->
        Map.put(acc, key, value)
    end
  end

  defp process_property(property, acc, values) do
    Enum.reduce(property, %{}, &process_property(&1, &2, values))
    |> Map.merge(acc)
  end

  defp process_selector(%{"$in" => {:^, [], [start, amount]}}, values) do
    %{"$in" => Enum.slice(values, start, amount)}
  end

  defp process_selector(selector, _values), do: selector

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

  defp typed_document(data, %{schema: resource}) do
    Map.put(data, :type, build_namespace(resource))
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

  defp parse_view_response({:ok, %{"rows" => rows}}, true, module_name) do
    rows
    |> Enum.map(&Map.get(&1, "doc"))
    |> Enum.filter(& &1)
    |> Enum.map(&build_structs(&1, module_name))
  end

  defp parse_view_response({:ok, %{"rows" => rows}}, _, _), do: rows
  defp parse_view_response({:ok, %{"bookmark" => _, "docs" => docs}}, _, _), do: docs
  defp parse_view_response({:ok, raw_response}, _, _), do: raw_response

  defp parse_view_response({:error, _} = error, _, _), do: error

  defp build_structs(map, module_name) do
    doc = Enum.reduce(map, %{}, &keys_to_atoms/2)
    module_name = fetch_module_name(map, module_name)
    struct(module_name, doc)
  end

  defp fetch_module_name(map, nil) do
    doc_type = Map.get(map, "_id")
               |> String.replace(~r{(/.+)}, "")
               |> Macro.camelize

    :"Elixir.SDB.#{doc_type}"
  end

  defp fetch_module_name(_map, name), do: name

  defp keys_to_atoms({key, value}, acc) do
    Map.put(acc, String.to_atom(key), value)
  end

  defp put_conn_id(config), do: config ++ [id: config[:name]]

  defp couchdb_supervisor_spec(config) do
    sup_id = config[:id] || CouchxAdapter

    %{
      id: sup_id,
      start: {
        Couchx.DbConnection,
        :start_link,
        [
          config,
        ]
      },
      restart: :permanent,
      shutdown: :infinity,
      type: :supervisor
    }
  end

  defp fields_meta({_, {_, _, _, fields_meta}}), do: fields_meta
  defp fields_meta(_), do: nil

  # Pending implementation

  def delete(meta, meta_schema, params, _opts) do
    doc_id = meta_schema
             |> Map.get(:schema)
             |> build_namespace()
             |> namespace_id(params[:_id])

    Couchx.DbConnection.get(meta[:pid], doc_id)
    |> find_to_delete(meta[:pid], doc_id)
  end

  def insert_all(_a, _b, _c, _d, _e, _f, _g) do
  end

  def update(meta, repo, fields, identity, returning, _opts) do
    data            = for {key, val} <- fields, into: %{}, do: {Atom.to_string(key), val}
    doc_id          = URI.encode_www_form(identity[:_id])
    {:ok, response} = Couchx.DbConnection.get(meta[:pid], doc_id)

    prev_fields = for {key, val} <- response, do: {String.to_atom(key), val}
    constraints = Constraint.call(meta[:pid], repo, fields, prev_fields)

    constraints
    |> DocumentState.merge_constraints
    |> do_update(constraints, doc_id, response, data, returning, meta[:pid])
  end


  def update!(meta, repo, fields, identity, returning, a) do
    {:ok, values} = update(meta, repo, fields, identity, returning, a)
    values
  end

  def stream(_a, _b, _c, _d, _e) do
  end

  def do_insert(errors, _, _, _, _, _)
    when length(errors) > 0 do
    {:invalid, errors}
  end

  def do_insert(_errors, repo, constraints, fields, returning, meta) do
    data = Enum.into(fields, %{})
           |> build_id(repo)
           |> typed_document(repo)

    url  = URI.encode_www_form(data._id)
    body = Jason.encode!(data)

    constraints
    |> DocumentState.process_constraints(meta[:pid])
    |> try_to_persist_insert(data, returning, meta, url, body)
  end

  defp try_to_persist_insert({:invalid, constraints}, _, _, _, _, _) do
    {:invalid, constraints[:invalid]}
  end

  defp try_to_persist_insert(%{invalid: constraints}, _, _, _, _, _) do
    {:invalid, constraints}
  end

  defp try_to_persist_insert(%{error: errors}, _data, _returning, _meta, _url, _body) do
    {:error, errors}
  end

  defp try_to_persist_insert(%{ok: _}, data, returning, meta, url, body) do
    case Couchx.DbConnection.insert(meta[:pid], url, body) do
      {:ok, response} ->
        values = Map.merge(data, %{_rev: response["rev"]})
        values = fetch_insert_values(response, values, returning)
        {:ok, Enum.zip(returning, values)}
      {:error, error} ->
        {:error, error}
    end
  end

  defp do_update(errors, _constraints, _id, _response, _data, _returning, _server)
    when length(errors) > 0 do
      {:invalid, errors}
  end

  defp do_update(_errors, constraints, doc_id, response, data, returning, server) do
    constraints
    |> DocumentState.process_constraints(server)
    |> try_to_persist_update(doc_id, response, returning, data, server)
  end

  defp try_to_persist_update({:invalid, constraints}, _, _, _, _, _) do
    {:invalid, constraints[:invalid]}
  end

  defp try_to_persist_update(%{invalid: constraints}, _, _, _, _, _) do
    {:invalid, constraints}
  end

  defp try_to_persist_update(%{error: errors}, _doc_id, _response, _returning, _data, _server) do
    {:error, errors}
  end

  defp try_to_persist_update(%{ok: _}, doc_id, response, returning, data, server) do
    values = Map.merge(response, data)
    body   = Jason.encode!(values)

    case Couchx.DbConnection.insert(server, doc_id, body) do
      {:ok, response} ->
        values = fetch_insert_values(response, values, returning)
        {:ok, Enum.zip(returning, values)}
      {:error, error} ->
        {:error, error}
    end
  end

  defp fetch_insert_values(%{"ok" => true}, response, returning) do
    data = case response do
      %{_id: _id} -> response
      _ -> for {key, val} <- response, into: %{}, do: {String.to_atom(key), val}
    end

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

  defp unencoded_namespace_id(namespace, id) do
    namespace
    |> namespace_id(id)
    |> URI.decode_www_form
  end

  defp prepare_view_options(options) do
    @encodable_keys
    |> Enum.reduce(options, fn key, acc ->
       Keyword.replace(acc, key, Jason.encode!(options[key]))
    end)
    |> Enum.into(%{})
  end
end
