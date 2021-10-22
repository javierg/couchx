defmodule Couchx.Adapter do
  alias Couchx.PrepareQuery
  alias Couchx.QueryHandler

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
    prepared_query = PrepareQuery.call(query)
    {:nocache, {System.unique_integer([:positive]), prepared_query}}
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

  def execute(:find, meta, selector, fields, opts) do
    query = %{selector: selector, fields: fields}

    Couchx.DbConnection.find(meta[:pid], query, opts)
    |> parse_view_response(opts[:include_docs])
  end

  def execute(meta, query_meta, query_cache, params, _opts) do
    {_, {_, query}}        = query_cache
    %{select: select}      = query_meta
    keys                   = query[:keys]
    query_options          = query[:options]
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
    selector = Enum.reduce(properties, %{type: namespace}, &process_property(&1, &2, values))
    query = select_query(selector, query_options)
    Couchx.DbConnection.find(server, query)
  end

  defp select_query(selector, options) do
    %{selector: selector}
    |> Map.merge(options)
  end

  defp process_property({key, {:^, [], [value_index]}}, acc, values) do
    value = Enum.fetch!(values, value_index)
    Map.put(acc, key, value)
  end

  defp process_property({key, value}, acc, _values) do
    Map.put(acc, key, value)
  end

  defp process_property(property, acc, values) do
    Enum.reduce(property, %{}, &process_property(&1, &2, values))
    |> Map.merge(acc)
  end

  #defp do_query(_, _, _, _), do: {:error, :not_implemented}

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
  defp parse_view_response({:ok, %{"bookmark" => _, "docs" => docs}}, _), do: docs

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

  defp unencoded_namespace_id(namespace, id) do
    namespace
    |> namespace_id(id)
    |> URI.decode_www_form
  end
end
