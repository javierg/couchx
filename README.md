# Couchx

### CouchDB Adapter for Ecto3.

The Adapter supports 1 main Repo, but also dynamic repos where you can querie on multiple Dbs with a dynamic supervisor.

The supported functions in this version are:

```
Repo.get Struct, doc_id
Repo.all from doc in Struct, where: doc._id in ^doc_ids_list
Repo.insert Struct, doc
Repo.delete Struct, %{_id: doc_id}
Repo.update changeset
```

It adds a simple way to execute JS view queries with:

```
  def query(name, design, view_map, key, opts \\ []) do
    {adapter, meta} = Ecto.Repo.Registry.lookup(name)
    adapter.execute(:view, meta, design, view_map, key, opts)
  end
```

Not ideal, so open to suggestions here.

## TODO:

* Repo.insert_all
* Repo.delete_all
* Bulk doc updates
* Better error handling
* Mango Queries
* A better view query pattern
* Auto indexing requested JS queries
* Index management
* Add tests

## Installation

The package can be installed by adding `couchx` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:couchx, "~> 0.1.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at [https://hexdocs.pm/couchx](https://hexdocs.pm/couchx).
