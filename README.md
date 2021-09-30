# Couchx

### CouchDB Adapter for Ecto3.

The Adapter supports 1 main Repo, but also dynamic repos where you can querie on multiple Dbs with a dynamic supervisor.

The supported functions in this version are:

```
Repo.get Struct, doc_id
Repo.all from doc in Struct, where: doc._id in ^doc_ids_list
Repo.all from doc in Struct, where: doc.field == ^field
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

### Mango support

Currently support for mango is limited to do queries on single property, on previously created indexes.
The selector is a map with the `type` property preset to the model namespace, such as:

```
%{
  selector: %{
    type: "user",
    email: "email@email.com"
  }
}
```

This will be the query generated by:

```
import Ecto.Query

Repo.all from u in User, where: u.email == "email@email.com"
```

It will also work with:

```
import Ecto.Query

email = "email@email.com"
Repo.all from u in User, where: u.email == ^email
```

## Mango Index

Couchx have a couple of tasks to handle database indexing:

```
$ mix couchx.gen.mango_index -r MyApp.Repo -n my-mango-index -f name,email
```

This command will generate a index file on `priv/my_app/repo/index/my-mango-index.exs`
It rely on repos declared under `config.exs`

```
import Config

config :my_app, ecto_repos: ["repo", "custom_repo"]
...
```

And build a file with contents such as:

```
defmodule MyApp.Repo.Index.MyMangoIndex do
  use Couchx.MangoIndex, repo_name: MyApp.Repo

  def up do
    create_index "my-mango-index" do
      %{fields: ["name", "email"]}
    end
  end

  def down do
    drop_index("my-mango-index")
  end
```

This file will be executed with

```
$ mix couchx.mango_index
```

which will persist the index document in the database defined by the repo.
And if you want to remove the index you can call:

`$ mix couch.mango_index.down -r MyApp.Repo, -n my-mango-index`

## TODO:

* Repo.insert_all
* Repo.delete_all
* Bulk doc updates
* Better error handling
* More Mango Queries
* A better view query pattern
* Auto indexing requested JS queries
* JS view index management
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
