defmodule Couchx.Migrator do
  import Couchx.Support.ApplicationHelper

  def run(repo, :up) do
    repo
    |> index_path
    |> Path.wildcard
    |> require_files
    |> Enum.map(&fetch_modules/1)
    |> run_direction(:up)
  end

  def run(_repo, :down, migrations) do
    migrations
    |> require_files
    |> Enum.map(&fetch_modules/1)
    |> run_direction(:down)
  end

  defp base_module_atom(module) do
    String.to_existing_atom(module)
  end

  defp ensure_migration(migration) when is_atom(migration) do
    migration
  end

  defp ensure_migration(migration) when is_binary(migration) do
    String.to_existing_atom(migration)
  end

  defp fetch_modules(file_path) do
    migration = migration_code(file_path)

    ~r{defmodule \s+ (?<module>\S+) }x
    |> Regex.named_captures(migration)
    |> migration_to_module
  end

  defp index_path(repo, directory \\ "index") do
    repo
    |> base_repo_path(directory)
    |> Path.join("*.exs")
  end

  defp migration_code(file_path) do
    file_path
    |> to_string
    |> File.read!
  end

  defp migration_to_module(%{"module" => module}) do
    "Elixir."
    |> Kernel.<>(module)
    |> base_module_atom
  end
  defp require_files(files) do
    Enum.each(files, &Code.require_file/1)
    files
  end

  def run_direction(migrations, direction) do
    migrations
    |> Enum.map(&ensure_migration/1)
    |> Enum.filter(&with_direction?(&1, direction))
    |> Enum.map(&apply(&1, direction, []))
  end

  defp with_direction?(module, direction) do
    Keyword.has_key?(module.__info__(:functions), direction)
  end
end
