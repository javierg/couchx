defmodule Mix.Tasks.Couchx.MangoIndex.Down do
  use Mix.Task
  import Mix.Ecto
  import Couchx.Support.ApplicationHelper

  @moduledoc """
  Deletes an index or list of indexes provided from the Repo DB.

  The Repo passed as `-r` or `--r` will define the DB to lookup for the index document.
  There is also a parameter set as `-n` or `--names` which will need to match the `ddoc` and the index `name`.

  ## Examples

      $ mix couchx.mango_index.down -r MyApp.Repo -n my-index-ddoc

  The Repo have to match one in the `config.exs` file. Also couchx will need knowledge of the main OTP app supervising the repos.

  ## Examples

  ```
    confix :couchx, otp_app: :my_app
    config :my_app, ecto_repos: [MyApp.Repo, Custom.Repo]
  ```
  """

  @shortdoc "Delete indexes with names provided"

  @directory "index"

  @impl true
  def run(args) do
    Mix.Task.run("app.config")

    [repo] = parse_repo(args)

    otp_app = Application.get_env(:couchx, :otp_app)
    Application.ensure_all_started(otp_app)

    ensure_repo(repo, args)

    Couchx.Migrator.run(
      repo,
      :down,
      fetch_migrations(args, repo)
    )

    :ok
  end

  defp fetch_migrations(["-n"|t], repo) do
    build_migration_modules(repo, t)
  end

  defp fetch_migrations(["--names"|t], repo) do
    build_migration_modules(repo, t)
  end

  defp fetch_migrations([_|t], repo) do
    fetch_migrations(t, repo)
  end

  defp fetch_migrations([], _repo) do
    raise "Missing migrations to be removed."
  end

  defp build_migration_modules(repo, list) do
    List.first(list)
    |> String.split(",")
    |> Enum.map(&build_module_path(&1, repo))
  end

  defp build_module_path(module, repo) do
    module
    |> prepend("#{base_repo_path(repo, @directory)}/")
    |> Kernel.<>(".exs")
  end

  def prepend(string, suffix) do
    position = String.length(string) + String.length(suffix)
    String.pad_leading(string, position, suffix)
  end
end
