defmodule Mix.Tasks.Couchx.MangoIndex do
  use Mix.Task
  import Mix.Ecto

  @moduledoc """
  Creates the index documents defined in the `priv/repo/index/` path.

  The migration files should be `exs` files and can have a `up` and `down` functions.

  ## Examples

  ```
  defmodule MyApp.Repo.Index.MyIndex do
    use Couchx.MangoIndex, repo_name: BS.Repo

    def up do
      create_index("my-index") do
        %{fields: ["name", "email"]}
      end
    end

    def down do
      drop_index("my-index")
    end
  end
  ```

  Current convention is that the file name uses `-` to separate words, and this is used to create de module name.

  ## Example

      $ priv/my_repo/index/my-index.exs
      $ mix couchx.mango_index

  Couchx will need knowledge of the main OTP app supervising the repos, also we need to configure ecto repos.

  ## Example

  ```
    confix :couchx, otp_app: :my_app
    config :my_app, ecto_repos: [MyApp.Repo, Custom.Repo]
  ```

  The task will look for paths for all the repos and process the indexes it finds.
  """


  @shortdoc "create indexes declared on priv/repo_path"

  @impl true
  def run(args) do
    Mix.Task.run("app.config")
    repos = parse_repo(args)

    otp_app = Application.get_env(:couchx, :otp_app)
    Application.ensure_all_started(otp_app)

    for repo <- repos do
      ensure_repo(repo, args)

      Couchx.Migrator.run(repo, :up)
      |> report_response
    end

    :ok
  end

  defp report_response([]), do: nil

  defp report_response({:error, response}) do
    IO.puts "Error #{response}"
  end

  defp report_response({:ok, indexes, _}) do
    report_response(indexes)
  end

  defp report_response(indexes) when is_list(indexes) do
    for index <- indexes do
      case index do
        %{"result" => "exists"} = index ->
          state = if index["result"] == "exists", do: "already exists", else: index["result"]
          IO.puts "\n==== Index #{index["name"]} #{state} ====="
          IO.inspect index
          IO.puts "========================\n"
        {:error, state} ->
          IO.puts "\n======================"
          IO.puts state
          IO.puts "======================\n"
        default ->
          IO.inspect default
      end
    end
  end
end
