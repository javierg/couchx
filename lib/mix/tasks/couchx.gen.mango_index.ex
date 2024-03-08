defmodule Mix.Tasks.Couchx.Gen.MangoIndex do
  use Mix.Task
  import Macro, only: [camelize: 1]
  import Mix.Generator
  import Mix.Ecto
  import Couchx.Support.ApplicationHelper

  @cwd File.cwd!

  @impl true
  def run(args) do
    Mix.Task.run("app.config")

    [repo] = parse_repo(args)
    index_name = parse_index_name(args)

    assigns= [
      repo_name: repo_name(repo),
      index_name: index_name,
      index_module: index_module(index_name),
      fields: parsed_fields(args)
    ]

    copy_template(
      template_file_path(),
      migrations_path(repo, index_name),
      assigns
    )
  end

  defp repo_name(repo) do
    repo
    |> to_string
    |> String.replace("Elixir.", "")
  end

  defp index_module(name) do
    name
    |> String.replace("-", "_")
    |> camelize
  end

  defp migrations_path(repo, filename) do
    repo
    |> base_repo_path("index")
    |> Path.join("#{filename}.exs")
  end

  defp template_file_path do
    "#{@cwd}/lib/templates/mango_index.exs.eex"
  end

  defp parse_index_name(["-n"|t]) do
    List.first(t)
  end

  defp parse_index_name(["--name"|t]) do
    List.first(t)
  end

  defp parse_index_name([_|t]) do
    parse_index_name(t)
  end

  defp parse_index_name([]) do
    raise "Missing Index Name"
  end

  defp parsed_fields(["-f"|t]) do
    fields_to_sigil(t)
  end

  defp parsed_fields(["--fields"|t]) do
    fields_to_sigil(t)
  end

  defp parsed_fields([_|t]) do
    parsed_fields(t)
  end

  defp parsed_fields([]), do: "[]"

  defp fields_to_sigil(fields) do
    "~w[#{split_fields(List.first(fields))}]"
  end

  defp split_fields(fields) do
    fields
    |> String.replace(",", " ")
  end
end
