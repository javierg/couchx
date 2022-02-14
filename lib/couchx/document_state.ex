defmodule Couchx.DocumentState do
  alias Couchx.DbConnection

  def merge_constraints(constraints) do
    constraints
    |> Enum.filter(fn({state, _})-> state == :invalid end)
    |> Keyword.values
    |> List.flatten
  end

  def process_constraints([], _server), do: %{ok: true}

  def process_constraints(constraints, server) do
    Enum.map(constraints, fn({:ok, constraint})->
      case constraint do
        %{type: :unique} = constraint ->
          constraint_doc_id = URI.encode_www_form(constraint.id)
          constraint_doc = %{_id: constraint.id, type: "constraint"} |> Jason.encode!
          DbConnection.insert(server, constraint_doc_id, constraint_doc)
        true ->
          {:ok, true}
      end
    end)
    |> Enum.group_by(fn({state, _})-> state end)
    |> Enum.reduce(%{}, fn({k, v}, acc)-> Map.put(acc, k, Keyword.values(v)) end)
  end
end
