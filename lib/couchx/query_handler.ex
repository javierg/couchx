defmodule Couchx.QueryHandler do
  @empty_response  [
    %{"rows" => []},
    %{"docs" => []},
    %{"bookmark" => "nil", "docs" => []}
  ]

  def query_results([], _, _), do: {0, []}
  def query_results({:error, reason}, _, _), do: raise(Couchx.DbError, message: "#{reason}")

  def query_results([%{"_id" => _}|_] = docs, fields, metadata) do
    Enum.map(docs, &process_docs(&1, fields, metadata))
    |> execute_response
  end

  def query_results({:ok, response}, _, _) when response in @empty_response do
    {0, []}
  end

  def query_results({:ok, response}, fields, metadata)
    when is_list(response) do
    Enum.map(response, &query_results(&1, fields, metadata))
    |> execute_response
  end

  def query_results({:ok, response}, fields, metadata) do
    query_results(response, fields, metadata)
  end

  def query_results(%{"rows" => rows}, fields, metadata) do
    Enum.map(rows, &query_results(&1, fields, metadata))
    |> execute_response
  end

  def query_results(%{"docs" => docs}, fields, metadata) do
    Enum.map(docs, &process_docs(&1, fields, metadata))
    |> execute_response
  end

  def query_results(%{"doc" => doc}, fields, metadata) do
    process_docs(doc, fields, metadata)
  end

  def query_results(%{"ok" => true, "id"=> id, "rev"=> rev}, _fields, nil) do
    [_id: id, _rev: rev]
  end

  def query_results(doc, fields, metadata) do
    process_docs(doc, fields, metadata)
    |> execute_response
  end

  defp execute_response([]), do: {0, []}

  defp execute_response([item | _] = values) when is_list(item) do
    {length(values), values}
  end

  defp execute_response(value), do: {1, [value]}

  defp process_docs(rows, fields, meta) when is_list(rows) do
    Enum.map(rows, &process_docs(&1, fields, meta))
  end

  defp process_docs(doc, fields, nil) do
    Enum.reduce(fields, [], fn({key, value}, acc) ->
      confirmed_value = if doc[key], do: value, else: nil
      acc ++ confirmed_value
    end)
  end

  defp process_docs(doc, _fields, meta) do
    Enum.reduce(meta, [], fn({key, type}, acc) ->
      value = Map.get(doc, to_string(key))
      acc ++ [Ecto.Type.cast(type, value) |> elem(1)]
    end)
  end
end
