defmodule Couchx.QueryHandler do
  @empty_response  [
    %{"rows" => []},
    %{"docs" => []},
    %{"bookmark" => "nil", "docs" => []}
  ]

  def query_results([], _, _), do: {0, []}
  def query_results({:error, reason}, _, _), do: raise Couchx.DbError, message: "#{reason}"

  def query_results({:ok, response}, _, _) when response in @empty_response do
    {0, []}
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
    fields
    |> Enum.reduce([], fn({key, value}, acc)->
         confirmed_value = if doc[key], do: value, else: nil
         acc ++ confirmed_value
       end)
  end

  defp process_docs(doc, fields, meta) do
    template = doc_template(meta)
    doc = Map.take(doc, fields)

    template
    |> Map.merge(doc)
    |> Map.values
  end

  defp doc_template(fields) do
    fields
    |> Enum.reduce(%{}, fn({key, type}, acc)->
         Map.put(acc, "#{key}", default_value(type))
       end)
  end

  defp default_value(:string), do: ""
  defp default_value(:integer), do: 0
  defp default_value(:boolean), do: false
  defp default_value({:array, _}), do: []
  defp default_value(:map), do: %{}
  defp default_value({:map, _}), do: %{}
  defp default_value(:binary_id), do: ""
end
