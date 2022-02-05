defmodule Couchx.Constraint do
  def call(server, %{source: source, schema: schema}, fields) do
    params = Enum.into(fields, %{})
    changeset = schema.changeset(schema.__struct__, params)

    changeset
    |> Map.get(:constraints)
    |> Enum.map(&process_constraints(&1, source, fields, server))
  end

  def call(_server, _repo, _fields) do
    {:error, "unknow error"}
  end

  defp process_constraints(%{constraint: constraint, type: :unique}, source, fields, server) do
    unique_fields = constraint_to_fields(constraint)

    fields
    |> Keyword.take(unique_fields)
    |> validate_unique_fields_presence(unique_fields)
    |> validate_uniqueness(constraint, source, server)
  end

  defp process_constraints(%{constraint: constraint, field: field, type: :foreign_key}, _source, fields, server) do
    doc_id = Keyword.get(fields, field)
             |> URI.encode_www_form

    case Couchx.DbConnection.get(server, doc_id) do
      {:ok, _} ->
        {:ok, true}
      {:error, "not_found :: missing"} ->
        {:invalid, [foreign_key: constraint]}
    end
  end

  defp process_constraints(%{type: :unique}, _source, _fields, _server) do
    raise "Unique indexes requires a name set with field names separated by \"-\"."
  end

  defp process_constraints(_constraints, _source, _fields, _server), do: {:ok, true}

  defp constraint_to_fields(constraint) do
    String.split(constraint, "-")
    |> Enum.map(&String.to_atom/1)
    |> List.delete(:index)
  end

  defp validate_uniqueness(false, _, _, _server) do
    raise "All unique fields are required."
  end

  defp validate_uniqueness(fields, constraint, source, server) do
    doc_id = "#{source}-#{Keyword.values(fields) |> Enum.join("-")}"

    Couchx.DbConnection.get(server, URI.encode_www_form(doc_id))
    |> try_confirm_uniqueness(constraint, doc_id)
  end

  defp try_confirm_uniqueness({:error, "not_found :: missing"}, constraint, doc_id) do
    {:ok, %{type: :unique, constraint: constraint, id: doc_id}}
  end

  defp try_confirm_uniqueness({:ok, _response}, constraint, _doc_id) do
    {:invalid, [unique: constraint]}
  end

  defp validate_unique_fields_presence([], _expected_fields), do: false

  defp validate_unique_fields_presence(fields, expected_fields) do
    expected_fields -- Keyword.keys(fields) == [] &&
      fields
  end
end
