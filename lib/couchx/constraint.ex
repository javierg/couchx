defmodule Couchx.Constraint do
  def call(server, repo, fields, prev_fields \\ [])

  def call(
      server,
      %{source: source, schema: schema},
      fields,
      prev_fields
    ) do
      if with_schema?(schema) do
        params = Enum.into(fields, %{})
        schema_struct = struct(schema.__struct__, prev_fields)
        changeset = schema.changeset(schema_struct, params)
        fields = Keyword.merge(prev_fields, fields)

        changeset
        |> Map.get(:constraints)
        |> Enum.map(&process_constraints(&1, source, fields, server, prev_fields))
      else
        [{:ok, true}]
      end
  end

  def call(_server, _repo, _fields, _prev_fields) do
    {:error, "unknow error"}
  end

  defp with_schema?(module) do
    function_exported?(module, :changeset, 2)
  end

  defp process_constraints(
    %{constraint: constraint, type: :unique},
    source,
    fields,
    server,
    prev_fields
  ) when prev_fields == [] do
    unique_fields = constraint_to_fields(constraint)

    fields
    |> Keyword.take(unique_fields)
    |> validate_unique_fields_presence(unique_fields)
    |> validate_uniqueness(unique_fields, constraint, source, server)
  end

  defp process_constraints(
    %{constraint: constraint, type: :unique} = constraints,
    source,
    fields,
    server,
    prev_fields
  ) do
    unique_fields = constraint_to_fields(constraint)
    doc_id = unique_doc_id(fields, unique_fields, source)
    prev_doc_id = unique_doc_id(prev_fields, unique_fields, source)

    if (doc_id == prev_doc_id) do
      {:ok, true}
    else
      process_constraints(constraints, source, fields, server, [])
    end
  end

  defp process_constraints(%{constraint: constraint, field: field, type: :foreign_key}, _source, fields, server, _prev_fields) do
    doc_id = Keyword.get(fields, field)
             |> URI.encode_www_form

    case Couchx.DbConnection.get(server, doc_id) do
      {:ok, _} ->
        {:ok, true}
      {:error, "not_found :: " <> _reason} ->
        {:invalid, [foreign_key: constraint]}
    end
  end

  defp process_constraints(%{type: :unique}, _source, _fields, _server, _prev_fields) do
    raise "Unique indexes requires a name set with field names separated by \"-\"."
  end

  defp process_constraints(_constraints, _source, _fields, _server, _prev_fields), do: {:ok, true}

  defp constraint_to_fields(constraint) do
    String.split(constraint, "-")
    |> Enum.map(&String.to_atom/1)
    |> List.delete(:index)
  end

  defp validate_uniqueness(false, _,  _, _, _server) do
    raise "All unique fields are required."
  end

  defp validate_uniqueness(fields, unique_fields, constraint, source, server) do
    doc_id = unique_doc_id(fields, unique_fields, source)

    Couchx.DbConnection.get(server, URI.encode_www_form(doc_id))
    |> try_confirm_uniqueness(constraint, doc_id)
  end

  defp try_confirm_uniqueness({:error, "not_found :: " <> _reason}, constraint, doc_id) do
    {:ok, %{type: :unique, constraint: constraint, id: doc_id}}
  end

  defp try_confirm_uniqueness({:ok, _response}, constraint, _doc_id) do
    {:invalid, [unique: constraint]}
  end

  defp validate_unique_fields_presence([], _expected), do: false

  defp validate_unique_fields_presence(fields, expected) do
    present = Keyword.keys(fields)

    expected -- present == [] &&
      fields
  end

  defp unique_doc_id(fields, unique_fields, source) do
    values = fields
             |> Keyword.take(unique_fields)
             |> Keyword.values
             |> Enum.join("-")

    "#{source}-#{values}"
  end
end
