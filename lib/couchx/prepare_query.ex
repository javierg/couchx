defmodule Couchx.PrepareQuery do
  @query_map [
    ==: "$eq",
    or: "$or",
    and: "$and",
    in: "$in"
  ]

  @operators [
    ==: "$eq",
    >: "$gt",
    <: "$lt",
    >=: "$gte",
    <=: "$lte",
    !=: "$ne"
  ]

  @operator_keys Keyword.keys(@operators)

  def call(%{wheres: wheres, limit: _limit} = query) do
    keys    = Enum.map(wheres, &parse_where/1)
    options = parse_options(query)

    [keys: keys, options: options]
  end

  def call(query) do
    query
    |> Map.merge(limit: nil)
    |> call
  end

  defp parse_where([]), do: []
  defp parse_where(%Ecto.Query.BooleanExpr{expr: expr}) do
    {condition, _, fields} = expr
    build_query_condition(condition, fields)
  end

  defp build_query_condition(condition, [{{_, [], [{_, [], [_]}, key]}, [], []}, value]) do
    cond do
      condition == :== ->
        %{ key => value }
      condition == :in and key == :_id ->
        %{ key => value }
      condition in @operator_keys ->
        %{ key => %{ @operators[condition] => value } }
      true ->
        {:error, "invalid query operator"}
    end
  end

  defp build_query_condition(condition, fields) do
    %{ @query_map[condition] => build_query(fields) }
  end

  defp build_query(fields) do
    Enum.map(fields, &build_field_condition/1)
  end

  defp build_field_condition({:^, [], [0]}), do: :primary_key
  defp build_field_condition({{_, _, [{_, _, [0]}, key]}, _, _}), do: %{key => :empty}

  defp build_field_condition({expr, _, [{{_, _, [{_, _, _}, key]}, _, _}, value]}) do
    %{ key => %{ @query_map[expr] => value } }
  end

  defp build_field_condition({expr, _, [{{_, _, [_, key]}, _, _}, value]}) do
    %{ key => %{ @query_map[expr] => value } }
  end

  defp build_field_condition({expr, _, list})
    when expr in ~w[== and]a and is_list(list) do
    Enum.reduce(list, %{}, &Map.merge(&2, build_field_condition(&1)))
  end

  defp build_field_condition({expr, _, list})
    when expr == :or and is_list(list) do
    %{"$or": [Enum.reduce(list, %{}, &Map.merge(&2, build_field_condition(&1)))] }
  end

  defp parse_options(%{order_bys: order_bys, limit: limit, offset: skip}) do
    %{}
    |> try_add_limit(limit)
    |> try_add_order(order_bys)
    |> try_add_skip(skip)
  end

  defp try_add_limit(options, nil), do: options
  defp try_add_limit(options, %{expr: limit}) do
    options
    |> Map.merge(%{limit: limit})
  end

  defp try_add_order(opts, []), do: opts
  defp try_add_order(opts, [%{expr: orders}]) do
    order = Enum.map(orders, &parse_orders/1)
    Map.merge(opts, %{sort: order})
  end

  defp try_add_skip(options, nil), do: options
  defp try_add_skip(options, %{expr: skip}) do
    options
    |> Map.merge(%{skip: skip})
  end

  defp parse_orders({order, {{_, _, [_, field]}, _, _}}) do
    %{"#{field}": order}
  end
end
