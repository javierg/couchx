defmodule Couchx.MangoIndex do
  defmacro __using__(repo_name: repo_name) do
    quote location: :keep do
      @repo_name unquote(repo_name)
      @default_type %{type: "json"}

      import Couchx.MangoIndex

      def create_index(name, do: block) do
        block
        |> build_index
        |> add_type
        |> Map.merge(%{name: name, ddoc: name})
        |> persist_index
      end

      def drop_index(name) do
        delete_index(name)
      end

      def drop_index(name, id) do
        delete_index(name, id)
      end

      defp add_type(%{type: _type} = doc), do: doc
      defp add_type(doc), do: Map.merge(doc, @default_type)

      defp build_index(index), do: %{index: index}

      defp persist_index(doc) do
        repo = Ecto.Repo.Registry.lookup(@repo_name)
        Couchx.DbConnection.index(repo.pid, doc)
        |> handle_response
      end

      defp delete_index(name, id \\ nil) do
        repo = Ecto.Repo.Registry.lookup(@repo_name)
        Couchx.DbConnection.delete(repo.pid, :index, name, id)
        |> handle_response
      end


      defp handle_response({:ok, response}), do: response
      defp handle_response({_, response}), do: {:error, response}
    end
  end
end
