defmodule Couchx.Finders do
  defmacro __using__(repo: repo, dynamic: is_dynamic) do
    quote location: :keep do
      @repo unquote(repo)
      @is_dynamic unquote(is_dynamic)

      if @is_dynamic do
        def find(id) do
          @repo.run(fn -> @repo.get(__MODULE__, id) end)
        end
      else
        def find(id) do
          @repo.get(__MODULE__, id)
        end
      end
    end
  end
end
