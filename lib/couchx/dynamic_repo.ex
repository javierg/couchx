defmodule Couchx.DynamicRepo do
  defmacro __using__(otp_app: otp_app, name: name) do
    quote location: :keep do
      @otp_app unquote(otp_app)
      @repo_name unquote(name)

      def default_options(_), do: [returning: true]

      def run(callback) do
        config = fetch_config()
        credentials = [username: config[:username], password: config[:password]]

        with_dynamic_repo(@repo_name, credentials, callback)
      end

      def with_dynamic_repo(name, credentials, callback) do
        name = if (is_atom(name)), do: name, else: String.to_atom(name)
        default_dynamic_repo = get_dynamic_repo()
        start_opts = [name: name] ++ credentials
        repo = __MODULE__.start_link(start_opts)
               |> maybe_fetch_repo()

        try do
          __MODULE__.put_dynamic_repo(repo)
          callback.()
        after
          __MODULE__.put_dynamic_repo(default_dynamic_repo)
          if Process.alive?(repo), do: Supervisor.stop(repo)
        end
      end

      defp fetch_config do
        Application.get_env(@otp_app, __MODULE__)
      end

      defp maybe_fetch_repo({:ok, repo}), do: repo
      defp maybe_fetch_repo({:error, {:already_started, repo}}), do: repo
    end
  end
end
