defmodule Couchx.Support.ApplicationHelper do
  def base_repo_path(repo, directory) do
    config = repo.config()
    priv   = config[:priv] || "priv/#{Macro.underscore(repo)}"
    app    = Keyword.fetch!(config, :otp_app)

    Application.app_dir(app, Path.join(priv, directory))
  end
end
