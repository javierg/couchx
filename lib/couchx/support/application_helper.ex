defmodule Couchx.Support.ApplicationHelper do
  @moduledoc """
  Helper module with repo information tools
  """

  def base_repo_path(repo, directory) do
    config = repo.config()
    priv = config[:priv] || "priv/#{Macro.underscore(repo)}"
    app = Keyword.fetch!(config, :otp_app)

    Application.app_dir(app, Path.join(priv, directory))
  end
end
