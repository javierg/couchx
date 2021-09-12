defmodule Couchx.MixProject do
  use Mix.Project

  def project do
    [
      app: :couchx,
      version: "0.1.0",
      elixir: "~> 1.12",
      name: "Couchx",
      description: "Limited CouchDb Adapter for Ecto",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [
        :ecto,
        :logger
      ]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:ecto_sql, "~> 3.0"},
      {:httpoison, "~> 1.8"},
      {:inflex, "~> 2.0.0"},
      {:jason, "~>1.1"}
    ]
  end
end
