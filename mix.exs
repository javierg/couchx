defmodule Couchx.MixProject do
  use Mix.Project

  def project do
    [
      app: :couchx,
      version: "2.0.0",
      elixir: "~> 1.12",
      name: "Couchx",
      description: "Limited CouchDb Adapter for Ecto",
      start_permanent: Mix.env() == :prod,
      package: package(),
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
      {:ecto_sql, "~> 3.10"},
      {:ex_doc, "~> 0.11", only: :dev},
      {:inflex, "~> 2.0.0"},
      {:jason, "~> 1.1"},
      {:req, "~> 0.6"}
    ]
  end

  defp package do
    [
     files: ["lib", "mix.exs", "README.md"],
     maintainers: ["Javier Guerra"],
     licenses: ["MIT"],
     links: %{
       "GitHub" => "https://github.com/javierg/couchx",
       "Docs" => "https://hexdocs.pm/couchx"
       }
     ]
  end
end
