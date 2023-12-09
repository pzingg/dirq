defmodule Dirq.MixProject do
  use Mix.Project

  def project do
    [
      app: :dirq,
      version: "0.1.0",
      elixir: "~> 1.13",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {Dirq.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:perc, "~> 1.0"},
      {:jason, "~> 1.4"},
      {:briefly, "~> 0.5", only: :test},
      {:dialyxir, "~> 1.4", only: :dev},
      {:credo, "~> 1.7", only: :dev}
      # {:dep_from_hexpm, "~> 0.3.0"},
      # {:dep_from_git, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"}
    ]
  end
end
