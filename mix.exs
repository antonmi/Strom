defmodule Strom.MixProject do
  use Mix.Project

  def project do
    [
      app: :strom,
      version: "0.4.1",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      description: description(),
      package: package(),
      deps: deps(),
      source_url: "https://github.com/antonmi/Strom"
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {Strom.Application, []}
    ]
  end

  defp deps do
    [
      {:alf, "0.11.0", only: [:dev, :test]},
      {:dialyxir, "~> 1.0", only: [:dev], runtime: false},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false}
    ]
  end

  defp description do
    "Flow-Based Programming"
  end

  defp package do
    [
      files: ~w(lib mix.exs README.md),
      maintainers: ["Anton Mishchuk"],
      licenses: ["MIT"],
      links: %{"github" => "https://github.com/antonmi/Strom"}
    ]
  end
end
