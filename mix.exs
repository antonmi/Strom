defmodule Strom.MixProject do
  use Mix.Project

  def project do
    [
      app: :strom,
      version: "0.8.7",
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
      #      extra_applications: [:logger],
      extra_applications: [:logger, :observer, :wx, :runtime_tools],
      mod: {Strom.Application, []}
    ]
  end

  defp deps do
    [
      {:dialyxir, "~> 1.4", only: [:dev], runtime: false},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false}
    ]
  end

  defp description do
    "Composable components for stream processing."
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
