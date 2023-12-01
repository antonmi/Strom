defmodule Strom.MixProject do
  use Mix.Project

  def project do
    [
      app: :strom,
      version: "0.1.0",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps()
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
      {:alf, "0.10.0"}
    ]
  end
end
