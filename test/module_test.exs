defmodule Strom.ModuleTest do
  use ExUnit.Case, async: true

  alias Strom.Module
  alias Strom.Source
  alias Strom.Source.ReadLines

  defmodule MyModule do
    defstruct state: nil

    def start(_opts) do
      %__MODULE__{state: :state}
    end

    def call(stream, %__MODULE__{state: :state}) do
      Stream.map(stream, &"foo-#{&1}")
    end

    def stop(%__MODULE__{state: :state}), do: :ok
  end

  setup do
    path = "test/data/orders.csv"
    source = Source.start(%ReadLines{path: path})
    flow = Source.call(%{}, source, :orders)
    %{flow: flow}
  end

  test "function", %{flow: flow} do
    module = Module.start(MyModule)
    %{orders: orders} = Module.call(flow, module, [:orders])
    orders = Enum.to_list(orders)
    Enum.each(orders, fn line -> assert String.starts_with?(line, "foo-") end)
    assert length(orders) == length(String.split(File.read!("test/data/orders.csv"), "\n"))
  end

  test "with several streams", %{flow: flow} do
    path = "test/data/parcels.csv"
    source2 = Source.start(%ReadLines{path: path})

    module = Module.start(MyModule)

    %{orders: orders, parcels: parcels} =
      flow
      |> Source.call(source2, :parcels)
      |> Module.call(module, [:parcels])

    parcels = Enum.to_list(parcels)
    Enum.each(parcels, fn line -> assert String.starts_with?(line, "foo-") end)
    assert length(parcels) == length(String.split(File.read!("test/data/parcels.csv"), "\n"))

    orders = Enum.to_list(orders)
    assert Enum.join(orders, "\n") == File.read!("test/data/orders.csv")
  end
end
