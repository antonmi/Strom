defmodule Strom.FunctionTest do
  use ExUnit.Case, async: true

  alias Strom.Function
  alias Strom.Source
  alias Strom.Source.ReadLines

  setup do
    path = "test/data/orders.csv"
    source = Source.start(%ReadLines{path: path})
    flow = Source.call(%{}, source, :orders)
    %{flow: flow}
  end

  test "function", %{flow: flow} do
    function =
      Function.start(fn stream ->
        Stream.map(stream, &"foo-#{&1}")
      end)

    %{orders: orders} = Function.call(flow, function, [:orders])
    orders = Enum.to_list(orders)
    Enum.each(orders, fn line -> assert String.starts_with?(line, "foo-") end)
    assert length(orders) == length(String.split(File.read!("test/data/orders.csv"), "\n"))
  end

  test "with several streams", %{flow: flow} do
    path = "test/data/parcels.csv"
    source2 = Source.start(%ReadLines{path: path})

    function =
      Function.start(fn stream ->
        Stream.map(stream, &"foo-#{&1}")
      end)

    %{orders: orders, parcels: parcels} =
      flow
      |> Source.call(source2, :parcels)
      |> Function.call(function, [:parcels])

    parcels = Enum.to_list(parcels)
    Enum.each(parcels, fn line -> assert String.starts_with?(line, "foo-") end)
    assert length(parcels) == length(String.split(File.read!("test/data/parcels.csv"), "\n"))

    orders = Enum.to_list(orders)
    assert Enum.join(orders, "\n") == File.read!("test/data/orders.csv")
  end
end
