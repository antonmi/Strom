defmodule Strom.SplitterTest do
  use ExUnit.Case, async: false

  alias Strom.Source
  alias Strom.Source.ReadLines
  alias Strom.Splitter

  def orders_and_parcels do
    orders =
      "test/data/orders.csv"
      |> File.read!()
      |> String.split("\n")

    parcels =
      "test/data/parcels.csv"
      |> File.read!()
      |> String.split("\n")

    {orders, parcels}
  end

  setup do
    source1 = Source.start(%ReadLines{path: "test/data/orders.csv"})
    source2 = Source.start(%ReadLines{path: "test/data/parcels.csv"})

    flow =
      %{}
      |> Source.stream(source1, :orders)
      |> Source.stream(source2, :parcels)

    %{flow: flow}
  end

  test "splitter", %{flow: flow} do
    splitter = Splitter.start([])

    assert %{
             :parcels => parcels,
             "111" => stream1,
             "222" => stream2,
             "333" => stream3
           } =
             flow
             |> Splitter.stream(splitter, :orders, %{
               "111" => fn el -> String.contains?(el, ",111,") end,
               "222" => fn el -> String.contains?(el, ",222,") end,
               "333" => fn el -> String.contains?(el, ",333,") end
             })

    orders111 = Enum.to_list(stream1)
    orders222 = Enum.to_list(stream2)
    orders333 = Enum.to_list(stream3)

    orders = orders111 ++ orders222 ++ orders333
    {original_orders, original_parcels} = orders_and_parcels()
    assert orders -- original_orders == []
    assert original_orders -- orders == []

    parcels = Enum.to_list(parcels)
    assert parcels -- original_parcels == []
    assert original_parcels -- parcels == []
  end

  test "stop" do
    splitter = Splitter.start([])
    assert Process.alive?(splitter.pid)
    :ok = Splitter.stop(splitter)
    refute Process.alive?(splitter.pid)
  end
end
