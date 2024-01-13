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
    source1 =
      :orders
      |> Source.new(%ReadLines{path: "test/data/orders.csv"})
      |> Source.start()

    source2 =
      :parcels
      |> Source.new(%ReadLines{path: "test/data/parcels.csv"})
      |> Source.start()

    flow =
      %{}
      |> Source.call(source1)
      |> Source.call(source2)

    %{flow: flow}
  end

  test "start and stop" do
    splitter = Splitter.start(Splitter.new(:s, [:s1, :s2]))
    assert Process.alive?(splitter.pid)
    :ok = Splitter.stop(splitter)
    refute Process.alive?(splitter.pid)
  end

  test "splitter with list of streams", %{flow: flow} do
    splitter =
      :orders
      |> Splitter.new(["111", "222", "333"])
      |> Splitter.start()

    assert %{
             :parcels => parcels,
             "111" => stream1,
             "222" => stream2,
             "333" => stream3
           } =
             flow
             |> Splitter.call(splitter)

    task111 = Task.async(fn -> Enum.to_list(stream1) end)
    task222 = Task.async(fn -> Enum.to_list(stream2) end)
    task333 = Task.async(fn -> Enum.to_list(stream3) end)

    orders111 = Task.await(task111)
    orders222 = Task.await(task222)
    orders333 = Task.await(task333)

    {original_orders, original_parcels} = orders_and_parcels()

    assert length(orders111) == length(original_orders)
    assert length(orders222) == length(original_orders)
    assert length(orders333) == length(original_orders)

    parcels = Enum.to_list(parcels)
    assert parcels -- original_parcels == []
    assert original_parcels -- parcels == []
  end

  test "splitter with partitions", %{flow: flow} do
    splitter =
      :orders
      |> Splitter.new(%{
        "111" => fn el -> String.contains?(el, ",111,") end,
        "222" => fn el -> String.contains?(el, ",222,") end,
        "333" => fn el -> String.contains?(el, ",333,") end
      })
      |> Splitter.start()

    assert %{
             :parcels => parcels,
             "111" => stream1,
             "222" => stream2,
             "333" => stream3
           } = Splitter.call(flow, splitter)

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
end
