defmodule Strom.DistributorTest do
  use ExUnit.Case, async: false

  alias Strom.Distributor

  test "start and stop" do
    distributor = Distributor.start()
    assert Process.alive?(distributor.pid)
    :ok = Distributor.stop(distributor)
    refute Process.alive?(distributor.pid)
  end

  describe "call" do
    setup do
      flow = %{numbers1: [1, 2, 3, 4, 5], numbers2: [6, 7, 8, 9, 10]}

      inputs = %{
        numbers1: fn el -> el < 5 end,
        numbers2: fn el -> el > 6 end
      }

      outputs = %{
        odd: fn el -> rem(el, 2) == 1 end,
        even: fn el -> rem(el, 2) == 0 end
      }

      %{flow: flow, inputs: inputs, outputs: outputs}
    end

    test "call", %{flow: flow, inputs: inputs, outputs: outputs} do
      distributor = Distributor.start()
      flow = Distributor.call(flow, distributor, inputs, outputs)

      assert Enum.sort(Enum.to_list(flow[:odd])) == [1, 3, 7, 9]
      assert Enum.sort(Enum.to_list(flow[:even])) == [2, 4, 8, 10]
    end

    test "call with function", %{flow: flow, inputs: inputs, outputs: outputs} do
      function = {fn el, nil -> {[el * el], nil} end, nil}
      distributor = Distributor.start(function: function)

      flow = Distributor.call(flow, distributor, inputs, outputs)

      assert Enum.sort(Enum.to_list(flow[:odd])) == [1, 9, 49, 81]
      assert Enum.sort(Enum.to_list(flow[:even])) == [4, 16, 64, 100]
    end

    test "call with function/2 and accumulator", %{flow: flow, inputs: inputs, outputs: outputs} do
      function = fn el, acc ->
        {[el, acc], acc + 1}
      end

      distributor = Distributor.start(function: {function, 1000})

      flow = Distributor.call(flow, distributor, inputs, outputs)

      assert Enum.sort(Enum.to_list(flow[:odd])) == [1, 3, 7, 9, 1001, 1003, 1005, 1007]
      assert Enum.sort(Enum.to_list(flow[:even])) == [2, 4, 8, 10, 1000, 1002, 1004, 1006]
    end
  end

  test "massive call" do
    #    :observer.start()
    flow = %{
      numbers1: Enum.to_list(1..100_000),
      numbers2: Enum.to_list(1..100_000),
      numbers3: Enum.to_list(1..100_000)
    }

    distributor = Distributor.start()

    inputs = %{
      numbers1: fn el -> rem(el, 3) == 0 end,
      numbers2: fn el -> rem(el, 4) == 0 end,
      numbers3: fn el -> rem(el, 5) == 0 end
    }

    outputs = %{
      odd: fn el -> rem(el, 2) == 1 end,
      even: fn el -> rem(el, 2) == 0 end
    }

    flow = Distributor.call(flow, distributor, inputs, outputs)

    task1 =
      Task.async(fn ->
        list = Enum.to_list(flow[:odd])
        assert length(list) == 26667
      end)

    task2 =
      Task.async(fn ->
        list = Enum.to_list(flow[:even])
        assert length(list) == 51666
      end)

    Task.await(task1, :infinity)
    Task.await(task2, :infinity)
  end

  #  test "huge files" do
  #    :observer.start()
  #    source1 = Strom.Source.start(%Strom.Source.ReadLines{path: "test_data/orders.csv"})
  #    source2 = Strom.Source.start(%Strom.Source.ReadLines{path: "test_data/parcels.csv"})
  #
  #    sink1 = Strom.Sink.start(%Strom.Sink.WriteLines{path: "test_data/odd.csv"})
  #    sink2 = Strom.Sink.start(%Strom.Sink.WriteLines{path: "test_data/even.csv"})
  #
  #    flow =
  #      %{}
  #      |> Strom.Source.call(source1, :source1)
  #      |> Strom.Source.call(source2, :source2)
  #
  #    distributor = Distributor.start()
  #
  #    inputs = %{
  #      source1: fn el -> el end,
  #      source2: fn el -> el end
  #    }
  #
  #    outputs = %{
  #      odd: fn el -> String.contains?(el, "ORDER_CREATED") end,
  #      even: fn el -> String.contains?(el, "PARCEL_SHIPPED") end
  #    }
  #
  #    flow
  #    |> Distributor.call(distributor, inputs, outputs)
  #    |> Strom.Sink.call(sink1, [:odd])
  #    |> Strom.Sink.call(sink2, [:even], true)
  #  end
end
