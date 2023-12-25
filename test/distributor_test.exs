defmodule Strom.DistributorTest do
  use ExUnit.Case, async: false

  alias Strom.Distributor

  #  test "start and stop" do
  #    mixer = Mixer.start()
  #    assert Process.alive?(mixer.pid)
  #    :ok = Mixer.stop(mixer)
  #    refute Process.alive?(mixer.pid)
  #  end

  test "call" do
    flow = %{numbers1: [1, 2, 3, 4, 5], numbers2: [6, 7, 8, 9, 10]}

    distributor = Distributor.start()

    inputs = %{
      numbers1: fn el -> el < 5 end,
      numbers2: fn el -> el > 6 end
    }

    outputs = %{
      odd: fn el -> rem(el, 2) == 1 end,
      even: fn el -> rem(el, 2) == 0 end
    }

    flow = Distributor.call(flow, distributor, inputs, outputs)

    assert Enum.sort(Enum.to_list(flow[:odd])) == [1,3,7,9]
    assert Enum.sort(Enum.to_list(flow[:even])) == [2,4,8,10]
  end

  test "massive call" do
    :observer.start()
    flow = %{numbers1: Enum.to_list(1..100_000), numbers2: Enum.to_list(200_000..300_000)}

    distributor = Distributor.start()

    inputs = %{
      numbers1: fn el -> rem(el, 3) == 0 end,
      numbers2: fn el -> rem(el, 5) == 0 end
    }

    outputs = %{
      odd: fn el -> rem(el, 2) == 1 end,
      even: fn el -> rem(el, 2) == 0 end
    }

    flow = Distributor.call(flow, distributor, inputs, outputs)

    Enum.to_list(flow[:odd])
    |> IO.inspect
    Enum.to_list(flow[:even])
    |> IO.inspect
  end
end
