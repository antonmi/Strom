defmodule Strom.Examples.SimpleNumbersTest do
  use ExUnit.Case

  alias Strom.{Mixer, Splitter, Function}

  test "simple numbers" do
    flow = %{numbers1: [1, 2, 3, 4, 5], numbers2: [6, 7, 8, 9, 10]}

    mixer = Mixer.start()
    splitter = Splitter.start()

    partitions = %{
      odd: fn el -> rem(el, 2) == 1 end,
      even: fn el -> rem(el, 2) == 0 end
    }

    function = Function.start(&(&1 + 1))

    %{odd: odd, even: even} =
      flow
      |> Mixer.call(mixer, [:numbers1, :numbers2], :number)
      |> Function.call(function, :number)
      |> Splitter.call(splitter, :number, partitions)

    assert Enum.sort(Enum.to_list(odd)) == [3, 5, 7, 9, 11]
    assert Enum.sort(Enum.to_list(even)) == [2, 4, 6, 8, 10]
  end
end
