defmodule Strom.DuplexMixTest do
  use ExUnit.Case, async: true
  alias Strom.DuplexMix

  @moduletag timeout: 5000

  def build_list_stream(list, sleep \\ 0) do
    Stream.resource(
      fn -> list end,
      fn
        [] ->
          {:halt, []}

        [hd | tl] ->
          Process.sleep(sleep)
          {[hd], tl}
      end,
      fn list -> list end
    )
  end

  test "start and stop" do
    duplex_mix = DuplexMix.start(%DuplexMix{})
    assert Process.alive?(duplex_mix.pid)
    :ok = DuplexMix.stop(duplex_mix)
    refute Process.alive?(duplex_mix.pid)
  end

  describe "call" do
    test "with input from left" do
      flow = %{numbers: [1, 2, 3, 4, 5]}

      outputs = %{
        odd: fn el ->
          {rem(el, 2) == 1, el == 3}
        end,
        even: fn el ->
          {rem(el, 2) == 0, el == 4}
        end
      }

      duplex = %DuplexMix{inputs: {[:numbers], []}, outputs: {outputs, %{}}}
      duplex_mix = DuplexMix.start(duplex)

      {left_flow, right_flow} = DuplexMix.call({flow, %{}}, duplex_mix)

      assert Enum.sort(Enum.to_list(left_flow[:odd])) == [1, 3, 5]
      assert Enum.sort(Enum.to_list(left_flow[:even])) == [2, 4]
      assert Enum.sort(Enum.to_list(right_flow[:numbers])) == [3, 4]

      :ok = DuplexMix.stop(duplex_mix)
    end

    test "with input from right" do
      flow = %{numbers: [1, 2, 3, 4, 5]}

      outputs = %{
        odd: fn el ->
          {rem(el, 2) == 1, el == 3}
        end,
        even: fn el ->
          {rem(el, 2) == 0, el == 4}
        end
      }

      duplex = %DuplexMix{inputs: {[], [:numbers]}, outputs: {%{}, outputs}}
      duplex_mix = DuplexMix.start(duplex)

      {left_flow, right_flow} = DuplexMix.call({%{}, flow}, duplex_mix)

      assert Enum.sort(Enum.to_list(right_flow[:odd])) == [1, 3, 5]
      assert Enum.sort(Enum.to_list(right_flow[:even])) == [2, 4]
      assert Enum.sort(Enum.to_list(left_flow[:numbers])) == [3, 4]

      :ok = DuplexMix.stop(duplex_mix)
    end

    test "with both flows" do
      left_flow = %{nums1: [1, 2, 3, 4, 5]}
      right_flow = %{nums2: [1, -1, 1, -1, 1]}

      outputs_left = %{
        odd: fn el ->
          {rem(el, 2) == 1, true}
        end,
        even: fn el ->
          {rem(el, 2) == 0, false}
        end
      }

      outputs_right = %{
        positive: fn el ->
          {el > 0, true}
        end,
        negative: fn el ->
          {el < 0, false}
        end
      }

      duplex = %DuplexMix{inputs: {[:nums1], [:nums2]}, outputs: {outputs_left, outputs_right}}
      duplex_mix = DuplexMix.start(duplex)

      {left_flow, right_flow} = DuplexMix.call({left_flow, right_flow}, duplex_mix)

      assert Enum.sort(Enum.to_list(left_flow[:odd])) == [1, 3, 5]
      assert Enum.sort(Enum.to_list(left_flow[:even])) == [2, 4]
      assert Enum.sort(Enum.to_list(right_flow[:nums1])) == [1, 2, 3, 4, 5]

      assert Enum.sort(Enum.to_list(right_flow[:positive])) == [1, 1, 1]
      assert Enum.sort(Enum.to_list(right_flow[:negative])) == [-1, -1]
      assert Enum.sort(Enum.to_list(left_flow[:nums2])) == [-1, -1, 1, 1, 1]

      :ok = DuplexMix.stop(duplex_mix)
    end
  end
end
