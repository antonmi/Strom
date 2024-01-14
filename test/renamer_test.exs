defmodule Strom.RenamerTest do
  use ExUnit.Case, async: true
  doctest Strom.Renamer

  alias Strom.Renamer

  test "start and stop" do
    renamer =
      %{a: :b}
      |> Renamer.new()
      |> Renamer.start()

    assert renamer == %Renamer{names: %{a: :b}}
    assert :ok = Renamer.stop(%Renamer{})
  end

  test "rename" do
    renamer =
      %{s1: :foo1, s2: :foo2}
      |> Renamer.new()
      |> Renamer.start()

    flow = %{s1: [1], s2: [2], s3: [3]}

    new_flow = Renamer.call(flow, renamer)

    refute new_flow[:s1]
    refute new_flow[:s2]

    assert Enum.to_list(new_flow[:foo1]) == [1]
    assert Enum.to_list(new_flow[:foo2]) == [2]
    assert Enum.to_list(new_flow[:s3]) == [3]
  end

  test "raise when there is no such name" do
    renamer =
      %{s2: :foo2}
      |> Renamer.new()
      |> Renamer.start()

    assert_raise KeyError, fn ->
      Renamer.call(%{s1: [1]}, renamer)
    end
  end
end
