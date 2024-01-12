defmodule Strom.RenamerTest do
  use ExUnit.Case, async: true

  alias Strom.Renamer

  test "start and stop" do
    assert Renamer.start() == %Renamer{}
    assert :ok = Renamer.stop(%Renamer{})
  end

  test "rename" do
    names = %{s1: :foo1, s2: :foo2}
    flow = %{s1: [1], s2: [2], s3: [3]}

    new_flow = Renamer.call(flow, names)

    refute new_flow[:s1]
    refute new_flow[:s2]

    assert Enum.to_list(new_flow[:foo1]) == [1]
    assert Enum.to_list(new_flow[:foo2]) == [2]
    assert Enum.to_list(new_flow[:s3]) == [3]
  end

  test "raise when there is no such name" do
    assert_raise KeyError, fn ->
      Renamer.call(%{s1: [1]}, %{s2: :foo2})
    end
  end
end
