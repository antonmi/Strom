defmodule Strom.RenamerTest do
  use ExUnit.Case, async: true

  alias Strom.Renamer

  test "start and stop" do
    rename = Renamer.start(%{s1: :s2})
    assert Process.alive?(rename.pid)
    :ok = Renamer.stop(rename)
    refute Process.alive?(rename.pid)
  end

  test "rename" do
    names = %{s1: :foo1, s2: :foo2}
    rename = Renamer.start(names)

    flow = %{s1: [1], s2: [2], s3: [3]}

    new_flow = Renamer.call(flow, rename, names)

    refute new_flow[:s1]
    refute new_flow[:s2]

    assert Enum.to_list(new_flow[:foo1]) == [1]
    assert Enum.to_list(new_flow[:foo2]) == [2]
    assert Enum.to_list(new_flow[:s3]) == [3]
  end

  test "raise when there is no such name" do
    names = %{s2: :foo2}
    rename = Renamer.start(names)
    flow = %{s1: [1]}

    assert_raise KeyError, fn ->
      Renamer.call(flow, rename, names)
    end
  end
end
