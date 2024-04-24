defmodule Strom.CrashTest do
  use ExUnit.Case, async: true

  alias Strom.{Source, Source.ReadLines, Transformer, Mixer, Splitter}

  import ExUnit.CaptureLog

  setup do
    source =
      :stream
      |> Source.new(ReadLines.new("test/data/numbers1.txt"))
      |> Source.start()

    %{source: source}
  end

  def crash_fun(el) do
    if Enum.member?(["1", "4"], el) do
      raise "error"
    else
      el
    end
  end

  describe "crash in transformer" do
    test "crash when chunk is 1", %{source: source} do
      transformer =
        :stream
        |> Transformer.new(&crash_fun/1, nil, chunk: 1)
        |> Transformer.start()

      capture_log(fn ->
        %{stream: stream} =
          %{}
          |> Source.call(source)
          |> Transformer.call(transformer)

        assert Enum.to_list(stream) == ["2", "3", "5"]
      end)
    end

    test "crash when chunk is 2", %{source: source} do
      transformer =
        :stream
        |> Transformer.new(&crash_fun/1, nil, chunk: 2)
        |> Transformer.start()

      capture_log(fn ->
        %{stream: stream} =
          %{}
          |> Source.call(source)
          |> Transformer.call(transformer)

        assert Enum.to_list(stream) == ["5"]
      end)
    end
  end

  describe "crash in mixer" do
    setup do
      source2 =
        :stream2
        |> Source.new(ReadLines.new("test/data/numbers2.txt"))
        |> Source.start()

      %{source2: source2}
    end

    test "crash in mixer", %{source: source, source2: source2} do
      partitions = %{
        stream: fn el -> if Enum.member?(["1", "4"], el), do: raise("error"), else: el end,
        stream2: fn el -> if Enum.member?(["20", "50"], el), do: raise("error"), else: el end
      }

      mixer =
        partitions
        |> Mixer.new(:mixed, chunk: 1)
        |> Mixer.start()

      capture_log(fn ->
        %{mixed: mixed} =
          %{}
          |> Source.call(source)
          |> Source.call(source2)
          |> Mixer.call(mixer)

        results =
          mixed
          |> Enum.to_list()
          |> Enum.sort()

        assert results == ["10", "2", "3", "30", "40", "5"]
      end)
    end
  end

  describe "crash in splitter" do
    test "crash in splitter", %{source: source} do
      partitions = %{
        s1: fn el -> if Enum.member?(["2", "4"], el), do: raise("error"), else: true end,
        s2: fn el -> if Enum.member?(["2", "5"], el), do: raise("error"), else: true end
      }

      splitter =
        :stream
        |> Splitter.new(partitions, chunk: 1)
        |> Splitter.start()

      capture_log(fn ->
        %{s1: s1, s2: s2} =
          %{}
          |> Source.call(source)
          |> Splitter.call(splitter)

        assert Enum.to_list(s1) == ["1", "3"]
        assert Enum.to_list(s2) == ["1", "3"]
      end)
    end
  end
end
