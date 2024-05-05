defmodule Strom.Examples.WindowTest do
  use ExUnit.Case

  alias Strom.{Composite, Mixer, Transformer, Source, Renamer}

  describe "batch window" do
    def build_batch_avg_composite(size) do
      batch = fn el, acc ->
        if length([el | acc]) == size do
          {[Enum.reverse([el | acc])], []}
        else
          {[], [el | acc]}
        end
      end

      batch_transformer = Transformer.new(:stream, batch, [])

      avg = fn el ->
        Enum.sum(el) / length(el)
      end

      avg_transformer = Transformer.new(:stream, avg, nil)

      Composite.new([batch_transformer, avg_transformer])
    end

    test "average of batches 5" do
      composite = Composite.start(build_batch_avg_composite(5))
      %{stream: stream} = Composite.call(%{stream: 1..20}, composite)
      assert Enum.to_list(stream) == [3.0, 8.0, 13.0, 18.0]
      Composite.stop(composite)
    end
  end

  describe "sliding window" do
    def build_batch_avg_composite(size, overlap) do
      batch = fn el, acc ->
        all = [el | acc]

        if length(all) == size do
          {[Enum.reverse(all)], Enum.slice(all, 0, size - overlap)}
        else
          {[], [el | acc]}
        end
      end

      batch_transformer = Transformer.new(:stream, batch, [])

      avg = fn el ->
        Enum.sum(el) / length(el)
      end

      avg_transformer = Transformer.new(:stream, avg, nil)

      Composite.new([batch_transformer, avg_transformer])
    end

    test "average of batches 5" do
      composite = Composite.start(build_batch_avg_composite(5, 3))
      %{stream: stream} = Composite.call(%{stream: 1..20}, composite)
      assert Enum.to_list(stream) == [3.0, 6.0, 9.0, 12.0, 15.0, 18.0]
      Composite.stop(composite)
    end
  end

  describe "time window" do
    def build_window_avg_composite(milliseconds) do
      stream =
        Stream.resource(
          fn -> nil end,
          fn nil ->
            Process.sleep(milliseconds)
            {[:tick], nil}
          end,
          fn nil -> nil end
        )

      source = Source.new(:ticks, stream)
      mixer = Mixer.new([:numbers, :ticks], :numbers_with_ticks, no_wait: true)

      batch = fn el, acc ->
        case el do
          :tick ->
            {[Enum.reverse(acc)], []}

          number ->
            {[], [number | acc]}
        end
      end

      batch_transformer = Transformer.new(:numbers_with_ticks, batch, [])

      avg = fn el ->
        if length(el) > 0 do
          Enum.sum(el) / length(el)
        else
          0
        end
      end

      avg_transformer = Transformer.new(:numbers_with_ticks, avg, nil)

      renamer = Renamer.new(%{numbers_with_ticks: :numbers})

      [source, mixer, batch_transformer, avg_transformer, renamer]
      |> Composite.new()
      |> Composite.start()
    end

    test "average of batches 5" do
      composite = build_window_avg_composite(50)

      numbers =
        Stream.resource(
          fn -> 1 end,
          fn count ->
            if count < 20 do
              Process.sleep(10)
              {[7], count + 1}
            else
              {:halt, :done}
            end
          end,
          fn :done -> :done end
        )

      %{numbers: numbers} = Composite.call(%{numbers: numbers}, composite)

      assert Enum.to_list(numbers) == [7.0, 7.0, 7.0, 7.0]
    end
  end
end
