defmodule Strom.CrashTest do
  use ExUnit.Case

  alias Strom.{Source, Source.ReadLines, Sink}
  alias Strom.{Transformer, Mixer, Splitter}

  import ExUnit.CaptureLog

  setup do
    source =
      :stream
      |> Source.new(ReadLines.new("test/data/numbers1.txt"), chunk: 1, buffer: 1)
      |> Source.start()

    %{source: source}
  end

  def crash_fun(el) do
    if Enum.member?(["3"], el) do
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

        assert Enum.to_list(stream) == ["1", "2", "4", "5"]
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

        results = Enum.to_list(stream)
        assert results == ["1", "2", "5"]
      end)
    end
  end

  describe "crash in mixer" do
    setup do
      source2 =
        :stream2
        |> Source.new(ReadLines.new("test/data/numbers2.txt"), buffer: 1)
        |> Source.start()

      %{source2: source2}
    end

    test "crash in mixer", %{source: source, source2: source2} do
      partitions = %{
        stream: fn el -> if Enum.member?(["4"], el), do: raise("error"), else: el end,
        stream2: fn el -> if Enum.member?(["10"], el), do: raise("error"), else: el end
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

        assert results == ["1", "2", "20", "3", "30", "40", "5", "50"]
      end)
    end
  end

  describe "crash in splitter" do
    test "crash in splitter", %{source: source} do
      partitions = %{
        s1: fn el -> if Enum.member?(["1"], el), do: raise("error"), else: true end,
        s2: fn el -> if Enum.member?(["4"], el), do: raise("error"), else: true end
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

        s1res = Enum.to_list(s1)
        s2res = Enum.to_list(s2)

        assert s1res == ["2", "3", "5"]
        assert s2res == ["2", "3", "5"]
      end)
    end
  end

  describe "crash in source" do
    defmodule CustomReadLines do
      @behaviour Strom.Source

      defstruct path: nil, file: nil, infinite: false

      def new(path) when is_binary(path), do: %__MODULE__{path: path}

      @impl true
      def start(%__MODULE__{} = read_lines), do: %{read_lines | file: File.open!(read_lines.path)}

      @impl true
      def call(%__MODULE__{} = read_lines) do
        case read_line(read_lines.file) do
          {:ok, data} ->
            if String.trim(data) == "4" do
              raise "error"
            else
              {[String.trim(data)], read_lines}
            end

          {:error, :eof} ->
            {:halt, read_lines}
        end
      end

      @impl true
      def stop(%__MODULE__{} = read_lines), do: %{read_lines | file: File.close(read_lines.file)}

      @impl true
      def infinite?(%__MODULE__{infinite: infinite}), do: infinite

      defp read_line(file) do
        case IO.read(file, :line) do
          data when is_binary(data) ->
            {:ok, data}

          :eof ->
            {:error, :eof}

          {:error, :terminated} ->
            {:error, :eof}

          {:error, reason} ->
            raise reason
        end
      end
    end

    setup do
      source =
        :stream
        |> Source.new(CustomReadLines.new("test/data/numbers1.txt"), buffer: 1)
        |> Source.start()

      %{source: source}
    end

    test "crash source", %{source: source} do
      capture_log(fn ->
        %{stream: stream} = Source.call(%{}, source)
        assert Enum.to_list(stream) == ["1", "2", "3", "5"]
      end)
    end
  end

  describe "crash in sink" do
    defmodule CustomWriteLines do
      @behaviour Strom.Sink

      @line_sep "\n"

      defstruct path: nil, file: nil, line_sep: @line_sep

      def new(path, line_sep \\ @line_sep) when is_binary(path) and is_binary(line_sep) do
        %__MODULE__{path: path, line_sep: line_sep}
      end

      @impl true
      def start(%__MODULE__{} = write_lines) do
        file = File.open!(write_lines.path, [:write])
        %{write_lines | file: file}
      end

      @impl true
      def call(%__MODULE__{} = write_lines, data) do
        if data == "2" do
          raise "error"
        else
          :ok = IO.write(write_lines.file, data <> write_lines.line_sep)
        end

        write_lines
      end

      @impl true
      def stop(%__MODULE__{} = write_lines) do
        %{write_lines | file: File.close(write_lines.file)}
      end
    end

    setup do
      sink =
        :stream
        |> Sink.new(CustomWriteLines.new("test/data/output.csv"))
        |> Sink.start()

      %{sink: sink}
    end

    test "crash in sink", %{source: source, sink: sink} do
      capture_log(fn ->
        %{}
        |> Source.call(source)
        |> Sink.call(sink)

        Process.sleep(50)
        Sink.stop(sink)

        assert File.read!("test/data/output.csv") == "1\n3\n4\n5\n"
      end)
    end
  end
end
