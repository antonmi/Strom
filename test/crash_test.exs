defmodule Strom.CrashTest do
  use ExUnit.Case, async: false

  alias Strom.{Source, Source.ReadLines, Sink}
  alias Strom.{Transformer, Splitter}
  alias Strom.Composite

  import ExUnit.CaptureLog

  def crash_fun(el) do
    if Enum.member?([3], el) do
      raise "error"
    else
      el * 2
    end
  end

  def crash_fun_with_acc(el, acc) do
    if Enum.member?([3], el) do
      raise "error"
    else
      {[el + acc], el + acc}
    end
  end

  def build_stream(list, sleep \\ 0) do
    {:ok, agent} = Agent.start_link(fn -> list end)

    Stream.resource(
      fn -> agent end,
      fn agent ->
        Process.sleep(sleep)

        Agent.get_and_update(agent, fn
          [] -> {{:halt, agent}, []}
          [datum | data] -> {{[datum], agent}, data}
        end)
      end,
      fn agent -> agent end
    )
  end

  describe "crash in transformer" do
    setup do
      %{stream: build_stream([1, 2, 3, 4, 5])}
    end

    test "crash when chunk is 1", %{stream: stream} do
      transformer =
        :stream
        |> Transformer.new(&crash_fun/1, nil, chunk: 1)
        |> Transformer.start()

      capture_log(fn ->
        %{stream: stream} = Transformer.call(%{stream: stream}, transformer)

        assert Enum.to_list(stream) == [2, 4, 8, 10]
      end)
    end

    test "crash when chunk is 2", %{stream: stream} do
      transformer =
        :stream
        |> Transformer.new(&crash_fun/1, nil, chunk: 2)
        |> Transformer.start()

      capture_log(fn ->
        %{stream: stream} = Transformer.call(%{stream: stream}, transformer)

        assert Enum.to_list(stream) == [2, 4, 10]
      end)
    end

    test "two sequentioal crashes" do
      stream = build_stream([1, 2, 3, 4, 5, 3, 4, 5])

      transformer =
        :stream
        |> Transformer.new(&crash_fun/1, nil, chunk: 1)
        |> Transformer.start()

      capture_log(fn ->
        %{stream: stream} = Transformer.call(%{stream: stream}, transformer)

        assert Enum.to_list(stream) == [2, 4, 8, 10, 8, 10]
      end)
    end

    test "crush when transformer process 2 steams", %{stream: stream} do
      stream2 = build_stream([10, 20, 30, 40, 50])

      transformer =
        [:stream, :stream2]
        |> Transformer.new(&crash_fun/1, nil, chunk: 1)
        |> Transformer.start()

      capture_log(fn ->
        %{stream: stream, stream2: stream2} =
          Transformer.call(%{stream: stream, stream2: stream2}, transformer)

        assert Enum.to_list(stream) == [2, 4, 8, 10]
        assert Enum.to_list(stream2) == [20, 40, 60, 80, 100]
      end)
    end

    test "crush when transformer process 2 steams with accumulators", %{stream: stream} do
      stream2 = build_stream([10, 20, 30, 40, 50])

      transformer =
        [:stream, :stream2]
        |> Transformer.new(&crash_fun_with_acc/2, 0, chunk: 1)
        |> Transformer.start()

      capture_log(fn ->
        %{stream: stream, stream2: stream2} =
          Transformer.call(%{stream: stream, stream2: stream2}, transformer)

        assert Enum.to_list(stream) == [1, 3, 7, 12]
        assert Enum.to_list(stream2) == [10, 30, 60, 100, 150]
      end)
    end
  end

  describe "kill transformer process" do
    setup do
      %{stream: build_stream(Enum.to_list(1..10), 1)}
    end

    test "task is alive", %{stream: stream} do
      transformer =
        :stream
        |> Transformer.new(& &1, nil, chunk: 1)
        |> Transformer.start()

      list_task =
        Task.async(fn ->
          %{stream: stream} = Transformer.call(%{stream: stream}, transformer)
          Enum.to_list(stream)
        end)

      Process.sleep(5)
      %{tasks: tasks} = :sys.get_state(transformer.pid)
      task_pid = hd(Map.keys(tasks))
      Process.exit(transformer.pid, :kill)

      try do
        Task.await(list_task, 20)
      catch
        :exit, {:timeout, {Task, :await, [%Task{pid: task_pid}, 20]}} ->
          assert task_pid == list_task.pid
      end

      assert Process.alive?(task_pid)
    end
  end

  describe "crash in component gm will crash composite" do
    test "crash composite" do
      stream = build_stream(Enum.to_list(1..10), 1)
      transformer = Transformer.new(:stream, & &1, nil, chunk: 1)

      composite =
        [transformer]
        |> Composite.new()
        |> Composite.start()

      task =
        Task.async(fn ->
          %{stream: stream} = Composite.call(%{stream: stream}, composite)
          Enum.to_list(stream)
        end)

      Process.sleep(5)
      [transformer] = Composite.components(composite)

      Process.unlink(composite.pid)
      Process.unlink(task.pid)

      capture_log(fn ->
        Process.exit(transformer.pid, :kill)

        Process.sleep(100)
        refute Process.alive?(composite.pid)
      end) =~ "(stop) {:component_crashed, %Strom.Transformer{pid:"
    end
  end

  describe "crash in splitter" do
    setup do
      %{stream: build_stream([1, 2, 3, 4, 5, 6], 1)}
    end

    test "crash in splitter, run in tasks", %{stream: stream} do
      partitions = %{
        s1: fn el -> if el == 1, do: raise("error"), else: true end,
        s2: fn el -> if el == 4, do: raise("error"), else: true end
      }

      splitter =
        :stream
        |> Splitter.new(partitions, chunk: 1)
        |> Splitter.start()

      capture_log(fn ->
        %{s1: s1, s2: s2} =
          %{stream: stream}
          |> Splitter.call(splitter)

        task1 = Task.async(fn -> Enum.to_list(s1) end)
        task2 = Task.async(fn -> Enum.to_list(s2) end)

        assert Task.await(task1) == [2, 3, 5, 6]
        assert Task.await(task2) == [2, 3, 5, 6]
      end)
    end

    test "crash in splitter, run one by one", %{stream: stream} do
      partitions = %{
        s1: fn el -> if el == 1, do: raise("error"), else: true end,
        s2: fn el -> if el == 4, do: raise("error"), else: true end
      }

      splitter =
        :stream
        |> Splitter.new(partitions, chunk: 1)
        |> Splitter.start()

      capture_log(fn ->
        %{s1: s1, s2: s2} =
          %{stream: stream}
          |> Splitter.call(splitter)

        assert Enum.to_list(s1) == [2, 3, 5, 6]
        assert Enum.to_list(s2) == [2, 3, 5, 6]
      end)
    end
  end

  describe "crash in source" do
    defmodule CustomReadLines do
      @behaviour Strom.Source

      defstruct path: nil, file: nil

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

      source =
        :stream
        |> Source.new(ReadLines.new("test/data/numbers1.txt"), chunk: 1, buffer: 1)
        |> Source.start()

      %{sink: sink, source: source}
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
