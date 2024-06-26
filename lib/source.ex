defmodule Strom.Source do
  @moduledoc """
  Produces stream of events.

      ## Example with Enumerable
      iex> alias Strom.Source
      iex> source = :numbers |> Source.new([1, 2, 3]) |> Source.start()
      iex> %{numbers: stream} = Source.call(%{}, source)
      iex> Enum.to_list(stream)
      [1, 2, 3]

      ## Example with file
      iex> alias Strom.{Source, Source.ReadLines}
      iex> source = :numbers |> Source.new(ReadLines.new("test/data/numbers1.txt")) |> Source.start()
      iex> %{numbers: stream} = Source.call(%{}, source)
      iex> Enum.to_list(stream)
      ["1", "2", "3", "4", "5"]

      ## If two sources are applied to one stream, the streams will be concatenated (Stream.concat/2)
      iex> alias Strom.{Source, Source.ReadLines}
      iex> source1 = :numbers |> Source.new([1, 2, 3]) |> Source.start()
      iex> source2 = :numbers |> Source.new(ReadLines.new("test/data/numbers1.txt")) |> Source.start()
      iex> %{numbers: stream} = %{} |> Source.call(source1) |> Source.call(source2)
      iex> Enum.to_list(stream)
      [1, 2, 3, "1", "2", "3", "4", "5"]

  Source defines a `@behaviour`. One can easily implement their own sources.
  See `Strom.Source.ReadLines`, `Strom.Source.Events`, `Strom.Source.IOGets`
  """

  @callback start(map) :: map
  @callback call(map) :: {[term], map} | {:halt, map} | no_return()
  @callback stop(map) :: map
  @callback infinite?(map) :: true | false

  use GenServer

  @buffer 1000
  @chunk 1

  defstruct origin: nil,
            name: nil,
            pid: nil,
            data: [],
            opts: [],
            task: nil,
            waiting_client: nil,
            chunk: @chunk,
            buffer: @buffer

  @type t() :: %__MODULE__{}
  @type event() :: any()

  @spec new(Strom.stream_name(), struct() | [event()] | Strom.stream(), list()) :: __MODULE__.t()
  def new(name, origin, opts \\ [])

  def new(name, origin, opts) when is_list(origin) and is_list(opts) do
    %__MODULE__{origin: Stream.concat([], origin), name: name, opts: opts}
  end

  def new(name, origin, opts)
      when (is_struct(origin) or is_function(origin, 2)) and is_list(opts) do
    %__MODULE__{origin: origin, name: name, opts: opts}
  end

  def start(%__MODULE__{origin: origin, opts: opts} = source) when is_struct(origin) do
    origin = apply(origin.__struct__, :start, [origin])

    source = %{
      source
      | origin: origin,
        buffer: Keyword.get(opts, :buffer, @buffer),
        chunk: Keyword.get(opts, :chunk, @chunk)
    }

    {:ok, pid} = start_child(source)
    :sys.get_state(pid)
  end

  def start(%__MODULE__{origin: origin, opts: opts} = source) when is_function(origin, 2) do
    source = %{
      source
      | origin: origin,
        buffer: Keyword.get(opts, :buffer, @buffer),
        chunk: Keyword.get(opts, :chunk, @chunk)
    }

    {:ok, pid} = start_child(source)
    :sys.get_state(pid)
  end

  def start_child(source) do
    DynamicSupervisor.start_child(
      {:via, PartitionSupervisor, {Strom.ComponentSupervisor, source}},
      %{id: __MODULE__, start: {__MODULE__, :start_link, [source]}, restart: :temporary}
    )
  end

  def start_link(%__MODULE__{} = source) do
    GenServer.start_link(__MODULE__, source)
  end

  @impl true
  def init(%__MODULE__{} = source), do: {:ok, %{source | pid: self()}}

  @spec call(__MODULE__.t()) :: event()
  def call(%__MODULE__{pid: pid}), do: GenServer.call(pid, :call, :infinity)

  def infinite?(%__MODULE__{pid: pid}), do: GenServer.call(pid, :infinite)

  @spec call(Strom.flow(), __MODULE__.t()) :: Strom.flow()
  def call(flow, %__MODULE__{name: name, origin: origin} = source)
      when is_map(flow) and is_struct(origin) do
    :ok = GenServer.call(source.pid, :run_input)

    stream =
      Stream.resource(
        fn ->
          nil
        end,
        fn nil ->
          case GenServer.call(source.pid, :get_data, :infinity) do
            {:data, data} ->
              {data, nil}

            :done ->
              {:halt, nil}

            :pause ->
              receive do
                :continue_client ->
                  {[], nil}
              end
          end
        end,
        fn nil -> nil end
      )

    prev_stream = Map.get(flow, name, [])
    Map.put(flow, name, Stream.concat(prev_stream, stream))
  end

  @spec call(Strom.flow(), __MODULE__.t()) :: Strom.flow()
  def call(flow, %__MODULE__{name: name, origin: stream})
      when is_map(flow) and is_function(stream, 2) do
    prev_stream = Map.get(flow, name, [])
    Map.put(flow, name, Stream.concat(prev_stream, stream))
  end

  @spec stop(__MODULE__.t()) :: :ok
  def stop(%__MODULE__{pid: pid}), do: GenServer.call(pid, :stop)

  defp async_run_input(source) do
    Task.Supervisor.async_nolink(
      {:via, PartitionSupervisor, {Strom.TaskSupervisor, self()}},
      fn ->
        loop_call(source)
      end
    )
  end

  defp loop_call(%__MODULE__{origin: origin} = source) do
    case apply(origin.__struct__, :call, [origin]) do
      {:halt, _origin} ->
        :task_done

      {events, origin} ->
        GenServer.cast(source.pid, {:new_data, events})

        receive do
          :continue_task ->
            flush(:continue_task)
        end

        loop_call(%{source | origin: origin})
    end
  end

  @impl true
  def handle_call(:run_input, _from, %__MODULE__{} = source) do
    task = async_run_input(source)

    {:reply, :ok, %{source | task: task}}
  end

  @impl true
  def handle_call(:call, _from, %__MODULE__{origin: origin} = source) do
    {events, origin} = apply(origin.__struct__, :call, [origin])
    source = %{source | origin: origin}
    {:reply, {events, source}, source}
  end

  def handle_call(:stop, _from, %__MODULE__{origin: origin, task: task} = source)
      when is_struct(origin) do
    origin = apply(origin.__struct__, :stop, [origin])

    if task do
      DynamicSupervisor.terminate_child(Strom.TaskSupervisor, task.pid)
    end

    {:stop, :normal, :ok, %{source | origin: origin}}
  end

  def handle_call(:stop, _from, %__MODULE__{origin: origin, task: task} = source)
      when is_function(origin, 2) do
    if task do
      DynamicSupervisor.terminate_child(Strom.TaskSupervisor, task.pid)
    end

    {:stop, :normal, :ok, source}
  end

  def handle_call(:get_data, {pid, _ref}, source) do
    if source.task do
      send(source.task.pid, :continue_task)
    end

    {data, rest} = Enum.split(source.data, source.chunk)

    cond do
      length(data) == 0 and is_nil(source.task) ->
        {:reply, :done, source}

      length(data) == 0 ->
        {:reply, :pause, %{source | waiting_client: pid}}

      true ->
        {:reply, {:data, data}, %{source | data: rest}}
    end
  end

  @impl true
  def handle_cast({:new_data, chunk}, %__MODULE__{} = source) do
    all_data = source.data ++ chunk

    if source.waiting_client do
      send(source.waiting_client, :continue_client)
    end

    if length(all_data) < source.buffer do
      send(source.task.pid, :continue_task)
    end

    {:noreply, %{source | data: all_data, waiting_client: nil}}
  end

  @impl true
  def handle_info({_task_ref, :task_done}, source) do
    if source.waiting_client do
      send(source.waiting_client, :continue_client)
    end

    {:noreply, %{source | task: nil, waiting_client: nil}}
  end

  def handle_info({:DOWN, _task_ref, :process, _task_pid, :normal}, source) do
    # do nothing for now
    {:noreply, source}
  end

  def handle_info({:DOWN, _task_ref, :process, _task_pid, _not_normal}, source) do
    task = async_run_input(source)

    {:noreply, %{source | task: task}}
  end

  defp flush(message) do
    receive do
      ^message ->
        flush(message)
    after
      0 -> :ok
    end
  end
end
