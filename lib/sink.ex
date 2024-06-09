defmodule Strom.Sink do
  @moduledoc """
  Runs a given steam and `call` origin on each even in stream.
  By default it runs the stream asynchronously (in `Task.async`).
  One can pass `true` a the third argument to the `Sink.new/3` function to run a stream synchronously.

      ## Example
      iex> alias Strom.{Sink, Sink.WriteLines}
      iex> sink = :strings |> Sink.new(WriteLines.new("test/data/sink.txt"), true) |> Sink.start()
      iex> %{} = Sink.call(%{strings: ["a", "b", "c"]}, sink)
      iex> File.read!("test/data/sink.txt")
      "a\\nb\\nc\\n"

  Sink defines a `@behaviour`. One can easily implement their own sinks.
  See `Strom.Sink.Writeline`, `Strom.Sink.IOPuts`, `Strom.Sink.Null`
  """
  @callback start(map) :: map
  @callback call(map, term) :: {:ok, {term, map}} | {:error, {term, map}}
  @callback stop(map) :: map

  use GenServer

  defstruct origin: nil,
            name: nil,
            sync: false,
            pid: nil,
            stream: nil,
            task: nil

  @type t() :: %__MODULE__{}
  @type event() :: any()

  @spec new(Strom.stream_name(), struct(), boolean()) :: __MODULE__.t()
  def new(name, origin, sync \\ false) when is_struct(origin) do
    %__MODULE__{origin: origin, name: name, sync: sync}
  end

  @spec start(__MODULE__.t()) :: __MODULE__.t()
  def start(%__MODULE__{origin: origin} = sink) when is_struct(origin) do
    origin = apply(origin.__struct__, :start, [origin])
    sink = %{sink | origin: origin}

    {:ok, pid} =
      DynamicSupervisor.start_child(
        {:via, PartitionSupervisor, {Strom.ComponentSupervisor, sink}},
        %{id: __MODULE__, start: {__MODULE__, :start_link, [sink]}, restart: :temporary}
      )

    :sys.get_state(pid)
  end

  def start_link(%__MODULE__{} = sink) do
    GenServer.start_link(__MODULE__, sink)
  end

  @impl true
  def init(%__MODULE__{} = sink), do: {:ok, %{sink | pid: self()}}

  @spec call(__MODULE__.t(), any()) :: event()
  def call(%__MODULE__{pid: pid}, data), do: GenServer.call(pid, {:call, data}, :infinity)

  @spec call(Strom.flow(), __MODULE__.t()) :: Strom.flow()
  def call(flow, %__MODULE__{name: name} = sink) when is_map(flow) do
    stream = Map.fetch!(flow, name)
    :ok = GenServer.call(sink.pid, {:run_stream, stream}, :infinity)

    Map.delete(flow, name)
  end

  defp async_run_sink(sink, stream) do
    Task.Supervisor.async_nolink(
      {:via, PartitionSupervisor, {Strom.TaskSupervisor, self()}},
      fn ->
        Stream.transform(stream, sink, fn el, sink ->
          call_sink(sink, el)
          {[], sink}
        end)
        |> Stream.run()

        :task_done
      end
    )
  end

  defp call_sink(%__MODULE__{origin: origin} = sink, data) do
    case apply(origin.__struct__, :call, [origin, data]) do
      {:ok, {[], origin}} ->
        {[], %{sink | origin: origin}}

      {:error, {:halt, origin}} ->
        {:halt, %{sink | origin: origin}}
    end
  end

  @spec stop(__MODULE__.t()) :: :ok
  def stop(%__MODULE__{pid: pid}), do: GenServer.call(pid, :stop)

  @impl true
  def handle_call({:run_stream, stream}, _from, %__MODULE__{} = sink) do
    task = async_run_sink(sink, stream)

    if sink.sync do
      Task.await(task, :infinity)
    end

    {:reply, :ok, %{sink | task: task, stream: stream}}
  end

  def handle_call({:call, data}, _from, %__MODULE__{} = sink) do
    {:reply, call_sink(sink, data), sink}
  end

  def handle_call(:stop, _from, %__MODULE__{origin: origin, task: task} = sink) do
    origin = apply(origin.__struct__, :stop, [origin])

    if task do
      DynamicSupervisor.terminate_child(Strom.TaskSupervisor, task.pid)
    end

    sink = %{sink | origin: origin}
    {:stop, :normal, :ok, sink}
  end

  @impl true
  def handle_info({_task_ref, :task_done}, sink) do
    # do nothing for now
    {:noreply, %{sink | task: nil}}
  end

  def handle_info({:DOWN, _task_ref, :process, _task_pid, :normal}, sink) do
    # do nothing for now
    {:noreply, sink}
  end

  def handle_info({:DOWN, _task_ref, :process, _task_pid, _not_normal}, sink) do
    task = async_run_sink(sink, sink.stream)

    {:noreply, %{sink | task: task}}
  end
end
