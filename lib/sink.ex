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
            pid: nil

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

    {:ok, pid} = start_link(sink)
    __state__(pid)
  end

  def start_link(%__MODULE__{} = sink) do
    GenServer.start_link(__MODULE__, sink)
  end

  @impl true
  def init(%__MODULE__{} = sink), do: {:ok, %{sink | pid: self()}}

  @spec call(__MODULE__.t(), any()) :: event()
  def call(%__MODULE__{pid: pid}, data), do: GenServer.call(pid, {:call, data})

  @spec call(Strom.flow(), __MODULE__.t()) :: Strom.flow()
  def call(flow, %__MODULE__{name: name, sync: sync} = sink) when is_map(flow) do
    stream = Map.fetch!(flow, name)

    stream =
      Stream.transform(stream, sink, fn el, sink ->
        call(sink, el)
        {[], sink}
      end)

    if sync do
      Stream.run(stream)
    else
      Task.async(fn -> Stream.run(stream) end)
    end

    Map.delete(flow, name)
  end

  @spec stop(__MODULE__.t()) :: :ok
  def stop(%__MODULE__{origin: origin, pid: pid}) do
    apply(origin.__struct__, :stop, [origin])
    GenServer.call(pid, :stop)
  end

  def __state__(pid) when is_pid(pid), do: GenServer.call(pid, :__state__)

  @impl true
  def handle_call({:call, data}, _from, %__MODULE__{origin: origin} = sink) do
    {[], sink} =
      case apply(origin.__struct__, :call, [origin, data]) do
        {:ok, {[], origin}} ->
          {[], %{sink | origin: origin}}

        {:error, {:halt, origin}} ->
          {:halt, %{sink | origin: origin}}
      end

    {:reply, {[], sink}, sink}
  end

  def handle_call(:stop, _from, %__MODULE__{origin: origin} = sink) do
    origin = apply(origin.__struct__, :stop, [origin])
    sink = %{sink | origin: origin}
    {:stop, :normal, :ok, sink}
  end

  def handle_call(:__state__, _from, sink), do: {:reply, sink, sink}
end
