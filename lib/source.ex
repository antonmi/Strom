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
  @callback call(map) :: {:ok, {[term], map}} | {:error, {:halt, map}}
  @callback stop(map) :: map
  @callback infinite?(map) :: true | false

  use GenServer

  defstruct origin: nil,
            name: nil,
            pid: nil

  @type t() :: %__MODULE__{}
  @type event() :: any()

  @spec new(Strom.stream_name(), struct() | [event()]) :: __MODULE__.t()
  def new(name, origin) when is_struct(origin) or is_list(origin) do
    %__MODULE__{origin: origin, name: name}
  end

  @spec start(__MODULE__.t()) :: __MODULE__.t()
  def start(%__MODULE__{origin: list} = source) when is_list(list) do
    start(%{source | origin: Strom.Source.Events.new(list)})
  end

  def start(%__MODULE__{origin: origin} = source) when is_struct(origin) do
    origin = apply(origin.__struct__, :start, [origin])
    source = %{source | origin: origin}

    {:ok, pid} = start_link(source)
    __state__(pid)
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
  def call(flow, %__MODULE__{name: name} = source) when is_map(flow) do
    stream =
      Stream.resource(
        fn -> source end,
        fn source -> call(source) end,
        fn source -> source end
      )

    prev_stream = Map.get(flow, name, [])
    Map.put(flow, name, Stream.concat(prev_stream, stream))
  end

  @spec stop(__MODULE__.t()) :: :ok
  def stop(%__MODULE__{origin: origin, pid: pid}) do
    apply(origin.__struct__, :stop, [origin])

    GenServer.call(pid, :stop)
  end

  def __state__(pid) when is_pid(pid), do: GenServer.call(pid, :__state__)

  @impl true
  def handle_call(:call, _from, %__MODULE__{origin: origin} = source) do
    {events, source} =
      case apply(origin.__struct__, :call, [origin]) do
        {:ok, {events, origin}} ->
          source = %{source | origin: origin}
          {events, source}

        {:error, {:halt, origin}} ->
          source = %{source | origin: origin}

          case apply(origin.__struct__, :infinite?, [origin]) do
            true -> {[], source}
            false -> {:halt, source}
          end
      end

    {:reply, {events, source}, source}
  end

  def handle_call(:stop, _from, %__MODULE__{origin: origin} = source) do
    origin = apply(origin.__struct__, :stop, [origin])
    source = %{source | origin: origin}
    {:stop, :normal, :ok, source}
  end

  def handle_call(:__state__, _from, source), do: {:reply, source, source}
end
