defmodule Strom.Source do
  @callback start(map) :: map
  @callback call(map) :: {:ok, {[term], map}} | {:error, {:halt, map}}
  @callback stop(map) :: map
  @callback infinite?(map) :: true | false

  use GenServer

  defstruct origin: nil,
            name: nil,
            pid: nil

  def new(name, origin) do
    unless is_struct(origin) or is_list(origin) do
      raise "Source origin must be a struct or just simple list, given: #{inspect(origin)}"
    end

    %__MODULE__{origin: origin, name: name}
  end

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

  def call(%__MODULE__{pid: pid}), do: GenServer.call(pid, :call, :infinity)

  def infinite?(%__MODULE__{pid: pid}), do: GenServer.call(pid, :infinite)

  def stop(%__MODULE__{origin: origin, pid: pid}) do
    apply(origin.__struct__, :stop, [origin])

    GenServer.call(pid, :stop)
  end

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
