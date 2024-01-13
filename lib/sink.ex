defmodule Strom.Sink do
  @callback start(map) :: map
  @callback call(map, term) :: {:ok, {term, map}} | {:error, {term, map}}
  @callback stop(map) :: map

  use GenServer

  defstruct origin: nil,
            name: nil,
            sync: false,
            pid: nil,
            flow_pid: nil,
            sup_pid: nil

  def new(name, origin, sync \\ false) do
    unless is_struct(origin) do
      raise "Sink origin must be a struct, given: #{inspect(origin)}"
    end

    %__MODULE__{origin: origin, name: name, sync: sync}
  end

  def start(%__MODULE__{origin: origin} = sink) when is_struct(origin) do
    origin = apply(origin.__struct__, :start, [origin])
    sink = %{sink | origin: origin}

    {:ok, pid} =
      if sink.sup_pid do
        DynamicSupervisor.start_child(sink.sup_pid, {__MODULE__, sink})
      else
        start_link(sink)
      end

    __state__(pid)
  end

  def start_link(%__MODULE__{} = sink) do
    GenServer.start_link(__MODULE__, sink)
  end

  @impl true
  def init(%__MODULE__{} = sink), do: {:ok, %{sink | pid: self()}}

  def call(%__MODULE__{pid: pid}, data), do: GenServer.call(pid, {:call, data})

  def stop(%__MODULE__{origin: origin, pid: pid, sup_pid: sup_pid}) do
    apply(origin.__struct__, :stop, [origin])

    if sup_pid do
      :ok
    else
      GenServer.call(pid, :stop)
    end
  end

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
