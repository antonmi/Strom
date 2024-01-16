defmodule Strom.Transformer do
  @moduledoc """
  Transforms a stream or several streams.
  It works as Stream.map/2 or Stream.transform/3.

      ## `map` example:
      iex> alias Strom.Transformer
      iex> transformer = :numbers |> Transformer.new(&(&1*2)) |> Transformer.start()
      iex> flow = %{numbers: [1, 2, 3]}
      iex> %{numbers: stream} = Transformer.call(flow, transformer)
      iex> Enum.to_list(stream)
      [2, 4, 6]

      ## `transform` example:
      iex> alias Strom.Transformer
      iex> fun = fn el, acc -> {[el, acc], acc + 10} end
      iex> transformer = :numbers |> Transformer.new(fun, 10) |> Transformer.start()
      iex> flow = %{numbers: [1, 2, 3]}
      iex> %{numbers: stream} = Transformer.call(flow, transformer)
      iex> Enum.to_list(stream)
      [1, 10, 2, 20, 3, 30]

      ## it can be applied to several streams:
      iex> alias Strom.Transformer
      iex> transformer = [:s1, :s2] |> Transformer.new(&(&1*2)) |> Transformer.start()
      iex> flow = %{s1: [1, 2, 3], s2: [4, 5, 6]}
      iex> %{s1: s1, s2: s2} = Transformer.call(flow, transformer)
      iex> {Enum.to_list(s1), Enum.to_list(s2)}
      {[2, 4, 6], [8, 10, 12]}
  """

  use GenServer

  @buffer 1000

  defstruct pid: nil,
            running: false,
            opts: [],
            buffer: @buffer,
            function: nil,
            acc: nil,
            names: [],
            tasks: %{},
            data: %{}

  @type t() :: %__MODULE__{}
  @type event() :: any()
  @type acc() :: any()

  @type func() ::
          (event() -> event())
          | (event(), acc() -> {[event()], acc()})

  @spec new(Strom.stream_name(), func(), acc(), list()) :: __MODULE__.t()
  def new(names, function, acc \\ nil, opts \\ []) when is_function(function) and is_list(opts) do
    %__MODULE__{
      function: function,
      acc: acc,
      names: names,
      opts: opts
    }
  end

  @spec start(__MODULE__.t()) :: __MODULE__.t()
  def start(%__MODULE__{opts: opts} = transformer) do
    transformer = %{transformer | buffer: Keyword.get(opts, :buffer, @buffer)}

    {:ok, pid} = start_link(transformer)
    __state__(pid)
  end

  def start_link(%__MODULE__{} = transformer) do
    GenServer.start_link(__MODULE__, transformer)
  end

  @impl true
  def init(%__MODULE__{} = call) do
    {:ok, %{call | pid: self()}}
  end

  @spec call(Strom.flow(), __MODULE__.t()) :: Strom.flow()
  def call(flow, %__MODULE__{names: names, function: function, acc: acc} = transformer)
      when is_map(flow) and is_function(function, 2) do
    names = if is_list(names), do: names, else: [names]

    input_streams =
      Enum.reduce(names, %{}, fn name, streams ->
        Map.put(streams, {name, function, acc}, Map.fetch!(flow, name))
      end)

    :ok = GenServer.call(transformer.pid, {:run_inputs, input_streams})

    sub_flow =
      names
      |> Enum.reduce(%{}, fn name, flow ->
        stream =
          Stream.resource(
            fn ->
              nil
            end,
            fn nil ->
              case GenServer.call(transformer.pid, {:get_data, name}, :infinity) do
                {:ok, data} ->
                  if length(data) == 0 do
                    receive do
                      :continue ->
                        flush()
                    end
                  end

                  {data, nil}

                {:error, :done} ->
                  {:halt, nil}
              end
            end,
            fn nil -> nil end
          )

        Map.put(flow, name, stream)
      end)

    flow
    |> Map.drop(names)
    |> Map.merge(sub_flow)
  end

  def call(flow, %__MODULE__{function: function} = transformer)
      when is_map(flow) and is_function(function, 1) do
    fun = fn el, nil -> {[function.(el)], nil} end
    transformer = %{transformer | function: fun}
    call(flow, transformer)
  end

  @spec stop(__MODULE__.t()) :: :ok
  def stop(%__MODULE__{pid: pid}) do
    GenServer.call(pid, :stop)
  end

  def __state__(pid) when is_pid(pid), do: GenServer.call(pid, :__state__)

  defp run_inputs(streams, pid, buffer) do
    Enum.reduce(streams, %{}, fn {{name, fun, acc}, stream}, streams_acc ->
      task = async_run_stream({name, fun, acc}, stream, buffer, pid)
      Map.put(streams_acc, name, task)
    end)
  end

  defp async_run_stream({name, fun, acc}, stream, buffer, pid) do
    Task.async(fn ->
      stream
      |> Stream.chunk_every(buffer)
      |> Stream.transform(acc, fn chunk, acc ->
        {chunk, new_acc} =
          Enum.reduce(chunk, {[], acc}, fn el, {events, acc} ->
            {new_events, acc} = fun.(el, acc)
            {events ++ new_events, acc}
          end)

        GenServer.cast(pid, {:new_data, name, chunk})

        receive do
          :continue ->
            flush()
        end

        {[], new_acc}
      end)
      |> Stream.run()

      GenServer.cast(pid, {:done, name})
    end)
  end

  defp flush do
    receive do
      :continue ->
        flush()
    after
      0 -> :ok
    end
  end

  @impl true
  def handle_call({:run_inputs, streams_to_call}, _from, %__MODULE__{} = transformer) do
    tasks = run_inputs(streams_to_call, transformer.pid, transformer.buffer)

    {:reply, :ok, %{transformer | running: true, tasks: tasks}}
  end

  def handle_call({:get_data, name}, {pid, _ref}, transformer) do
    send(pid, :continue)

    data = Map.get(transformer.data, name, [])

    if length(data) == 0 and !transformer.running do
      {:reply, {:error, :done}, transformer}
    else
      transformer = %{transformer | data: Map.put(transformer.data, name, [])}
      {:reply, {:ok, data}, transformer}
    end
  end

  def handle_call(:stop, _from, %__MODULE__{} = transformer) do
    {:stop, :normal, :ok, %{transformer | running: false}}
  end

  def handle_call(:__state__, _from, transformer), do: {:reply, transformer, transformer}

  @impl true
  def handle_cast({:new_data, name, chunk}, %__MODULE__{} = transformer) do
    task = Map.fetch!(transformer.tasks, name)
    send(task.pid, :continue)

    prev_data = Map.get(transformer.data, name, [])
    new_data = Map.put(transformer.data, name, prev_data ++ chunk)
    transformer = %{transformer | data: new_data}

    {:noreply, transformer}
  end

  def handle_cast({:done, name}, %__MODULE__{} = transformer) do
    transformer = %{transformer | tasks: Map.delete(transformer.tasks, name)}
    running = map_size(transformer.tasks) > 0
    {:noreply, %{transformer | running: running}}
  end

  @impl true
  def handle_info({_task_ref, :ok}, transformer) do
    # do nothing for now
    {:noreply, transformer}
  end

  def handle_info({:DOWN, _task_ref, :process, _task_pid, :normal}, transformer) do
    # do nothing for now
    {:noreply, transformer}
  end
end
