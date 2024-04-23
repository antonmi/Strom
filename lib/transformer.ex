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

  @chunk 1
  @buffer 1000

  defstruct pid: nil,
            opts: [],
            chunk: @chunk,
            buffer: @buffer,
            function: nil,
            acc: nil,
            names: [],
            tasks: %{},
            data: %{},
            waiting_clients: %{}

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
    transformer = %{
      transformer
      | chunk: Keyword.get(opts, :chunk, @chunk),
        buffer: Keyword.get(opts, :buffer, @buffer)
    }

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

  defp run_inputs(streams, pid, chunk) do
    Enum.reduce(streams, %{}, fn {{name, fun, acc}, stream}, streams_acc ->
      task = async_run_stream({name, fun, acc}, stream, chunk, pid)
      Map.put(streams_acc, name, task)
    end)
  end

  defp async_run_stream({name, fun, acc}, stream, chunk, pid) do
    Task.async(fn ->
      stream
      |> Stream.chunk_every(chunk)
      |> Stream.transform(acc, fn chunk, acc ->
        {chunk, new_acc} =
          Enum.reduce(chunk, {[], acc}, fn el, {events, acc} ->
            {new_events, acc} = fun.(el, acc)
            {events ++ new_events, acc}
          end)

        GenServer.cast(pid, {:new_data, name, chunk})

        receive do
          :continue_task ->
            flush(:continue_task)
        end

        {[], new_acc}
      end)
      |> Stream.run()

      GenServer.cast(pid, {:done, name})
    end)
  end

  defp flush(message) do
    receive do
      ^message ->
        flush(message)
    after
      0 -> :ok
    end
  end

  @impl true
  def handle_call({:run_inputs, streams_to_call}, _from, %__MODULE__{} = transformer) do
    tasks = run_inputs(streams_to_call, transformer.pid, transformer.chunk)

    {:reply, :ok, %{transformer | tasks: tasks}}
  end

  def handle_call({:get_data, name}, {pid, _ref}, transformer) do
    if task = transformer.tasks[name] do
      send(task.pid, :continue_task)
    end

    data = Map.get(transformer.data, name, [])
    #    IO.inspect(length(data), label: inspect({:get_data, name}))
    #    IO.inspect(transformer.tasks, label: inspect({:get_data, name}))

    cond do
      length(data) == 0 and is_nil(transformer.tasks[name]) ->
        {:reply, :done, transformer}

      length(data) == 0 ->
        waiting_clients = Map.put(transformer.waiting_clients, name, pid)
        {:reply, :pause, %{transformer | waiting_clients: waiting_clients}}

      true ->
        transformer = %{transformer | data: Map.put(transformer.data, name, [])}
        {:reply, {:data, data}, transformer}
    end
  end

  def handle_call(:stop, _from, %__MODULE__{} = transformer) do
    {:stop, :normal, :ok, transformer}
  end

  def handle_call(:__state__, _from, transformer), do: {:reply, transformer, transformer}

  @impl true
  def handle_cast({:new_data, name, chunk}, %__MODULE__{} = transformer) do
    prev_data = Map.get(transformer.data, name, [])
    all_data = prev_data ++ chunk
    #    IO.inspect(length(all_data), label: inspect({:new_data, name}))

    waiting_clients =
      case transformer.waiting_clients[name] do
        nil ->
          transformer.waiting_clients

        client_pid when is_pid(client_pid) ->
          send(client_pid, :continue_client)
          Map.delete(transformer.waiting_clients, name)
      end

    if length(all_data) <= transformer.buffer do
      task = Map.fetch!(transformer.tasks, name)
      send(task.pid, :continue_task)
    end

    new_data = Map.put(transformer.data, name, all_data)
    transformer = %{transformer | data: new_data, waiting_clients: waiting_clients}

    {:noreply, transformer}
  end

  def handle_cast({:done, name}, %__MODULE__{} = transformer) do
    transformer = %{transformer | tasks: Map.delete(transformer.tasks, name)}
    # IO.inspect({:done, name})
    waiting_clients =
      case transformer.waiting_clients[name] do
        nil ->
          transformer.waiting_clients

        client_pid when is_pid(client_pid) ->
          send(client_pid, :continue_client)
          Map.delete(transformer.waiting_clients, name)
      end

    {:noreply, %{transformer | waiting_clients: waiting_clients}}
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
