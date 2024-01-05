defmodule Strom.Transformer do
  use GenServer

  @buffer 1000

  defstruct pid: nil,
            running: false,
            buffer: @buffer,
            function: nil,
            opts: nil,
            tasks: %{},
            data: %{}

  # TODO supervisor
  def start(opts \\ []) when is_list(opts) do
    state = %__MODULE__{
      buffer: Keyword.get(opts, :buffer, @buffer),
      opts: Keyword.get(opts, :opts, nil)
    }

    {:ok, pid} = GenServer.start_link(__MODULE__, state)
    __state__(pid)
  end

  @impl true
  def init(%__MODULE__{} = call) do
    {:ok, %{call | pid: self()}}
  end

  def call(flow, %__MODULE__{} = call, names, {function, acc})
      when is_map(flow) and is_function(function, 3) do
    names = if is_list(names), do: names, else: [names]

    input_streams =
      Enum.reduce(names, %{}, fn name, streams ->
        Map.put(streams, {name, function, acc}, Map.fetch!(flow, name))
      end)

    :ok = GenServer.call(call.pid, {:run_inputs, input_streams})

    sub_flow =
      names
      |> Enum.reduce(%{}, fn name, flow ->
        stream =
          Stream.resource(
            fn ->
              nil
            end,
            fn nil ->
              case GenServer.call(call.pid, {:get_data, name}) do
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

  def call(flow, %__MODULE__{} = call, names, {function, acc})
      when is_map(flow) and is_function(function, 2) do
    fun = fn el, acc, nil -> function.(el, acc) end
    call(flow, %__MODULE__{} = call, names, {fun, acc})
  end

  def call(flow, %__MODULE__{} = call, names, function)
      when is_map(flow) and is_function(function, 1) do
    fun = fn el, nil, nil -> {[function.(el)], nil} end
    call(flow, %__MODULE__{} = call, names, {fun, nil})
  end

  def stop(%__MODULE__{pid: pid}), do: GenServer.call(pid, :stop)

  def __state__(pid) when is_pid(pid), do: GenServer.call(pid, :__state__)

  defp run_inputs(streams, pid, buffer, opts) do
    Enum.reduce(streams, %{}, fn {{name, fun, acc}, stream}, streams_acc ->
      task = async_run_stream({name, fun, acc, opts}, stream, buffer, pid)
      Map.put(streams_acc, name, task)
    end)
  end

  defp async_run_stream({name, fun, acc, opts}, stream, buffer, pid) do
    Task.async(fn ->
      stream
      |> Stream.chunk_every(buffer)
      |> Stream.transform(acc, fn chunk, acc ->
        {chunk, new_acc} =
          Enum.reduce(chunk, {[], acc}, fn el, {events, acc} ->
            {new_events, acc} = fun.(el, acc, opts)
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
      _ -> flush()
    after
      0 -> :ok
    end
  end

  @impl true
  def handle_call({:run_inputs, streams_to_call}, _from, %__MODULE__{opts: opts} = call) do
    tasks = run_inputs(streams_to_call, call.pid, call.buffer, opts)

    {:reply, :ok, %{call | running: true, tasks: tasks}}
  end

  def handle_call({:get_data, name}, {pid, _ref}, call) do
    send(pid, :continue)

    data = Map.get(call.data, name, [])

    if length(data) == 0 and !call.running do
      {:reply, {:error, :done}, call}
    else
      call = %{call | data: Map.put(call.data, name, [])}
      {:reply, {:ok, data}, call}
    end
  end

  def handle_call(:stop, _from, %__MODULE__{} = call) do
    {:stop, :normal, :ok, %{call | running: false}}
  end

  def handle_call(:__state__, _from, call), do: {:reply, call, call}

  @impl true
  def handle_cast({:new_data, name, chunk}, %__MODULE__{} = call) do
    task = Map.fetch!(call.tasks, name)
    send(task.pid, :continue)

    prev_data = Map.get(call.data, name, [])
    new_data = Map.put(call.data, name, prev_data ++ chunk)
    call = %{call | data: new_data}

    {:noreply, call}
  end

  def handle_cast({:done, name}, %__MODULE__{} = call) do
    call = %{call | tasks: Map.delete(call.tasks, name)}
    running = map_size(call.tasks) > 0
    {:noreply, %{call | running: running}}
  end

  @impl true
  def handle_info({_task_ref, :ok}, call) do
    # do nothing for now
    {:noreply, call}
  end

  def handle_info({:DOWN, _task_ref, :process, _task_pid, :normal}, call) do
    # do nothing for now
    {:noreply, call}
  end
end
