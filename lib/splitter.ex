defmodule Strom.Splitter do
  use GenServer

  defstruct [:pid, :stream, :partitions, :running, :chunk_every]

  @chunk_every 100

  def start(opts \\ []) when is_list(opts) do
    state = %__MODULE__{
      running: MapSet.new(),
      partitions: %{},
      chunk_every: Keyword.get(opts, :chunk_every, @chunk_every)
    }

    {:ok, pid} = GenServer.start_link(__MODULE__, state)
    __state__(pid)
  end

  def init(%__MODULE__{} = splitter) do
    {:ok, %{splitter | pid: self()}}
  end

  def call(flow, %__MODULE__{} = splitter, name, partitions) when is_list(partitions) do
    partitions =
      Enum.reduce(partitions, %{}, fn name, acc ->
        Map.put(acc, name, fn _el -> true end)
      end)

    call(flow, splitter, name, partitions)
  end

  def call(flow, %__MODULE__{} = splitter, name, partitions) when is_map(partitions) do
    GenServer.call(splitter.pid, {:set_partitions, partitions})
    stream_to_run = Map.fetch!(flow, name)

    sub_flow =
      partitions
      |> Enum.reduce(%{}, fn {name, fun}, flow ->
        stream =
          Stream.resource(
            fn -> GenServer.call(splitter.pid, {:run_stream, stream_to_run, {name, fun}}) end,
            fn splitter ->
              case GenServer.call(splitter.pid, {:get_data, {name, fun}}) do
                {:ok, data} ->
                  {data, splitter}

                {:error, :done} ->
                  {:halt, splitter}
              end
            end,
            fn splitter -> splitter end
          )

        Map.put(flow, name, stream)
      end)

    flow
    |> Map.delete(name)
    |> Map.merge(sub_flow)
  end

  def stop(%__MODULE__{pid: pid}), do: GenServer.call(pid, :stop)

  def __state__(pid) when is_pid(pid), do: GenServer.call(pid, :__state__)

  defp async_run_stream(stream, {name, fun}, chunk_every, pid) do
    Task.async(fn ->
      stream
      |> Stream.chunk_every(chunk_every)
      |> Stream.each(fn chunk ->
        data_size = GenServer.call(pid, {:new_data, chunk})
        maybe_wait(data_size, chunk_every)
      end)
      |> Stream.run()

      GenServer.call(pid, {:done, {name, fun}})
    end)
  end

  defp maybe_wait(data_size, chunk_every) do
    if data_size > 10 * chunk_every do
      div = div(data_size, 10 * chunk_every)
      to_sleep = trunc(:math.pow(2, div))
      Process.sleep(to_sleep)
    end
  end

  def handle_call({:new_data, data}, _from, %__MODULE__{} = splitter) do
    new_partitions =
      Enum.reduce(splitter.partitions, %{}, fn {{name, fun}, prev_data}, acc ->
        case Enum.split_with(data, fun) do
          {[], _} ->
            Map.put(acc, {name, fun}, prev_data)

          {data, _} ->
            new_data = prev_data ++ data
            Map.put(acc, {name, fun}, new_data)
        end
      end)

    data_size =
      Enum.reduce(new_partitions, 0, fn {_key, data}, acc -> acc + length(data) end)

    {:reply, data_size, %{splitter | partitions: new_partitions}}
  end

  def handle_call({:run_stream, stream, {name, fun}}, _from, %__MODULE__{} = splitter) do
    async_run_stream(stream, {name, fun}, splitter.chunk_every, splitter.pid)
    splitter = %{splitter | running: MapSet.put(splitter.running, {name, fun})}
    {:reply, splitter, splitter}
  end

  def handle_call({:set_partitions, partitions}, _from, %__MODULE__{} = splitter) do
    partitions =
      Enum.reduce(partitions, %{}, fn {name, fun}, acc -> Map.put(acc, {name, fun}, []) end)

    splitter = %{splitter | partitions: partitions}
    {:reply, splitter, splitter}
  end

  def handle_call({:done, {name, fun}}, _from, %__MODULE__{} = splitter) do
    running = MapSet.delete(splitter.running, {name, fun})
    {:reply, :ok, %{splitter | running: running}}
  end

  def handle_call(
        {:get_data, partition_fun},
        _from,
        %__MODULE__{partitions: partitions, running: running} = splitter
      ) do
    data = Map.get(partitions, partition_fun)

    if length(data) == 0 && MapSet.size(running) == 0 do
      {:reply, {:error, :done}, splitter}
    else
      {:reply, {:ok, data}, %{splitter | partitions: Map.put(partitions, partition_fun, [])}}
    end
  end

  def handle_call(:stop, _from, %__MODULE__{} = splitter) do
    {:stop, :normal, :ok, %{splitter | running: MapSet.new(), partitions: %{}}}
  end

  def handle_call(:__state__, _from, splitter), do: {:reply, splitter, splitter}

  def handle_info({_task_ref, :ok}, splitter) do
    # do nothing for now
    {:noreply, splitter}
  end

  def handle_info({:DOWN, _task_ref, :process, _task_pid, :normal}, splitter) do
    # do nothing for now
    {:noreply, splitter}
  end
end
