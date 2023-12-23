defmodule Strom.Splitter do
  use GenServer

  @buffer 1000

  defstruct pid: nil,
            stream: nil,
            partitions: %{},
            running: false,
            buffer: @buffer,
            no_data_counter: 0,
            task: nil,
            consumers: []

  def start(opts \\ []) when is_list(opts) do
    state = %__MODULE__{
      buffer: Keyword.get(opts, :buffer, @buffer)
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

    task = GenServer.call(splitter.pid, {:run_stream, stream_to_run})

    sub_flow =
      partitions
      |> Enum.reduce(%{}, fn {name, fun}, flow ->
        stream =
          Stream.resource(
            fn ->
              GenServer.call(splitter.pid, {:register_consumer, self()})
              |> IO.inspect
            end,
            fn splitter ->
              case GenServer.call(splitter.pid, {:get_data, {name, fun}}) do
                {:ok, {data, no_data_counter}} ->
#                if rem(no_data_counter, 10) == 9 do
                if length(data) == 0 do
#                  Process.sleep(1)
                  IO.inspect(no_data_counter, label: "no_data_counter_splitter: #{name}")
                  receive do
                    :continue ->
                      flush()
                  end
                end
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

  defp async_run_stream(stream, buffer, pid) do
    Task.async(fn ->
      stream
      |> Stream.chunk_every(buffer)
      |> Stream.each(fn chunk ->
        GenServer.cast(pid, {:new_data, chunk})
        receive do
          :continue ->
            flush()
        end
      end)
      |> Stream.run()

      GenServer.call(pid, :done)
    end)
    |> IO.inspect(label: "slitter task")
  end

  defp flush do
    receive do
      _ -> flush()
    after
      0 -> :ok
    end
  end

  def handle_cast({:new_data, data}, %__MODULE__{} = splitter) do
    new_partitions =
      Enum.reduce(splitter.partitions, %{}, fn {{name, fun}, prev_data}, acc ->
        {valid_data, _} = Enum.split_with(data, fun)
        new_data = prev_data ++ valid_data
        Map.put(acc, {name, fun}, new_data)
      end)

    splitter.consumers
    |> Enum.shuffle()
    |> Enum.each(&send(&1, :continue))

    {:noreply, %{splitter | partitions: new_partitions}}
  end

  def handle_call({:run_stream, stream}, _from, %__MODULE__{} = splitter) do
    task = async_run_stream(stream, splitter.buffer, splitter.pid)
    {:reply, :ok, %{splitter | running: true, task: task}}
  end

  def handle_call({:set_partitions, partitions}, _from, %__MODULE__{} = splitter) do
    partitions =
      Enum.reduce(partitions, %{}, fn {name, fun}, acc -> Map.put(acc, {name, fun}, []) end)

    splitter = %{splitter | partitions: partitions}
    {:reply, splitter, splitter}
  end

  def handle_call(:done, _from, %__MODULE__{} = splitter) do
    {:reply, :ok, %{splitter | running: false}}
  end

  def handle_call({:register_consumer, pid},_from,%__MODULE__{consumers: consumers} = splitter) do
    splitter = %{splitter | consumers: [pid | consumers]}
    {:reply, splitter, splitter}
  end

  def handle_call(
        {:get_data, partition_fun},
        _from,
        %__MODULE__{partitions: partitions, running: running} = splitter
      ) do
    send(splitter.task.pid, :continue)

    data = Map.get(partitions, partition_fun)
    if length(data) == 0 && !running do
      {:reply, {:error, :done}, splitter}
    else
      no_data_counter = if length(data) == 0, do: splitter.no_data_counter + 1, else: 0

      splitter = %{
        splitter
        | partitions: Map.put(partitions, partition_fun, []),
          no_data_counter: no_data_counter
      }

      {:reply, {:ok, {data, no_data_counter}}, splitter}
    end
  end

  def handle_call(:stop, _from, %__MODULE__{} = splitter) do
    {:stop, :normal, :ok, %{splitter | running: false, partitions: %{}}}
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
