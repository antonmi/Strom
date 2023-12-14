defmodule Strom.Splitter do
  use GenServer

  @buffer 1000

  defstruct pid: nil,
            stream: nil,
            partitions: %{},
            running: false,
            buffer: @buffer,
            no_data_counter: 0

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

    :ok = GenServer.call(splitter.pid, {:run_stream, stream_to_run})

    sub_flow =
      partitions
      |> Enum.reduce(%{}, fn {name, fun}, flow ->
        stream =
          Stream.resource(
            fn -> splitter end,
            fn splitter ->
              case GenServer.call(splitter.pid, {:get_data, {name, fun}}) do
                {:ok, {data, no_data_counter}} ->
                  maybe_wait(no_data_counter, 0)

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
      |> Stream.each(fn el ->
        data_size = GenServer.call(pid, {:new_data, el})
        maybe_wait(data_size, buffer)
      end)
      |> Stream.run()

      GenServer.call(pid, :done)
    end)
  end

  defp maybe_wait(current, allowed) do
    if current > allowed do
      diff = current - allowed
      to_sleep = trunc(:math.pow(2, diff))
      Process.sleep(to_sleep)
      to_sleep
    end
  end

  def handle_call({:new_data, datum}, _from, %__MODULE__{} = splitter) do
    new_partitions =
      Enum.reduce(splitter.partitions, %{}, fn {{name, fun}, prev_data}, acc ->
        if fun.(datum) do
          Map.put(acc, {name, fun}, [datum | prev_data])
        else
          Map.put(acc, {name, fun}, prev_data)
        end
      end)

    data_size =
      Enum.reduce(new_partitions, 0, fn {_key, data}, acc -> acc + length(data) end)

    {:reply, data_size, %{splitter | partitions: new_partitions}}
  end

  def handle_call({:run_stream, stream}, _from, %__MODULE__{} = splitter) do
    async_run_stream(stream, splitter.buffer, splitter.pid)
    {:reply, :ok, %{splitter | running: true}}
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

  def handle_call(
        {:get_data, partition_fun},
        _from,
        %__MODULE__{partitions: partitions, running: running} = splitter
      ) do
    data =
      partitions
      |> Map.get(partition_fun)
      |> Enum.reverse()

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
