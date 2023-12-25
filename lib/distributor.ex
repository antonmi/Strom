defmodule Strom.Distributor do
  use GenServer

  @buffer 1000

  defstruct streams: %{},
            pid: nil,
            running: false,
            data: %{},
            buffer: @buffer,
            no_data_counter: 0,
            in_tasks: %{},
            consumers: %{}

  def start(opts \\ []) when is_list(opts) do
    state = %__MODULE__{
      buffer: Keyword.get(opts, :buffer, @buffer)
    }

    {:ok, pid} = GenServer.start_link(__MODULE__, state)
    __state__(pid)
  end

  def init(%__MODULE__{} = distributor) do
    {:ok, %{distributor | pid: self()}}
  end

  def call(flow, %__MODULE__{} = distributor, inputs, outputs)
      when is_map(flow) and is_map(inputs) and is_map(outputs) do
    input_streams =
      Enum.reduce(inputs, %{}, fn {name, fun}, acc ->
        Map.put(acc, {name, fun}, Map.fetch!(flow, name))
      end)

    sub_flow =
      outputs
      |> Enum.reduce(%{}, fn {name, fun}, flow ->
        cons = Strom.Cons.start({name, fun}, distributor.pid)
        :ok = GenServer.call(distributor.pid, {:register_consumer, {{name, fun}, cons}})
        stream = Strom.Cons.call(cons)
        Map.put(flow, name, stream)
      end)

    :ok = GenServer.call(distributor.pid, {:run_inputs, input_streams})

    flow
    |> Map.drop(Map.keys(inputs))
    |> Map.merge(sub_flow)
  end

  def stop(%__MODULE__{pid: pid}), do: GenServer.call(pid, :stop)

  def __state__(pid) when is_pid(pid), do: GenServer.call(pid, :__state__)

  defp run_inputs(streams, pid, buffer) do
    Enum.reduce(streams, %{}, fn {{name, fun}, stream}, acc ->
      task = async_run_stream({name, fun}, stream, buffer, pid)
      Map.put(acc, {name, fun}, task)
    end)
  end

  defp async_run_stream({name, fun}, stream, buffer, pid) do
    Task.async(fn ->
      stream
      |> Stream.chunk_every(buffer)
      |> Stream.each(fn chunk ->
        {chunk, _} = Enum.split_with(chunk, fun)
        GenServer.cast(pid, {:new_data, {name, fun}, chunk})

        receive do
          :continue ->
            flush()
        end
      end)
      |> Stream.run()

      GenServer.cast(pid, {:done, {name, fun}})
    end)
  end

  defp flush do
    receive do
      _ -> flush()
    after
      0 -> :ok
    end
  end

  def handle_cast({:new_data, {name, fun}, chunk}, %__MODULE__{} = distributor) do
    Enum.each(distributor.consumers, fn {{name, fun}, cons} ->
      GenServer.cast(cons.pid, {:put_data, chunk})
      GenServer.cast(cons.pid, :continue)
    end)

    {:noreply, distributor}
  end

  def handle_call({:run_inputs, streams_to_mix}, _from, %__MODULE__{} = mixer) do
    in_tasks = run_inputs(streams_to_mix, mixer.pid, mixer.buffer)

    {:reply, :ok, %{mixer | running: true, streams: streams_to_mix, in_tasks: in_tasks}}
  end

  def handle_call({:register_consumer, {{name, fun}, cons}}, _from, %__MODULE__{} = distributor) do
    distributor = %{distributor | consumers: Map.put(distributor.consumers, {name, fun}, cons)}
    {:reply, :ok, distributor}
  end

  def handle_call(:stop, _from, %__MODULE__{} = mixer) do
    {:stop, :normal, :ok, %{mixer | running: false}}
  end

  def handle_cast({:done, {name, fun}}, %__MODULE__{} = distributor) do
    in_tasks = Map.delete(distributor.in_tasks, {name, fun})
    distributor = %{distributor | in_tasks: in_tasks}

    if map_size(distributor.in_tasks) == 0 do
      Enum.each(distributor.consumers, fn {{name, fun}, cons} ->
        GenServer.cast(cons.pid, :continue)
        GenServer.cast(cons.pid, :stop)
      end)
    end

    {:noreply, distributor}
  end

  def handle_cast(:continue, %__MODULE__{} = distributor) do
    Enum.each(distributor.in_tasks, fn {{name, fun}, task} ->
      send(task.pid, :continue)
    end)

    {:noreply, distributor}
  end

  def handle_call(:__state__, _from, mixer), do: {:reply, mixer, mixer}

  def handle_info({_task_ref, :ok}, mixer) do
    # do nothing for now
    {:noreply, mixer}
  end

  def handle_info({:DOWN, _task_ref, :process, _task_pid, :normal}, mixer) do
    # do nothing for now
    {:noreply, mixer}
  end
end
