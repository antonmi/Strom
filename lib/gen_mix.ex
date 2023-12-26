defmodule Strom.GenMix do
  use GenServer

  @buffer 1000

  defstruct pid: nil,
            running: false,
            buffer: @buffer,
            producers: %{},
            consumers: %{}

  alias Strom.GenMix.Consumer

  # TODO supervisor
  def start(opts \\ []) when is_list(opts) do
    state = %__MODULE__{
      buffer: Keyword.get(opts, :buffer, @buffer)
    }

    {:ok, pid} = GenServer.start_link(__MODULE__, state)
    __state__(pid)
  end

  @impl true
  def init(%__MODULE__{} = mix) do
    {:ok, %{mix | pid: self()}}
  end

  def call(flow, %__MODULE__{} = mix, inputs, outputs)
      when is_map(flow) and is_map(inputs) and is_map(outputs) do

    input_streams =
      Enum.reduce(inputs, %{}, fn {name, fun}, acc ->
        Map.put(acc, {name, fun}, Map.fetch!(flow, name))
      end)
    sub_flow =
      outputs
      |> Enum.reduce(%{}, fn {name, fun}, flow ->
        consumer = Consumer.start({name, fun}, mix.pid)
        :ok = GenServer.call(mix.pid, {:register_consumer, {{name, fun}, consumer}})
        stream = Consumer.call(consumer)
        Map.put(flow, name, stream)
      end)

    :ok = GenServer.call(mix.pid, {:run_inputs, input_streams})

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

  @impl true
  def handle_call({:run_inputs, streams_to_mix}, _from, %__MODULE__{} = mix) do
    producers = run_inputs(streams_to_mix, mix.pid, mix.buffer)

    {:reply, :ok, %{mix | running: true, producers: producers}}
  end

  def handle_call({:register_consumer, {{name, fun}, cons}}, _from, %__MODULE__{} = mix) do
    mix = %{mix | consumers: Map.put(mix.consumers, {name, fun}, cons)}
    {:reply, :ok, mix}
  end

  def handle_call(:stop, _from, %__MODULE__{} = mix) do
    {:stop, :normal, :ok, %{mix | running: false}}
  end

  def handle_call(:__state__, _from, mix), do: {:reply, mix, mix}

  @impl true
  def handle_cast({:new_data, {_name, _fun}, chunk}, %__MODULE__{} = mix) do
    Enum.each(mix.consumers, fn {_, cons} ->
      GenServer.cast(cons.pid, {:put_data, chunk})
      GenServer.cast(cons.pid, :continue)
    end)

    {:noreply, mix}
  end

  def handle_cast({:done, {name, fun}}, %__MODULE__{} = mix) do
    mix = %{mix | producers: Map.delete(mix.producers, {name, fun})}

    if map_size(mix.producers) == 0 do
      Enum.each(mix.consumers, fn {_, cons} ->
        GenServer.cast(cons.pid, :continue)
        GenServer.cast(cons.pid, :stop)
      end)
    end

    {:noreply, mix}
  end

  def handle_cast({:consumer_got_data, {_name, _fun}}, %__MODULE__{} = mix) do
    Enum.each(mix.producers, fn {_, task} ->
      send(task.pid, :continue)
    end)

    {:noreply, mix}
  end

  @impl true
  def handle_info({_task_ref, :ok}, mix) do
    # do nothing for now
    {:noreply, mix}
  end

  def handle_info({:DOWN, _task_ref, :process, _task_pid, :normal}, mix) do
    # do nothing for now
    {:noreply, mix}
  end
end
