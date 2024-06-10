defmodule Strom.Loop do
  @moduledoc false

  @timeout 5000
  @sleep 1

  defstruct name: nil,
            last_empty_call_at: nil,
            timeout: @timeout,
            sleep: @sleep,
            infinite: false

  def new(name, opts \\ []) do
    %__MODULE__{
      name: name,
      timeout: Keyword.get(opts, :timeout, @timeout),
      sleep: Keyword.get(opts, :sleep, @sleep)
    }
  end

  def start(%__MODULE__{name: name} = loop) do
    case Agent.start_link(fn -> [] end, name: name) do
      {:ok, _pid} ->
        loop

      {:error, {:already_started, _pid}} ->
        loop
    end
  end

  # call as a source
  def call(%__MODULE__{name: name} = loop) do
    Agent.get_and_update(name, fn data ->
      case data do
        [hd | tl] -> {hd, tl}
        [] -> {nil, []}
      end
    end)
    |> case do
      nil ->
        case loop.last_empty_call_at do
          nil ->
            Process.sleep(loop.sleep)
            {[], %{loop | last_empty_call_at: System.os_time(:millisecond)}}

          last_empty_call_at ->
            if System.os_time(:millisecond) - last_empty_call_at > loop.timeout do
              {:halt, loop}
            else
              {[], loop}
            end
        end

      datum ->
        {[datum], %{loop | last_empty_call_at: nil}}
    end
  end

  # call as a sink
  def call(%__MODULE__{name: name} = loop, data) do
    :ok = Agent.update(name, fn prev_data -> prev_data ++ [data] end)
    loop
  end

  def stop(%__MODULE__{name: name} = loop) do
    :ok = Agent.stop(name)
    loop
  end

  def infinite?(%__MODULE__{infinite: infinite}), do: infinite
end
