ExUnit.start()

defmodule Strom.TestHelper do
  def build_stream(list, sleep \\ 0) do
    {:ok, agent} = Agent.start_link(fn -> list end)

    Stream.resource(
      fn -> agent end,
      fn agent ->
        Process.sleep(sleep)

        Agent.get_and_update(agent, fn
          [] -> {{:halt, agent}, []}
          [datum | data] -> {{[datum], agent}, data}
        end)
      end,
      fn agent -> agent end
    )
  end

  def wait_for_dying(pid) do
    if Process.alive?(pid) do
      wait_for_dying(pid)
    else
      true
    end
  end
end
