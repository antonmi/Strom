# defmodule Strom.ExperimentsTest do
#   use ExUnit.Case, async: true

#   alias Strom.{Composite, Mixer, Splitter, Transformer}

#   def build_stream(list, sleep \\ 0) do
#     {:ok, agent} = Agent.start_link(fn -> list end)

#     Stream.resource(
#       fn -> agent end,
#       fn agent ->
#         Process.sleep(sleep)

#         Agent.get_and_update(agent, fn
#           [] -> {{:halt, agent}, []}
#           [datum | data] -> {{[datum], agent}, data}
#         end)
#       end,
#       fn agent -> agent end
#     )
#   end

#   def factorial(n) do
#     Enum.reduce(1..n, 1, &(&1 * &2))
#   end

#   test "lazy" do
#     stream1 = build_stream(Enum.to_list(1..1000))
#     stream2 = build_stream(Enum.to_list(1001..2000))

#     mixer =
#       [:stream1, :stream2]
#       |> Mixer.new(:mixed)
#       |> Mixer.start()

#     splitter =
#       :mixed
#       |> Splitter.new(%{
#         odd: fn el -> rem(el, 2) == 1 end,
#         even: fn el -> rem(el, 2) == 0 end
#       })
#       |> Splitter.start()

#     transformer_odd =
#       :odd
#       |> Transformer.new(&factorial/1, nil)
#       |> Transformer.start()

#     transformer_even =
#       :even
#       |> Transformer.new(&factorial/1, nil)
#       |> Transformer.start()

#     mixer2 =
#       [:odd, :even]
#       |> Mixer.new(:stream)
#       |> Mixer.start()

#     %{stream: stream} =
#       %{stream1: stream1, stream2: stream2}
#       |> Mixer.call(mixer)
#       |> Splitter.call(splitter)
#       |> Transformer.call(transformer_odd)
#       |> Transformer.call(transformer_even)
#       |> Mixer.call(mixer2)

#     #    :observer.start()
#     Process.sleep(5000)

#     stream
#     |> Stream.each(&IO.inspect/1)
#     |> Stream.run()
#   end

#   @tag timeout: :infinity
#   test "event speed in transformer" do
#     :observer.start()

#     print_time =
#       Transformer.new(:stream, fn _event ->
#         Process.sleep(1000)
#         IO.inspect(:erlang.system_time(:millisecond))
#       end)

#     transformer = Transformer.new(:stream, & &1)
#     transformers = List.duplicate(transformer, 100_000)

#     duration =
#       Transformer.new(:stream, fn time ->
#         IO.inspect(:erlang.system_time(:millisecond) - time)
#       end)

#     IO.inspect("starting")
#     starting = :erlang.system_time(:second)

#     composite =
#       [print_time, transformers, duration]
#       |> Composite.new()
#       |> Composite.start()

#     IO.inspect("started #{:erlang.system_time(:second) - starting}")
#     Process.sleep(5000)

#     IO.inspect("calling")
#     numbers = Enum.to_list(1..10000)
#     starting = :erlang.system_time(:second)
#     flow = Composite.call(%{stream: numbers}, composite)
#     IO.inspect("called #{:erlang.system_time(:second) - starting}")

#     Process.sleep(5000)
#     IO.inspect("running")

#     miliseconds =
#       Enum.to_list(flow[:stream])
#       |> IO.inspect()

#     IO.inspect(Enum.sum(miliseconds) / length(numbers))

#     Process.sleep(5000)
#     :erlang.garbage_collect() |> IO.inspect()
#     Process.sleep(5000)
#   end

#   @tag timeout: :infinity
#   test "event speed in mixer" do
#     :observer.start()

#     print_time =
#       Transformer.new(:stream, fn _event ->
#         Process.sleep(1000)
#         IO.inspect(:erlang.system_time(:millisecond))
#       end)

#     mixers = List.duplicate(Mixer.new([:stream], :stream), 100_000)

#     duration =
#       Transformer.new(:stream, fn time ->
#         IO.inspect(:erlang.system_time(:millisecond) - time)
#       end)

#     IO.inspect("starting")
#     starting = :erlang.system_time(:second)

#     composite =
#       [print_time, mixers, duration]
#       |> Composite.new()
#       |> Composite.start()

#     IO.inspect("started #{:erlang.system_time(:second) - starting}")

#     Process.sleep(5000)

#     IO.inspect("calling")
#     starting = :erlang.system_time(:second)

#     flow =
#       %{stream: Enum.to_list(1..10)}
#       |> Composite.call(composite)

#     IO.inspect("called #{:erlang.system_time(:second) - starting}")

#     Process.sleep(5000)
#     IO.inspect("Running")

#     miliseconds =
#       Enum.to_list(flow[:stream])
#       |> IO.inspect()

#     IO.inspect(Enum.sum(miliseconds) / 10)

#     Process.sleep(5000)
#   end

#   test "tasks memory consumption" do
#     :observer.start()

#     Enum.map(1..1_000_000, fn _i ->
#       Task.async(fn -> Process.sleep(50000) end)
#     end)
#     |> Enum.map(&Task.await(&1, :infinity))
#   end

#   test "two streams balancing" do
#     quick = Stream.cycle([101])
#     slow = build_stream(Enum.to_list(1..100), 10)

#     mixer = Mixer.new([:quick, :slow], :stream, buffer: 3, chunk: 1)

#     mark =
#       Transformer.new(
#         :stream,
#         fn el, acc ->
#           {large, small} = acc
#           #      IO.inspect(acc, label: :acc)

#           case {el > 100, large - small > 0} do
#             {true, true} ->
#               {[{:slow_down, el}], {large + 1, small}}

#             {true, false} ->
#               {[el], {large + 1, small}}

#             {false, _} ->
#               if small - large > 0 do
#                 {[{:slow_down, el}], {large, small + 1}}
#               else
#                 {[el], {large, small + 1}}
#               end
#           end
#         end,
#         {0, 0},
#         buffer: 3,
#         chunk: 1
#       )

#     splitter =
#       Splitter.new(
#         :stream,
#         %{
#           slow: fn
#             {:slow_down, el} -> el <= 100
#             el -> el <= 100
#           end,
#           quick: fn
#             {:slow_down, el} -> el > 100
#             el -> el > 100
#           end
#         },
#         buffer: 3,
#         chunk: 1
#       )

#     slow_down =
#       Transformer.new(
#         [:slow, :quick],
#         fn
#           {:slow_down, el} ->
#             IO.inspect("-----#{el}-----------------------------------------------------")
#             Process.sleep(10)
#             el

#           el ->
#             el
#         end,
#         nil,
#         buffer: 3,
#         chunk: 1
#       )

#     composite =
#       [mixer, mark, splitter, slow_down, mixer]
#       |> Composite.new()
#       |> Composite.start()

#     %{stream: stream} = Composite.call(%{quick: quick, slow: slow}, composite)

#     stream
#     |> Stream.each(&IO.inspect/1)
#     |> Stream.run()
#   end
# end
