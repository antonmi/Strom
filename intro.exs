# Stateless Transformer
plus_one = Transformer.new(:numbers, &(&1 + 1))
plus_one = Transformer.start(plus_one)
flow = %{numbers: [1, 2, 3, 4]}
new_flow = Transformer.call(flow, plus_one)
new_flow[:numbers] |> Enum.to_list()

# Stateful Transformer
sum_pairs = Transformer.new(
  :numbers,
  fn event, acc ->
    case acc do
      nil ->
        {[], event}
      number ->
        {[number + event], nil}
    end
  end
) |> Transformer.start()

new_flow = flow |> Transformer.call(plus_one) |> Transformer.call(sum_pairs)
new_flow[:numbers] |> Enum.to_list()

# Mixer
flow = %{num1: [1, 2, 3], num2: [4, 5, 6]}
mixer = Mixer.new([:num1, :num2], :numbers) |> Mixer.start()
new_flow = flow |> Mixer.call(mixer) |> Transformer.call(plus_one)
new_flow[:numbers] |> Enum.to_list()

# Composite
plus_one = Transformer.new(:numbers, &(&1 + 1))
mixer = Mixer.new([:num1, :num2], :numbers)
composite = Composite.new([mixer, plus_one]) |> Composite.start()
flow = %{num1: [1, 2, 3], num2: [4, 5, 6]}
new_flow = flow |> Composite.call(composite)
new_flow[:numbers] |> Enum.to_list()

# Complex composite
plus_one = Transformer.new(:numbers, &(&1 + 1))
mixer = Mixer.new([:num1, :num2], :numbers)
composite1 = Composite.new([mixer, plus_one])

sum_pairs = Transformer.new(
  :numbers,
  fn event, acc ->
    case acc do
      nil ->
        {[], event}
      number ->
        {[number + event], nil}
    end
  end
)

composite2 = Composite.new([sum_pairs])

composite = Composite.new([composite1, composite2]) |> Composite.start()

flow = %{num1: [1, 2, 3], num2: [4, 5, 6]}
new_flow = flow |> Composite.call(composite)
new_flow[:numbers] |> Enum.to_list()