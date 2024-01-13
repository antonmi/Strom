# Strom

## Flow-based Programming Framework

Strom provides a set of abstractions for creating, routing and modifying streams of data.

### Data
The data abstractions are:

#### Event
Any piece of data - number, string, list, map, struct, etc.

#### Stream
A sequence (can be infinite) of events made available over time.

See [Elixir Stream](https://hexdocs.pm/elixir/1.15/Stream.html).

#### Flow
Flow - is a named set of streams.

For example:
```elixir
flow = %{stream1: Stream.cycle([1, 2, 3]), stream2: ["a", "b", "c"]}
```
Flow can be empty - `%{}`.


### Operators (functions)
There are several operators (functions) that can be applied to flows.
Each operator accept flow as input and return a modified flow.


#### Source (source)
Adds a stream of "external data" to a flow. 
```elixir
%{} -> source(Src, :foo) -> %{foo: sfoo} 
%{bar: Sbar} -> source(Src, :foo) -> %{foo: sfoo, bar: sbar} 
```

#### Sink (sink)
Writes a stream data back to somewhere.
```elixir
%{foo: sfoo} -> sink(Snk, :foo) -> %{} 
%{foo: sfoo, bar: sbar} -> sink(Snk, :foo) -> %{bar: sbar} 
```

#### Mixer (mix)
Mixes several streams.
```elixir
%{foo: sfoo, bar: sbar} -> mix([:foo, :bar], :mixed) -> %{mixed: smixed} 
```

#### Splitter (split)
Split a stream into several streams.
```elixir
%{foo: sfoo} -> split(:foo, [:bar, :baz]) -> %{bar: sbar, baz: sbaz} 
```

#### Transformer (transform)
Applies a function to each event of a stream or streams.

```elixir
%{foo: sfoo, bar: sbar} -> transform(:foo, F) -> %{foo: F(sfoo)} 
%{foo: sfoo, bar: sbar} -> transform([:foo, :bar], F) -> %{foo: F(sfoo), bar: F(sbar} 
```

A function gets an event as input and must return a modified event.
So, it's the map operation. Think about &Stream.map/2, which is used under the hood.


### Symbolic representation

<img src="images/components.png" alt="Implicit components" width="500"/>

### Implementation details and interface

Under the hood, each operation is performed inside "components".
Component is a separate process - GenServer.

A component can be:
- build - `new/2`, `new/3`
- started - `start/1`
- stopped - `stop/1` 
- and called - `call/2`

#### Example
Let's say one wants to stream a file:

```elixir
alias Strom.Source
alias Strom.Source.ReadLines

source = 
  :lines
  |> Source.new(%ReadLines{path: "input.txt"})
  |> Source.start() 
  
%{lines: stream} = Source.call(%{}, source)
# adds the :lines stream to the empty flow (%{})

Enum.to_list(stream) 
# runs the stream and returns a list of strings

Source.stop(source)
# stops the source process
```

Here the `Strom.Source.ReadLines` module is used to read line from file.

To specify a custom source, one can implement a module with the `Strom.Source` behaviour.

Strom provides a couple of simple sources, see [sources](https://github.com/antonmi/Strom/blob/main/lib/source/).

The same for sinks.

Then, for example, one wants to split the stream into two streams, one with short lines, another - with long ones:

```elixir
alias Strom.Splitter

parts = %{
  long: &(String.length(&1) > 3),
  short: &(String.length(&1) <= 3)
}

splitter = 
    :lines
    |> Splitter.new(parts)
    |> Splitter.start()

stream = ["Hey", "World"]

%{long: long, short: short} = Splitter.call(%{lines: stream}, splitter)

# Splits the :lines stream into the :long and :short streams based on rules defined in parts
```

And then, one wants to save the streams into two files:

```elixir
alias Strom.Sink
alias Strom.Sink.WriteLines

sink_short = 
  :short
  |> Sink.new(%WriteLines{path: "short.txt"})
  |> Sink.start()

sink_long = 
  :long
  |> Sink.new(%WriteLines{path: "long.txt"})
  |> Sink.start()

%{} = 
  %{long: long, short: short}
  |> Sink.call(sink_short)
  |> Sink.call(sink_long)  
```

#### Transformer
With the Function component everything is straightforward.
Let's calculate the length of each string and produce a stream of numbers:

```elixir
alias Strom.Transformer

function = &String.length(&1)

transformer = 
  :stream
  |> Transformer.new(function)
  |> Transformer.start()

%{stream: stream} = Transformer.call(%{stream: ["Hey", "World"]} , transformer)

# now the stream is the stream of numbers
```

The function can be applied to several steams simultaneously:

```elixir
transformer = 
  [:short, :long]
  |> Transformer.new(function)
  |> Transformer.start()
  
flow = %{short: ["Hey"], long: ["World"]}  
%{short: short, long: long} = Transformer.call(flow, transformer)
```

Transformer can operate 2-arity functions with accumulator.

The function must return 2-elements tuple.
```elixir
{list(event), acc}
```
The first element is a list of events that will be returned from the component.
The second is a new accumulator.

```elixir
alias Strom.Transformer

function = fn event, acc ->
  {[event * acc, :new_event], acc + 1}   
end

transformer = :events |> Transformer.new(function, 0) |> Transformer.start()

%{events: stream} = Transformer.call(%{events: [1, 2, 3]}, transformer)

Enum.to_list(stream)
# returns
[0, :new_event, 2, :new_event, 6, :new_event]
```

### Strom.Composite and Strom.DSL

Since the operations have a similar interface and behaviour, it's possible to compose them.

See [composite_test.exs](https://github.com/antonmi/Strom/blob/main/test/composite_test.exs).

See more examples in tests.