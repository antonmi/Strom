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
- started - `start/0`, `start/1`
- stopped - `stop/1` 
- and called - `call/3` and `call/4`

#### Example
Let's say one wants to stream a file:

```elixir
alias Strom.Source
alias Strom.Source.ReadLines

source = Source.start(%ReadLines{path: "input.txt"}) 
# returns a struct %Source{pid: pid, origin: %ReadLines{}}

%{lines: stream} = Source.call(%{}, source, :lines)
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

splitter = Splitter.start()
# starts the splitter process

parts = %{
  long: fn event -> String.length(event) > 100 end,
  short: fn event -> String.length(event) <= 100 end
}

%{long: long, short: short} = 
  Splitter.call(%{lines: stream}, splitter, :lines, parts)

# Splits the :lines stream into the :long and :short streams based on rules defined in parts  
```

And then, one wants to save the streams into two files:

```elixir
alias Strom.Sink

sink_short = Sink.start(%WriteLines{path: "short.txt"})
sink_long = Sink.start(%WriteLines{path: "long.txt"})

%{} =
  %{long: long, short: short}
  |> Sink.call(sink_short, :short)
  |> Sink.call(sink_long, :long, true)
  
# the first sink will run the stream aynchronously (using the Elixir Task)
# the second sink (see `true` as the last argument) runs the stream synchronously
```

#### Transformer
With the Function component everything is straightforward.
Let's calculate the length of each string and produce a stream of numbers:

```elixir
alias Strom.Transformer

transformer = Transformer.start()
function = &String.length(&1)
%{short: short} =
  %{short: short} 
  |> Transformer.call(transformer, :short, function)
# now the stream is the stream of numbers
```

The function can be applied to several steams simultaneously:

```elixir
%{short: short, long: long} =
  %{short: short, long: long}
  |> Transformer.call(transformer, [:short, :long], function)
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

transformer = Transformer.start()

%{events: stream} = Transformer.call(%{events: [1, 2, 3]}, transformer, :events, {function, 0})

Enum.to_list(stream)
# returns
[0, :new_event, 2, :new_event, 6, :new_event]
```

Let's consider the ["Telegram problem"](https://jpaulm.github.io/fbp/examples.html).

The program accepts a stream of strings and should produce another stream of string with the length less then a specified value.

The solution requires two components - decomposer and recomposer.
The first will split strings into words. The second will "recompose" words into new strings

The decomposer module is quite simple
```elixir
defmodule Decompose do
  def call(event, nil) do
    {String.split(event, ","), nil}
  end
end
```

The recomposer will store incoming words and when a line is ready, it will produce an event.
```elixir
defmodule Recompose do
    @length 100
    
    def call(event, words) do
    line = Enum.join(words, " ")
    new_line = line <> " " <> event
    
    if String.length(new_line) > @length do
      {[new_line], [event]}
    else
      {[], words ++ [event]}
    end
  end
end
```

See [telegram_test.exs](https://github.com/antonmi/Strom/blob/main/test/examples/telegram_test.exs)

It's also possible to parameterize the Transformer component by passing `opts` to its `start/1` function:

```elixir
alias Strom.Transformer

function = fn event, acc, opts ->
  {[event * acc], acc + opts[:inc]}   
end

transformer = Transformer.start(opts: %{inc: 1})

%{events: stream} = Transformer.call(%{events: [1, 2, 3]}, transformer, :events, {function, 0})

Enum.to_list(stream)
# returns
[0, 2, 6]
```


### Strom.DSL

Since the operations have a similar interface and behaviour, it's possible to define the topology of calculation in a simple declarative way.

Each component has a corresponding macro, see the [Strom.DSL](https://github.com/antonmi/Strom/blob/main/lib/dsl.ex) module and its tests.

The topology form the first examples (with long and short strings) can be defined like that:

```elixir
defmodule MyFlow do
  use Strom.DSL
  alias Strom.Source.ReadLines
  alias Strom.Sink.WriteLines
  
  def topology(_opts) do
    parts = %{
      long: fn event -> String.length(event) > 100 end,
      short: fn event -> String.length(event) <= 100 end
    }
    
    [
      source(:lines, %ReadLines{path: "input.txt"}),
      split(:lines, parts),
      sink(:short, %WriteLines{path: "short.txt"}),
      sink(:long, %WriteLines{path: "long.txt"})
    ] 
  end
end
```

See more examples in tests.