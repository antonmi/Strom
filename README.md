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


#### Source
Adds a stream of "external data" to a flow. 
```elixir
%{} -> source(Src, :foo) -> %{foo: sfoo} 
%{bar: Sbar} -> source(Src, :foo) -> %{foo: sfoo, bar: sbar} 
```

#### Sink
Writes a stream data back to somewhere.
```elixir
%{foo: sfoo} -> sink(Snk, :foo) -> %{} 
%{foo: sfoo, bar: sbar} -> sink(Snk, :foo) -> %{bar: sbar} 
```

#### Mixer
Mixes several streams.
```elixir
%{foo: sfoo, bar: sbar} -> mixer([:foo, :bar], :mixed) -> %{mixed: smixed} 
```

#### Splitter
Split a stream into several streams.
```elixir
%{foo: sfoo} -> splitter(:foo, [:bar, :baz]) -> %{bar: sbar, baz: sbaz} 
```

#### Function
Applies a function to each event of a stream or streams.

```elixir
%{foo: sfoo, bar: sbar} -> funtion(F, :foo) -> %{foo: F(sfoo)} 
%{foo: sfoo, bar: sbar} -> funtion(F, [:foo, :bar]) -> %{foo: F(sfoo), bar: F(sbar} 
```

A function gets an event as input and must return a modified event.
So, it's the map operation. Think about &Stream.map/2, which is used under the hood.

#### Module
Does almost the same as function, but allows to accumulate events and may return many events (or none).
Details are below.

### Symbolic representation

<img src="images/components.png" alt="Implicit components" width="500"/>

### Implementation details and interface

Under the hood, each operation is performed inside "components".
Component is a separate process - GenServer.

A component can be:
- started - `start/0`, `start/1`
- stopped - `stop/1` 
- and called - `call/3`, `call/4`

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

#### Function and Module
With the Function component everything is straightforward.
Let's calculate the length of each string and produce a stream of numbers:

```elixir
alias Strom.Function

function = Function.start(&String.length(&1))
%{short: short} =
  %{short: short} 
  |> Function.call(function, :short)
# now the stream is the stream of numbers
```

The function can be applied to several steams simultaneously:

```elixir
%{short: short, long: long} =
  %{short: short, long: long}
  |> Function.call(function, [:short, :long])
```

With a Module one can perform more complex transformations.

User's module must implement three functions:
- `start(opts)` - the function will be called when the corresponding component is starting. The function must return the initial accumulator, or `memo`. 
- `call(event, memo, acc)` - will be called on each event in a stream. Must return a tuple `{events, memo}`. `events` - is a list of new events (can be empty), `memo` is a new accumulated value.
- `stop(memo, opts)` - for clean-up actions, when the module component is stopped.

Let's consider the ["Telegram problem"](https://jpaulm.github.io/fbp/examples.html).

The program accepts a stream of strings and should produce another stream of string with the length less then a specified value.

The solution requires two components - decomposer and recomposer.
The first will split strings into words. The second will "recompose" words into new strings

The decomposer module is quite simple
```elixir
defmodule Decompose do
  def start([]), do: nil # we don't need memo in a decomposer

  def call(event, nil, []) do
    {String.split(event, " "), nil} # events are words
  end

  def stop(nil, []), do: :ok
end
```

The recomposer will store incoming words and when a line is ready, it will produce an event.
```elixir
defmodule Recompose do
  @length 30

  def start([]), do: [] # memo is an empty list initially

  def call(event, words, []) do # words is the memo here
    line = Enum.join(words, " ")
    new_line = line <> " " <> event

    if String.length(new_line) > @length do
      {[line], [event]} # produces a line and stores the extra word in memo
    else
      {[], words ++ [event]} # just collects more words in memo
    end
  end

  def stop(_acc, _memo), do: :ok
end
```

See [telegram_test.exs](https://github.com/antonmi/Strom/blob/main/test/examples/telegram_test.exs) 

### Strom.DSL

Since the operations have a similar interface and behaviour, it's possible to define the topology of calculation in a simple declarative way.

Each component has a corresponding macro, see the [Strom.DSL](https://github.com/antonmi/Strom/blob/main/lib/dsl.ex) module and its tests.

The topology form the first examples (with long and short strings) can be defined like that:

```elixir
defmodule MyFlow do
  use Strom.DSL
  
  


  def topology(_opts) do
    parts = %{
      long: fn event -> String.length(event) > 100 end,
      short: fn event -> String.length(event) <= 100 end
    }
    
    [
      source(:lines, %ReadLines{path: "input.txt"}),
      splitter(:lines, parts),
      sink(:short, %WriteLines{path: "short.txt"}),
      sink(:long, %WriteLines{path: "long.txt"})
    ] 
  end
end
```

See more examples in tests.

