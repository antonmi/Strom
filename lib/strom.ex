defmodule Strom do
  @moduledoc "Strom is a framework for building stream processing applications."

  @type event() :: any()
  @type stream_name() :: any()
  @type component() :: struct()
  @type stream() :: Enumerable.t(event())
  @type flow() :: %{optional(stream_name()) => stream()}
end
