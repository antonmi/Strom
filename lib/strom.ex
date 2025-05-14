defmodule Strom do
  @moduledoc false

  @type event() :: any()
  @type stream_name() :: any()
  @type component() :: struct()
  @type stream() :: Enumerable.t(event())
  @type flow() :: %{optional(stream_name()) => stream()}
end
