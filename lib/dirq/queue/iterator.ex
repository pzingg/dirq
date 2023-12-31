defmodule Dirq.Queue.Iterator do
  @moduledoc """
  Holds the directory and name of the last element seen
  during an iteration.
  """

  @never_seen {"00000000", "00000000000000"}

  defstruct queue: nil,
            last_seen: @never_seen

  alias Dirq.Queue
  alias __MODULE__

  @type t() :: %__MODULE__{
          queue: Queue.t(),
          last_seen: {String.t(), String.t()}
        }

  @type value() :: [elem :: binary()]
  @type next() :: {value() | :halt, t()}

  @doc false
  @spec new(Queue.t()) :: t()
  def new(%Queue{} = queue) do
    %__MODULE__{queue: queue}
  end

  @doc false
  @spec new?(t()) :: boolean()
  def new?(%Iterator{last_seen: last_seen}), do: last_seen == @never_seen

  @doc false
  @spec next_elem(t()) :: next()
  def next_elem(%Iterator{queue: %Queue{path: qpath}, last_seen: {last_dir, last_file}} = iter) do
    dirs = Queue.get_intermediate_dirs(qpath, last_dir) |> Enum.sort()

    case dirs do
      [] ->
        {:halt, iter}

      [next_dir | rest] ->
        next_file = Path.join(qpath, next_dir) |> Queue.get_next_element_file(last_file)

        if is_nil(next_file) do
          try_next_dir(iter, rest)
        else
          {[Path.join(next_dir, next_file)], %Iterator{iter | last_seen: {next_dir, next_file}}}
        end
    end
  end

  defp try_next_dir(iter, []), do: {:halt, iter}

  defp try_next_dir(%Iterator{queue: %Queue{path: qpath}} = iter, [next_dir | _]) do
    next_file = Path.join(qpath, next_dir) |> Queue.get_next_element_file()

    if is_nil(next_file) do
      {:halt, iter}
    else
      {[Path.join(next_dir, next_file)], %Iterator{iter | last_seen: {next_dir, next_file}}}
    end
  end
end
