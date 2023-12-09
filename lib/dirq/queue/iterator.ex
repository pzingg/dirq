defmodule Dirq.Queue.Iterator do
  @moduledoc """
  Holds the directory and name of the last element seen
  during an iteration.
  """

  defstruct queue: nil,
            last_seen: {"00000000", "00000000000000"}

  alias Dirq.Queue
  alias __MODULE__

  @type t() :: %__MODULE__{
          queue: Queue.t(),
          last_seen: {String.t(), String.t()}
        }

  @element_regexp ~r/^[0-9a-f]{14}$/

  @doc false
  def new(%Queue{} = queue) do
    %__MODULE__{queue: queue}
  end

  @doc false
  def next_elem(%Iterator{queue: %Queue{path: qpath}, last_seen: {last_dir, last_file}} = iter) do
    case Queue.get_list_of_interm_dirs(qpath, last_dir) |> Enum.sort() do
      [] ->
        {:halt, iter}

      [next_dir | rest] ->
        path = Path.join(qpath, next_dir)

        files =
          case File.ls(path) do
            {:ok, files} ->
              Enum.filter(files, fn file ->
                Regex.match?(@element_regexp, file) && file > last_file
              end)
              |> Enum.sort()

            _ ->
              []
          end

        case {files, rest} do
          {[next_file | _], _} ->
            {[Path.join(next_dir, next_file)], %Iterator{iter | last_seen: {next_dir, next_file}}}

          {[], []} ->
            {:halt, iter}

          {[], [next_dir | _]} ->
            path = Path.join(qpath, next_dir)

            files =
              case File.ls(path) do
                {:ok, files} ->
                  Enum.filter(files, &Regex.match?(@element_regexp, &1))
                  |> Enum.sort()

                _ ->
                  []
              end

            case files do
              [next_file | _] ->
                {[Path.join(next_dir, next_file)],
                 %Iterator{iter | last_seen: {next_dir, next_file}}}

              [] ->
                {:halt, iter}
            end
        end
    end
  end
end
