defmodule Dirq.QueueSet do
  @moduledoc """
  Interface to elements on a set of directory based queues.
  """

  alias Dirq.{Queue, QueueError}
  alias Dirq.Queue.Iterator
  alias __MODULE__

  defstruct qset: []

  @type t() :: %__MODULE__{
          qset: [Queue.t()]
        }

  @doc """
  Creates a queue set on the given queues.

  Argument:
    * `queues` - a single `Dirq.Queue` or list of `Dirq.Queue`s.

  Raises:
    *  `ArgumentError` - one of objects provided is not a `Dirq.Queue`
  """
  @spec new(Queue.t() | [Queue.t()]) :: t()
  def new(queues) do
    %QueueSet{} |> do_add(queues)
  end

  @doc """
  Returns the number of elements in the queue set, regardless of
  their state.
  """
  @spec count(t()) :: integer()
  def count(%QueueSet{qset: s}) do
    s |> Enum.reduce(0, fn q, acc -> acc + Queue.count(q) end)
  end

  @doc """
  Adds queues or lists of queues to an existing queue set.

  Argument:
    * `queues` - a single `Dirq.Queue` or list of `Dirq.Queue`s.

  Raises:
    * `Dirq.QueueError` - queue already in the set
    * `ArgumentError` - wrong queue object type provided
  """
  @spec add(t(), Queue.t() | [Queue.t()]) :: t()
  def add(%QueueSet{} = queue_set, queues) do
    do_add(queue_set, queues)
  end

  @doc """
  Removes a queue and its respective elements.

  Argument:
    * `queue` - queue to be removed
  """
  @spec remove(t(), Queue.t()) :: t()
  def remove(%QueueSet{qset: s} = queue_set, %Queue{id: given_id, path: _path}) do
    %QueueSet{queue_set | qset: Enum.reject(s, fn %{id: id} -> id == given_id end)}
  end

  def remove(%QueueSet{}, _queue) do
    raise ArgumentError, "Queue object expected."
  end

  @doc """
  Creates a `Stream` that can be used to enumerate all the queue elements in
  the queue set.
  """
  @spec iterate(t()) :: Enumerable.t()
  def iterate(%QueueSet{qset: s}) do
    Stream.resource(
      fn -> Enum.map(s, fn q -> Iterator.new(q) |> Iterator.next_elem() end) end,
      &next_elem_in_set/1,
      fn _ -> :ok end
    )
  end

  defp next_elem_in_set([]), do: {:halt, :ok}

  defp next_elem_in_set(nexts) do
    # Iterator.next_elem/1 returns a tuple:
    # {[elem :: binary()] | :halt, Iterator.t()}
    nexts =
      Enum.reject(nexts, fn
        {:halt, _} -> true
        _ -> false
      end)

    if Enum.empty?(nexts) do
      {:halt, :ok}
    else
      {elems, low_iter} = Enum.min_by(nexts, fn {[elem], _iter} -> elem end)

      advance(nexts, elems, low_iter)
    end
  end

  defp advance(nexts, [elem], %Iterator{queue: %Queue{id: low_id} = queue} = low_iter) do
    nexts =
      Enum.map(nexts, fn {_elems, %Iterator{queue: %Queue{id: id}}} = next ->
        if id == low_id do
          Iterator.next_elem(low_iter)
        else
          next
        end
      end)

    {[{queue, elem}], nexts}
  end

  defp do_add(%QueueSet{} = queue_set, %Queue{} = queue) do
    do_add(queue_set, [queue])
  end

  defp do_add(%QueueSet{} = queue_set, queues) when is_list(queues) do
    # Add lists of queues to existing ones.
    queues
    |> Enum.map(&List.wrap/1)
    |> List.flatten()
    |> Enum.reduce(queue_set, fn
      %Queue{id: queue_id, path: path} = q, %QueueSet{qset: s} = acc ->
        existing_ids = Enum.map(s, fn %{id: id} -> id end)

        if queue_id in existing_ids do
          raise QueueError, "Queue already in the set: #{path}"
        else
          %QueueSet{acc | qset: s ++ [q]}
        end

      _, _acc ->
        raise ArgumentError, "Expected Queue objects expected"
    end)
  end
end
