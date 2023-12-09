defmodule QueueSetTest do
  use ExUnit.Case
  # doctest Dirq.QueueSet

  alias Dirq.{Queue, QueueError, QueueSet}

  setup do
    {:ok, tempdir} = Briefly.create(prefix: "dirq-set", directory: true)

    attrs = [
      {:tempdir, tempdir}
      | Enum.map(1..4, fn i -> {String.to_atom("q#{i}"), Path.join(tempdir, "q#{i}")} end)
    ]

    # on_exit(fn -> File.rm_rf(tempdir) end)
    Map.new(attrs)
  end

  def show(qdir) do
    {output, _} = System.cmd("ls", ["-lR", qdir])
    IO.puts(output)
  end

  test "add/remove", context do
    q1 = Queue.new(context.q1)
    q2 = Queue.new(context.q2)
    q3 = Queue.new(context.q3)
    q4 = Queue.new(context.q4)

    Enum.each(1..10, fn i ->
      Queue.add(q1, %{data: "q1-#{i}"}, :json_atoms)
      Queue.add(q2, %{data: "q2-#{i}"}, :json_atoms)
      Queue.add(q3, %{data: "q3-#{i}"}, :json_atoms)
      Queue.add(q4, %{data: "q4-#{i}"}, :json_atoms)
    end)

    queue_set = QueueSet.new([q1, q2])
    queue_set = QueueSet.add(queue_set, q3)
    queue_set = QueueSet.remove(queue_set, q1)
    queue_set = QueueSet.add(queue_set, [q1, q4])
    assert_raise(QueueError, fn -> QueueSet.add(queue_set, q1) end)
  end

  test "count and iterate", context do
    q1 = Queue.new(context.q1)
    q2 = Queue.new(context.q2)

    Enum.each(1..10, fn i ->
      Queue.add(q1, %{data: "q1-#{i}"}, :json_atoms)
      Queue.add(q2, %{data: "q2-#{i}"}, :json_atoms)
    end)

    queue_set = QueueSet.new([q1, q2])
    assert QueueSet.count(queue_set) == 20

    assert [{q, elem}] = QueueSet.iterate(queue_set) |> Enum.take(1)
    assert q == q1
    assert elem

    QueueSet.iterate(queue_set)
    |> Enum.reduce(0, fn {q, elem}, i ->
      Queue.lock(q, elem)
      data = Queue.get(q, elem, :json_atoms)
      Queue.unlock(q, elem)
      IO.puts("[#{i + 1}]: Queue id #{q.id} at #{elem}: #{inspect(data)}")

      if i == 10 do
        Queue.add(q1, %{data: "q1-Late"}, :json_atoms)
      end

      i + 1
    end)
  end
end
