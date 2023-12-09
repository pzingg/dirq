# Dirq

**TODO: Add description**

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `dirq` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:dirq, "~> 0.1.0"}
  ]
end
```

## Usage

Simple producer

```
queue = Queue.new("/tmp/test")
Enum.each(1..100, fn i ->
  name = Queue.add(queue, "element #{i}")
  IO.puts("added element #{i} as #{name}")
end)
```

Simple consumer

```
Queue.iterate(queue) |> Enum.each(fn name ->
  if Queue.lock(queue, name) do
    IO.puts("reading element #{name}")
    Queue.get(queue, name) |> IO.puts()
    Queue.remove(queue, name)
    # Or use Queue.unlock(queue, name) to peek at data
  end
end)
```