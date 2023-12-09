# Dirq

An almost-FIFO, filesystem directory based queue, translated from 
the Perl library [Directory::Queue](http://search.cpan.org/dist/Directory-Queue/)

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

A simple producer:

```
queue = Dirq.Queue.new("/tmp/test")
Enum.each(1..100, fn i ->
  name = Dirq.Queue.add(queue, %{data: "element " <> to_string(i)}, :json_atoms)
  IO.puts("added element " <> to_string(i) <> " as " <> name)
end)
```

A simple consumer:

```
Dirq.Queue.iterate(queue) |> Enum.each(fn name ->
  if Dirq.Queue.lock(queue, name) do
    IO.puts("reading element " <> name)
    Dirq.Queue.get(queue, name, :json_atoms) |> IO.inspect()
    Dirq.Queue.remove(queue, name)
    # Or use Dirq.Queue.unlock(queue, name) to peek at data
  end
end)
```
