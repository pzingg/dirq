defmodule Dirq do
  @moduledoc """
  Documentation for `Dirq`.
  """

  defmodule QueueError do
    defexception [:message]

    @impl true
    def exception(msg) do
      %__MODULE__{message: msg}
    end
  end

  defmodule QueueLockError do
    defexception [:message]

    @impl true
    def exception(msg) do
      %__MODULE__{message: msg}
    end
  end

  @doc """
  Hello world.

  ## Examples

      iex> Dirq.hello()
      :world

  """
  def hello do
    :world
  end
end
