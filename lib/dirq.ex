defmodule Dirq do
  @moduledoc false

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
end
