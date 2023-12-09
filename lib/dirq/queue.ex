defmodule Dirq.Queue do
  @moduledoc """
  Directory based queue.

  A port of Perl module Directory::Queue::Simple
  http://search.cpan.org/dist/Directory-Queue/

  The goal of this module is to offer a simple queue system using the
  underlying filesystem for storage, security and to prevent race
  conditions via atomic operations. It focuses on simplicity, robustness
  and scalability.

  This module allows multiple concurrent readers and writers to interact
  with the same queue.

  ## Terminology

  An element is something that contains one or more pieces of (String or byte)
  data.

  A queue is a "best effort FIFO" collection of elements.

  It is very hard to guarantee pure FIFO behavior with multiple writers
  using the same queue. Consider for instance:

  1. Writer1: calls the `add/3` function
  2. Writer2: calls the `add/3` function
  3. Writer2: the `add/3` function returns
  4. Writer1: the `add/3` function returns

  Who should be first in the queue, Writer1 or Writer2?

  For simplicity, this implementation provides only "best effort FIFO",
  i.e. there is a very high probability that elements are processed in
  FIFO order but this is not guaranteed. This is achieved by using a
  high-resolution time function and having elements sorted by the time
  the element's final directory gets created.

  ## Data modes

  The data mode defines how user supplied data is stored in the queue. It is
  only required by the `add/3` and `get/3` functions.

  The data modes are:
    * `:binary` - the data is a sequence of binary bytes, it will be stored directly
      in a plain file with no further encoding.
    * `:utf8` - the data is a text string (i.e. a sequence of characters), it will
      be UTF-8 encoded.
    * `:json` - the data is a value to be JSON encoded before writing to the file,
      and will be JSON decoded, with string keys, when getting the file.
    * `:json_atoms` - the data is a value to be JSON encoded before writing to the file,
      and will be JSON decoded, with atom keys, when getting the file.

  ## Locking

  Adding an element is not a problem because the `add/3` function is atomic.

  In order to support multiple processes interacting with the same queue,
  advisory locking is used. Processes should first lock an element before
  working with it. In fact, the `get/2` and `remove/2` functions raise an
  exception if they are called on unlocked elements.

  If the process that created the lock dies without unlocking the element,
  we end up with a stale lock. The `purge/2` function can be used to
  remove these stale locks.

  An element can basically be in only one of two states: locked or
  unlocked.

  A newly created element is unlocked as a writer usually does not need
  to do anything more with the element once dropped in the queue.

  The `Stream` iterator, created by the `iterate/1` function, returns
  all the elements, regardless of their states.

  There is no function to get an element state as this information is
  usually useless since it may change at any time. Instead, programs should
  directly try to lock elements to make sure they are indeed locked.

  ## Directory Structure

  The top-level directory contains intermediate directories that contain
  the stored elements, each of them in a file.

  The names of the intermediate directories are time based: the element
  insertion time is used to create a 8-digit-long hexadecimal number.
  The granularity (see `new/2`) is used to limit the number of
  new directories. For instance, with a granularity of 60 (the default),
  new directories will be created at most once per minute.

  Since there is usually a filesystem limit in the number of directories
  a directory can hold, there is a trade-off to be made. If you want to
  support many added elements per second, you should use a low
  granularity to keep small directories. However, in this case, you will
  create many directories and this will limit the total number of
  elements you can store.

  The elements themselves are stored in files (one per element) with a
  14-digit-long hexadecimal name SSSSSSSSMMMMMR where:

    * SSSSSSSS represents the number of seconds since the Epoch
    * MMMMM represents the microsecond part of the time since the Epoch
    * R is a random digit used to reduce name collisions

  A temporary element (being added to the queue) will have a ".tmp"
  suffix.

  A locked element will have a hard link with the same name and the
  ".lck" suffix.

  ## Security

  There are no specific security mechanisms in this module.

  The elements are stored as plain files and directories. The filesystem
  security features (owner, group, permissions, ACLs...) should be used
  to adequately protect the data.

  By default, the process' umask is respected. See the documentation
  for `new/2` if you want an other behavior.

  If multiple readers and writers with different uids are expected, the
  easiest solution is to have all the files and directories inside the
  top-level directory world-writable (i.e. umask=0). Then, the permissions
  of the top-level directory itself (e.g. group-writable) are enough to
  control who can access the queue.
  """

  require Logger

  alias Dirq.QueueLockError
  alias Dirq.Queue.Iterator
  alias __MODULE__

  @volatile_file_suffix_regexp ~r/(\.tmp|\.lck)$/
  @directory_regexp ~r/^[0-9a-f]{8}$/
  @element_regexp ~r/^[0-9a-f]{14}$/

  defstruct id: nil,
            path: nil,
            umask: nil,
            rndhex: nil,
            granularity: nil

  @type t() :: %__MODULE__{
          id: String.t(),
          path: String.t(),
          umask: integer() | nil,
          rndhex: integer(),
          granularity: integer()
        }

  @type data_mode() :: :json | :json_atoms | :utf8 | :binary

  @type config_opt() :: {:umask, integer()} | {:rndhex, integer()} | {:granulary, integer()}
  @type config() :: [config_opt()]

  @type purge_opt() :: {:maxtemp, integer()} | {:maxlock, integer()}
  @type purge_opts() :: [purge_opt()]

  @doc """
  Creates a new directory-based queue within the `path` top-level directory.

  Options:
    * `umask` - the umask to use when creating files and directories
      (default: use the running process' umask)
    * `rndhex` - the hexadecimal digit (an integer in the range 0..15) to
      use in names (default: randomly chosen)
    * `granularity` - the time granularity for intermediate directories
      (default: 60)

  Raises:
    * `ArgumentError` - wrong types of options
    * `File.Error` - if stats for the `path` directory cannot be read.
  """
  @spec new(binary(), config()) :: t()
  def new(path, opts \\ []) do
    umask = Keyword.get(opts, :umask)
    rndhex = Keyword.get(opts, :rndhex)
    granularity = Keyword.get(opts, :granularity, 60)

    rndhex =
      cond do
        is_nil(rndhex) ->
          :rand.uniform(16) - 1

        is_integer(rndhex) && rndhex >= 0 ->
          rem(rndhex, 16)

        true ->
          raise ArgumentError, "rndhex must be an integer"
      end

    # Create top-level directory
    _ = special_mkdir(path, umask)

    # Generate the queue unique identifier
    # Note: stat.minor_device is normally 0
    id =
      case File.stat(path) do
        {:ok, %{major_device: device, inode: inode}} ->
          "#{device}:#{inode}"

        {:error, reason} ->
          raise File.Error,
            reason: reason,
            action: "read file stats",
            path: IO.chardata_to_string(path)
      end

    %Queue{id: id, path: path, umask: umask, rndhex: rndhex, granularity: granularity}
  end

  @doc """
  Adds data to the queue as a file.

  Arguments:
    * `data` - binary data to write
    * `format` - `:json`, `:json_atoms`, `:utf8`, or `:binary`

  Returns the element name (<directory name>/<file name>).
  """
  @spec add(t(), any(), data_mode()) :: binary()
  def add(%Queue{} = queue, data, format \\ :utf8) do
    {dir, path} = add_data(queue, data, format)
    add_path(queue, path, dir)
  end

  @doc """
  Creates a new element in the queue and writes `data` to it.

  Arguments:
    * `data` - binary data to write
    * `format` - `:utf8`, `:json`, or `:binary`

  Returns a 2-tuple of the directory name where the file was written,
  and the full path to the temporary file.

  Raises:
    * `File.Error`
  """
  @spec add_data(t(), any(), data_mode()) :: {binary(), binary()}
  def add_data(%Queue{path: qpath, rndhex: rndhex, umask: umask} = queue, data, format \\ :utf8) do
    dir = new_dir_name(queue)
    name = new_elem_name(rndhex)
    path = Path.join([qpath, dir, name <> ".tmp"])

    case file_write(path, umask, data, format) do
      {:error, reason} ->
        raise File.Error,
          reason: reason,
          action: "write to file",
          path: IO.chardata_to_string(path)

      _ ->
        {dir, path}
    end
  end

  @doc """
  Adds the given file (identified by its path) to the queue and return
  the corresponding element name, the file must be on the same
  filesystem and will be moved to the queue.
  """
  @spec add_path(t(), binary()) :: binary()
  def add_path(%Queue{path: qpath, umask: umask} = queue, path) do
    dir = new_dir_name(queue)
    _ = Path.join(qpath, dir) |> special_mkdir(umask)
    add_path(queue, path, dir)
  end

  @doc """
  Given temporary file and directory where it resides: creates a hard
  link to that file and removes the temporary one.

  Returns element name (<directory name>/<file name>).
  """
  @spec add_path(t(), binary(), binary()) :: binary()
  def add_path(%Queue{path: qpath, rndhex: rndhex} = queue, tmp, dir) do
    name = new_elem_name(rndhex)
    path = Path.join([qpath, dir, name])

    case File.ln(tmp, path) do
      :ok ->
        _ = File.rm(tmp)
        Path.join(dir, name)

      {:error, :eexist} ->
        # Try again
        add_path(queue, tmp, dir)

      {:error, reason} ->
        raise File.LinkError,
          reason: reason,
          action: "create hard link",
          existing: IO.chardata_to_string(tmp),
          new: IO.chardata_to_string(path)
    end
  end

  @doc """
  Returns the number of elements in the queue, locked or not
  (but not temporary).
  """
  @spec count(t()) :: integer()
  def count(%Queue{path: qpath}) do
    # get list of intermediate directories
    get_list_of_interm_dirs(qpath)
    |> Enum.reduce(0, fn name, acc ->
      # count elements in sub-directories
      path = Path.join(qpath, name)

      files =
        case File.ls(path) do
          {:ok, files} ->
            Enum.filter(files, &Regex.match?(@element_regexp, &1))

          _ ->
            []
        end

      acc + Enum.count(files)
    end)
  end

  @doc """
  Returns the path of the locked element given the name.
  """
  @spec get_path(t(), binary()) :: binary()
  def get_path(%Queue{path: qpath}, name) do
    Path.join(qpath, name <> ".lck")
  end

  @doc """
  Returns the content of a locked element.

  Arguments:
    * `name` - name of an element
    * `format` - `:json`, `:json_atoms`, `:utf8`, or `:binary`

  Raises:
    * `File.Error` - problems opening, closing, or reading file
  """
  @spec get(t(), binary(), data_mode()) :: any()
  def get(%Queue{} = queue, name, format \\ :utf8) do
    get_path(queue, name)
    |> file_read(format)
  end

  @doc """
  Locks, then returns the content of an element from the queue.
  Element will not be removed. Operations performed: lock(queue, name),
  get(queue, name), unlock(queue, name)

  Arguments:
    * `name` - name of an element
    * `permissive` - work in permissive mode

  Raises:
    * `QueueLockError` - couldn't lock element
  """
  @spec get_element(t(), binary(), boolean()) :: any()
  def get_element(%Queue{} = queue, name, permissive \\ false) do
    if !lock(queue, name, permissive) do
      raise QueueLockError, "Couldn't lock element #{name}"
    end

    data = get(queue, name)
    _ = unlock(queue, name, permissive)
    data
  end

  @doc """
  Dequeues an element from the queue. Performs operations:
  `lock(queue, name)`, `get(queue, name)`, `remove(queue, name)`

  Arguments:
    * `name` - name of an element
    * `permissive` - work in permissive mode

  Returns the content of the element.

  Raises:
    * `QueueLockError` - couldn't lock element
    * `File.Error` - problems opening/closing directory/file
  """
  @spec dequeue(t(), binary(), boolean()) :: any()
  def(dequeue(%Queue{} = queue, name, permissive \\ true)) do
    if !lock(queue, name, permissive) do
      raise QueueLockError, "Couldn't lock element #{name}"
    end

    data = get(queue, name)
    _ = remove(queue, name)
    data
  end

  @doc """
  Locks an element.

  Arguments:
    * `name` - name of an element
    * `permissive` - work in permissive mode

  Returns:
    * `true` on success
    * `false` in case the element could not be locked (in permissive mode)

  Raises:
    * `File.Error` or `File.LinkError`
  """
  @spec lock(t(), binary(), boolean()) :: boolean()
  def lock(%Queue{path: qpath}, name, permissive \\ true) do
    elem_path = Path.join(qpath, name)

    with true <- hard_link(elem_path, permissive) do
      update_utime(elem_path, permissive)
    end
  end

  defp hard_link(elem_path, permissive) do
    case File.ln(elem_path, elem_path <> ".lck") do
      :ok ->
        true

      {:error, reason} ->
        if permissive && reason in [:eexist, :enoent] do
          false
        else
          raise File.LinkError,
            reason: reason,
            action: "create hard link",
            existing: elem_path,
            new: elem_path <> ".lck"
        end
    end
  end

  defp update_utime(elem_path, permissive) do
    case File.touch(elem_path) do
      :ok ->
        true

      {:error, reason} ->
        if permissive and reason == :enoent do
          # RACE: the element file does not exist anymore
          # (this can happen if an other process locked & removed the
          # element while our link() was in progress...
          # yes, this can happen!)
          _ = File.rm(elem_path <> ".lck")
          false
        else
          raise File.Error,
            reason: reason,
            action: "touch",
            path: IO.chardata_to_string(elem_path)
        end
    end
  end

  @doc """
  Unlocks an element.

  Arguments:
    * `name` - name of an element
    * `permissive` - work in permissive mode

  Returns:
    * `true` on success
    * `false` in case the element could not be unlocked (in permissive
      mode)

  Raises:
    * `File.Error`
  """
  @spec unlock(t(), binary(), boolean()) :: boolean()
  def unlock(%Queue{} = queue, name, permissive \\ false) do
    lock_path = get_path(queue, name)

    case File.rm(lock_path) do
      :ok ->
        true

      {:error, reason} ->
        if permissive && reason == :enoent do
          false
        else
          raise File.Error,
            reason: reason,
            action: "remove file",
            path: IO.chardata_to_string(lock_path)
        end
    end
  end

  @doc """
  Removes a locked element from the queue.
  """
  @spec remove(t(), binary()) :: :ok
  def remove(%Queue{path: qpath}, name) do
    elem_path = Path.join(qpath, name)
    _ = File.rm(elem_path)
    _ = File.rm(elem_path <> ".lck")
    :ok
  end

  @doc """
  Returns a `Stream` from the queue that can be enumerated.
  """
  @spec iterate(t()) :: Enumerable.t()
  def iterate(%Queue{} = queue) do
    Stream.resource(
      fn -> Iterator.new(queue) end,
      &Iterator.next_elem/1,
      fn _ -> :ok end
    )
  end

  @doc """
  Purges the queue by removing unused intermediate directories,
  removing too old temporary elements and unlocking too old locked
  elements (aka stale locks). Note: this can take a long time on
  queues with many elements.

  Options:
    * `maxtemp` - maximum time for a temporary element
            (in seconds, default 300);
            if set to 0, temporary elements will not be removed
    * `maxlock` - maximum time for a locked element
            (in seconds, default 600);
            if set to 0, locked elements will not be unlocked
  """
  @spec purge(t(), purge_opts()) :: :ok
  def purge(%Queue{path: qpath}, opts \\ []) do
    # get list of intermediate directories
    dirs = get_list_of_interm_dirs(qpath)

    now = System.os_time(:second)
    maxtemp = Keyword.get(opts, :maxtemp, 300)
    maxlock = Keyword.get(opts, :maxlock, 600)

    oldtemp =
      if maxtemp > 0 do
        now - maxtemp
      else
        0
      end

    oldlock =
      if maxlock > 0 do
        now - maxlock
      else
        0
      end

    remove_stale_elements(qpath, dirs, oldtemp, oldlock)
    purge_empty_directories(qpath, dirs)
  end

  defp remove_stale_elements(_, _, 0, 0), do: :ok

  defp remove_stale_elements(qpath, dirs, oldtemp, oldlock) do
    # Remove stale temporary or locked elements
    Enum.each(dirs, fn dir ->
      path = Path.join(qpath, dir)

      temp_lock_elems =
        case File.ls(path) do
          {:ok, files} ->
            Enum.filter(files, &Regex.match?(@volatile_file_suffix_regexp, &1))

          _ ->
            []
        end

      Enum.each(temp_lock_elems, &remove_stale(path, &1, oldtemp, oldlock))
    end)

    :ok
  end

  defp remove_stale(path, old, oldtemp, oldlock) do
    old_path = Path.join(path, old)

    case File.stat(old_path, time: :posix) do
      {:error, :enoent} ->
        :ok

      {:error, reason} ->
        raise File.Error,
          reason: reason,
          action: "read file stats",
          path: IO.chardata_to_string(old_path)

      {:ok, %{mtime: mtime}} ->
        if (String.ends_with?(old, ".tmp") && mtime < oldtemp) ||
             (String.ends_with?(old, ".lck") && mtime < oldlock) do
          rm_existing(old_path)
        end
    end
  end

  defp rm_existing(path) do
    Logger.debug("purge: removing stale volatile file: #{path}")

    case File.rm(path) do
      {:error, :enoent} ->
        :ok

      {:error, reason} ->
        raise File.Error,
          reason: reason,
          action: "remove file",
          path: IO.chardata_to_string(path)

      :ok ->
        :ok
    end
  end

  defp purge_empty_directories(qpath, dirs) do
    # Try to remove all but the last intermediate directory
    Enum.sort(dirs)
    |> Enum.reverse()
    |> Enum.drop(1)
    |> Enum.each(fn dir ->
      path = Path.join(qpath, dir)

      case File.ls(path) do
        {:ok, []} ->
          _ = special_rmdir(path)
          :ok

        {:ok, _files} ->
          Logger.debug("purge: directory #{path} is not empty")
          :ok

        _error ->
          :ok
      end
    end)

    :ok
  end

  @doc """
  Touch an element directory to indicate that it is still being used.

  Note: This is only really useful for locked elements but we allow it
    for all.

  Raises:
    File.Error
  """
  @spec touch(t(), binary()) :: :ok
  def touch(%Queue{path: qpath}, name) do
    Path.join(qpath, name)
    |> File.touch!()
  end

  @doc false
  def get_list_of_interm_dirs(qpath, from \\ nil)

  def get_list_of_interm_dirs(qpath, nil) do
    # Get names of intermediate directories.
    case File.ls(qpath) do
      {:ok, files} ->
        Enum.filter(files, &Regex.match?(@directory_regexp, &1))

      _ ->
        []
    end
  end

  def get_list_of_interm_dirs(qpath, from) do
    case File.ls(qpath) do
      {:ok, files} ->
        Enum.filter(files, fn file ->
          Regex.match?(@directory_regexp, file) && file >= from
        end)

      _ ->
        []
    end
  end

  @doc false
  def to_hex(i, len) when is_integer(i) and is_integer(len) do
    s = Integer.to_string(i, 16)

    if len > 0 do
      if String.length(s) > len do
        raise ArgumentError, "to_hex(#{i}, #{len}): length overflow"
      else
        String.pad_leading(s, len, "0") |> String.downcase()
      end
    else
      String.downcase(s)
    end
  end

  defp new_elem_name(rndhex) do
    # Returns the name of a new element to (try to) use with:
    #
    # * 8 hexadecimal digits for the number of seconds since the Epoch
    # * 5 hexadecimal digits for the microseconds part
    # * 1 hexadecimal digit from the pid to further reduce name collisions
    #
    # Properties:
    # * fixed size (14 hexadecimal digits)
    # * likely to be unique (with high-probability)
    # * can be lexically sorted
    # * ever increasing (for a given process)
    # * reasonably compact
    # * matching `@element_regexp`
    now = System.os_time(:microsecond)
    secs = div(now, 1_000_000) |> to_hex(8)
    msecs = rem(now, 1_000_000) |> to_hex(5)
    secs <> msecs <> to_hex(rndhex, 1)
  end

  defp new_dir_name(%Queue{granularity: granularity}) do
    # Returns new directory name based on time and granularity.
    now = System.os_time(:second)

    now =
      if granularity > 1 do
        now - rem(now, granularity)
      else
        now
      end

    to_hex(now, 8)
  end

  defp file_read(path, format) do
    # Reads from a file.
    #
    # Raises:
    #   * `File.Error` - problems opening, closing, or reading file
    #   * `Jason.Error`
    modes =
      case format do
        :binary -> [:read]
        _ -> [:read, :utf8]
      end

    case File.open(path, modes) do
      {:error, reason} ->
        raise File.Error, reason: reason, action: "open", path: IO.chardata_to_string(path)

      {:ok, h} ->
        data = read_and_decode(h, format, modes)

        case File.close(h) do
          {:error, reason} ->
            raise File.Error, reason: reason, action: "close", path: IO.chardata_to_string(path)

          :ok ->
            data
        end
    end
  end

  def read_and_decode(h, format, modes) do
    data =
      if :utf8 in modes do
        IO.read(h, :eof)
      else
        IO.binread(h, :eof)
      end

    case {data, format} do
      {{:error, reason}, _} ->
        raise File.Error,
          reason: reason,
          action: "read file",
          path: IO.chardata_to_string(path)

      {:eof, _} ->
        nil

      {_, :json} ->
        Jason.decode!(data)

      {_, :json_atoms} ->
        Jason.decode!(data, keys: :atoms)

      _ ->
        data
    end
  end

  defp file_write(path, umask, data, format) do
    # Creates, writes to, then closes a file. Creates subdirectories if
    # necessary.
    #
    # Returns {:ok, _pid} or {:error, action, reason}
    #
    # Raises:
    #   * `File.Error` if file cannot be opened or if
    #     subdirectories cannot be created.
    res =
      case file_create(path, umask, format) do
        {:error, :enoent} ->
          _ = Path.dirname(path) |> special_mkdir(umask)
          file_create(path, umask, format)

        {:error, reason} ->
          raise File.Error,
            reason: reason,
            action: "create",
            path: IO.chardata_to_string(path)

        {:ok, h} ->
          {:ok, h}
      end

    {data, format} =
      if format in [:json, :json_atoms] do
        {Jason.encode!(data), :utf8}
      else
        {data, format}
      end

    case {res, format} do
      {{:ok, h}, :binary} ->
        res = IO.binwrite(h, data)
        _ = File.close(h)
        res

      {{:ok, h}, _} ->
        res = IO.write(h, data)
        _ = File.close(h)
        res

      {{:error, reason}, _} ->
        {:error, reason}
    end
  end

  defp file_create(path, umask, format) do
    # Opens a new file defined by 'path', returning the device handle or error tuple.
    modes =
      case format do
        :binary -> [:write, :exclusive]
        _ -> [:write, :exclusive, :utf8]
      end

    if is_nil(umask) do
      File.open(path, modes)
    else
      old_umask = :perc.umask(umask)
      result = File.open(path, modes)
      :perc.umask(old_umask)
      result
    end
  end

  defp special_mkdir(path, umask) do
    # Recursively create directories specified in path.
    #   * returns true on success
    #   * returns false if the directory already exists
    #   * raises `File.Error` in case of any other error
    {result, reason} =
      if is_nil(umask) do
        wrapped_mkdir_p(path)
      else
        old_umask = :perc.umask(umask)
        {result, reason} = wrapped_mkdir_p(path)
        :perc.umask(old_umask)
        {result, reason}
      end

    if is_nil(reason) do
      result
    else
      raise File.Error,
        reason: reason,
        action: "make directory (with -p)",
        path: IO.chardata_to_string(path)
    end
  end

  defp wrapped_mkdir_p(path) do
    # Wrapped `File.mkdir_p/1` used by `special_mkdir/1`
    case File.mkdir_p(path) do
      :ok ->
        {true, nil}

      {:error, :eexist} ->
        {false, nil}

      {:error, :enotdir} ->
        {false, nil}

      {:error, reason} ->
        {false, reason}
    end
  end

  defp special_rmdir(path) do
    # Delete a directory:
    # * returns true on success
    # * returns false if the path does not exist (anymore)
    # * raises `File.Error` in case of any other error
    case File.rmdir(path) do
      :ok ->
        true

      {:error, :enoent} ->
        false

      {:error, reason} ->
        raise File.Error,
          reason: reason,
          action: "remove directory",
          path: IO.chardata_to_string(path)
    end
  end
end
