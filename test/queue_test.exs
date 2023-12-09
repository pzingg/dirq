defmodule QueueTest do
  use ExUnit.Case
  # doctest Dirq.Queue

  alias Dirq.Queue

  setup do
    {:ok, tempdir} = Briefly.create(prefix: "dirq-simple", directory: true)
    qdir = Path.join(tempdir, "dirq")
    on_exit(fn -> File.rm_rf(tempdir) end)
    %{tempdir: tempdir, qdir: qdir}
  end

  def show(qdir) do
    {output, _} = System.cmd("ls", ["-lR", qdir])
    IO.puts(output)
  end

  test "new/2", %{tempdir: tempdir} do
    path = Path.join(tempdir, "aaa/bbb/ccc")
    granularity = 30
    queue = Queue.new(path, granularity: granularity)
    assert queue.path == path
    assert queue.granularity == granularity
  end

  test "add_data/3", %{qdir: qdir} do
    data = "f0o"
    queue = Queue.new(qdir)
    {subdir_name, full_fn_orig} = Queue.add_data(queue, data)
    {:ok, subdirs} = File.ls(qdir)
    assert Enum.count(subdirs) == 1
    assert subdir_name == hd(subdirs)
    subdir = Path.join(qdir, hd(subdirs))
    {:ok, files} = File.ls(subdir)
    assert Enum.count(files) == 1
    file_name = Path.join(subdir, hd(files))
    assert full_fn_orig == file_name
    assert {:ok, ^data} = File.read(file_name)
  end

  test "add_path/3", %{tempdir: tempdir, qdir: qdir} do
    data = "abc"
    queue = Queue.new(qdir)
    dir = "elems"
    elems_dir = Path.join(qdir, dir)
    assert File.mkdir(elems_dir) == :ok
    tmp_name = Path.join(tempdir, "elem.tmp")
    assert File.write(tmp_name, data) == :ok
    assert File.ls!(tempdir) |> Enum.count() == 2
    new_name = Queue.add_path(queue, tmp_name, dir)
    assert File.ls!(elems_dir) |> Enum.count() == 1
    assert File.ls!(tempdir) |> Enum.count() == 1
    assert {:ok, ^data} = Path.join(qdir, new_name) |> File.read()
  end

  test "add/3", %{qdir: qdir} do
    data = "foo bar"
    queue = Queue.new(qdir)
    elem = Queue.add(queue, data)
    assert {:ok, ^data} = Path.join(qdir, elem) |> File.read()
  end

  test "add_path/2", %{tempdir: tempdir, qdir: qdir} do
    queue = Queue.new(qdir)
    data = "foo0oo"
    path = Path.join(tempdir, "foo.bar")
    File.write(path, data)
    elem = Queue.add_path(queue, path)
    assert {:ok, ^data} = Path.join(qdir, elem) |> File.read()
    refute File.exists?(path)
  end

  test "lock/2 and unlock/2", %{qdir: qdir} do
    queue = Queue.new(qdir)
    data = "foo"
    elem_name = "foo.bar"
    elem_full_path = Path.join(qdir, elem_name)
    File.write(elem_full_path, data)
    assert Queue.lock(queue, elem_name)
    assert File.exists?("#{elem_full_path}.lck")
    _ = Queue.unlock(queue, elem_name)
  end

  test "get/2 (:utf8)", %{qdir: qdir} do
    queue = Queue.new(qdir)
    data = "foo"
    elem = Queue.add(queue, data)
    assert Queue.lock(queue, elem)
    assert Queue.get(queue, elem) == data
  end

  test "get/2 (:binary)", %{qdir: qdir} do
    queue = Queue.new(qdir)
    data = "foo"
    elem = Queue.add(queue, data, :binary)
    assert Queue.lock(queue, elem)
    assert Queue.get(queue, elem, :binary) == data
  end

  test "get/2 (:json)", %{qdir: qdir} do
    queue = Queue.new(qdir)
    data = %{data: "foo"}
    elem = Queue.add(queue, data, :json)
    assert Queue.lock(queue, elem)
    assert Queue.get(queue, elem, :json) == %{"data" => "foo"}
  end

  test "get/2 (:json_atoms)", %{qdir: qdir} do
    queue = Queue.new(qdir)
    data = %{data: "foo"}
    elem = Queue.add(queue, data, :json_atoms)
    assert Queue.lock(queue, elem)
    assert Queue.get(queue, elem, :json_atoms) == data
  end

  test "count/1", %{qdir: qdir} do
    queue = Queue.new(qdir)
    # add "normal" element
    _elem = Queue.add(queue, "foo")
    # simply add a file (fake element) into the elements directory
    fake_elem = File.ls!(qdir) |> hd() |> Path.join("foo.bar")
    fake_path = Path.join(qdir, fake_elem)
    File.write(fake_path, "")
    assert Queue.count(queue) == 1
  end

  test "iterate/1 and remove/2", %{qdir: qdir} do
    queue = Queue.new(qdir, granularity: 1)
    Enum.each(1..5, fn _i -> _elem = Queue.add(queue, "foo") end)
    assert Queue.count(queue) == 5

    Queue.iterate(queue)
    |> Enum.each(fn elem ->
      Queue.lock(queue, elem)
      Queue.remove(queue, elem)
    end)

    assert Queue.count(queue) == 0
  end

  test "purge/2 one directory and one element", %{qdir: qdir} do
    queue = Queue.new(qdir)
    Queue.add(queue, "foo")
    assert Queue.count(queue) == 1

    elem = Queue.iterate(queue) |> Enum.take(1)
    Queue.lock(queue, elem)
    elem_path_lock = Path.join(qdir, "#{elem}.lck")
    assert File.exists?(elem_path_lock)
    Process.sleep(2_000)

    Queue.purge(queue, maxlock: 1)
    refute File.exists?(elem_path_lock)
    assert Queue.count(queue) == 1
    assert File.ls!(qdir) |> Enum.count() == 1
  end

  test "purge/2 multiple directories and elements", %{qdir: qdir} do
    queue = Queue.new(qdir, granularity: 1)
    Queue.add(queue, "foo")
    assert Queue.count(queue) == 1
    Process.sleep(2_000)

    Queue.add(queue, "bar")
    assert Queue.count(queue) == 2
    assert File.ls!(qdir) |> Enum.count() == 2

    Queue.purge(queue)
    assert Queue.count(queue) == 2

    elem = Queue.iterate(queue) |> Enum.take(1)
    Queue.lock(queue, elem)
    Queue.remove(queue, elem)
    assert Queue.count(queue) == 1

    Queue.purge(queue)
    assert File.ls!(qdir) |> Enum.count() == 1
    Process.sleep(2_000)

    Queue.add(queue, "baz")
    assert File.ls!(qdir) |> Enum.count() == 2

    [elem1, elem2] =
      Queue.iterate(queue)
      |> Enum.map(fn elem ->
        Queue.lock(queue, elem)
        elem
      end)

    lock_path1 = Path.join(qdir, "#{elem1}.lck")
    assert File.exists?(lock_path1)

    File.touch(lock_path1, System.os_time(:second) - 25)
    Queue.purge(queue, maxlock: 10)
    refute File.exists?(lock_path1)

    lock_path2 = Path.join(qdir, "#{elem2}.lck")
    assert File.exists?(lock_path2)
  end
end
