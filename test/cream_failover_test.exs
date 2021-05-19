defmodule CreamFailoverTest do
  use ExUnit.Case, async: false

  # import ExUnit.CaptureLog # Used to capture logging and assert against it.

  alias Test.Cluster

  setup do
    {:ok, _} = Toxiproxy.reset()
    Cluster.flush
    :ok
  end

  test "failover" do
    assert Cluster.get("test1") == nil
    Cluster.set({"test1", "111"})
    assert Cluster.get("test1") == "111"

    n = server_num("test1")
    down(n)
    assert Cluster.get("test1") == nil
    assert Cluster.get("test1") == nil # should failover at 2nd error
    assert Cluster.set({"test1", "222"}) == :ok
    assert Cluster.get("test1") == "222"

    up(n)
    wait_until(fn -> Cluster.get("test1") == "111" end)
  end

  defp wait_until(func, retry \\ 5) do
    assert retry > 0
    unless func.() do
      :timer.sleep(1000)
      wait_until(func, retry - 1)
    end
  end

  defp up(n) do
    {:ok, _} = Toxiproxy.update(%{name: "memcached_0#{n}", enabled: true})
  end

  defp down(n) do
    {:ok, _} = Toxiproxy.update(%{name: "memcached_0#{n}", enabled: false})
  end

  defp server_num(key) do
    Cluster.with_conn([key], fn _, _ -> :ok end)
    |> Map.keys
    |> Enum.at(0)
    |> String.last
    |> String.to_integer
  end
end
