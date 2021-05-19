require Logger

defmodule Cream.Cluster.State do
  @moduledoc false

  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  def init(opts) do
    server_names = opts[:servers]

    failure_counts =
      Enum.reduce(server_names, %{}, fn name, acc ->
        Map.put(acc, name, 0)
      end)

    {:ok, %{opts: opts, failure_counts: failure_counts}}
  end

  def handle_call({:alive?, server_name}, _from, state) do
    {:reply, alive?(server_name, state), state}
  end

  def handle_cast({:failed, server_name}, state) do
    state = incr_failure_count(server_name, state)
    {:noreply, state}
  end

  def handle_cast({:recover, server_name}, state) do
    state = Map.put(state, :failure_counts, Map.put(state.failure_counts, server_name, 0))
    {:noreply, state}
  end

  # Always returns true if the `failover' option is turned off
  defp alive?(server_name, state) do
    !state.opts[:failover] || state.failure_counts[server_name] < state.opts[:max_failures]
  end

  defp incr_failure_count(server_name, state) do
    failure_count = state.failure_counts[server_name] + 1

    if failure_count == state.opts[:max_failures] do
      Logger.error("Memcache server \"#{server_name}\" is now in failure state.")
      parent = self()
      Task.start(fn -> check_connection(parent, state.opts, server_name) end)
    end

    Map.put(state, :failure_counts, Map.put(state.failure_counts, server_name, failure_count))
  end

  defp check_connection(parent, opts, server_name) do
    result =
      try do
        Cream.Cluster.noop(opts[:name], server_name)
      catch
        :exit, e ->
          {:error, e}
      end

    case result do
      {:ok} ->
	Logger.warn("Memcache server \"#{server_name}\" recovered.")
        GenServer.cast(parent, {:recover, server_name})

      {:error, reason} ->
        Logger.warn("Memcache server \"#{server_name}\" is still failing: #{inspect(reason)}.")
        Process.sleep(opts[:down_retry_delay] * 1000)
        check_connection(parent, opts, server_name)
    end
  end
end
