require Logger

defmodule Cream.Cluster.Worker do
  @moduledoc false

  use GenServer

  alias Cream.Continuum

  def start_link(connection_map, state) do
    GenServer.start_link(__MODULE__, {connection_map, state})
  end

  def init({connection_map, state}) do
    Logger.debug "Starting: #{inspect connection_map}"

    continuum = connection_map
      |> Map.keys
      |> Continuum.new

    {:ok, %{continuum: continuum, connection_map: connection_map, state: state}}
  end

  def handle_call({:set, pairs, options}, _from, state) do
    pairs_by_conn = Enum.group_by pairs, fn {key, _value} ->
      find_conn(state, key)
    end

    reply = Enum.reduce pairs_by_conn, %{}, fn {{conn, server}, pairs}, acc ->
      responses =
	case handle_error(server, state, fn ->
	      Memcache.multi_set(conn, pairs, options)
	    end) do
          {:ok, results} -> results
          {:error, reason} when is_atom(reason) -> Enum.map(pairs, fn _ ->
            {:error, Atom.to_string(reason)}
          end)
	end

      Enum.zip(pairs, responses)
        |> Enum.reduce(acc, fn {pair, status}, acc ->
          {key, _value} = pair
          status = case status do
            {:ok} -> :ok
            status -> status
          end
          Map.put(acc, key, status)
        end)
    end

    {:reply, reply, state}
  end

  def handle_call({:get, keys, options}, _from, state) do
    keys_by_conn = Enum.group_by keys, fn key ->
      find_conn(state, key)
    end

    reply = Enum.reduce keys_by_conn, %{}, fn {{conn, server}, keys}, acc ->
      case handle_error(server, state, fn ->
	    Memcache.multi_get(conn, keys, options)
	  end) do
        {:ok, map} -> Map.merge(acc, map)
        _ -> acc
      end
    end

    {:reply, reply, state}
  end

  def handle_call({:with_conn, keys, func}, _from, state) do
    keys_by_conn_and_server = Enum.group_by keys, fn key ->
      find_conn(state, key)
    end

    keys_by_conn_and_server
      |> Enum.reduce(%{}, fn {{conn, server}, keys}, acc ->
        Map.put(acc, server, func.(conn, keys))
      end)
      |> reply(state)
  end

  def handle_call({:flush, options}, _from, state) do
    ttl = Keyword.get(options, :ttl, 0)

    reply = Enum.map state.connection_map, fn {server, conn} ->
      case handle_error(server, state, fn ->
	    Memcache.Connection.execute(conn, :FLUSH, [ttl])
	  end) do
        {:ok} -> :ok
        whatever -> whatever
      end
    end

    {:reply, reply, state}
  end

  def handle_call({:delete, keys}, _from, state) do
    Enum.group_by(keys, &find_conn(state, &1))
      |> Enum.reduce(%{}, fn {{conn, server}, keys}, results ->
        commands = Enum.map(keys, &{:DELETEQ, [&1]})
      case handle_error(server, state, fn ->
	    Memcache.Connection.execute_quiet(conn, commands)
	  end) do
          {:ok, conn_results} -> Enum.zip(keys, conn_results)
            |> Enum.reduce(results, fn {key, conn_results}, results ->
              case conn_results do
                {:ok} -> Map.put(results, key, :ok)
                error -> Map.put(results, key, error)
              end
            end)
          {:error, _} -> Enum.reduce(keys, results, fn key, results ->
            Map.put(results, key, {:error, "connection error"})
          end)
        end
      end)
      |> reply(state)
  end

  def handle_call({:noop, server}, _from, state) do
    conn = state.connection_map[server]
    handle_timeout(fn -> Memcache.noop(conn) end) |> reply(state)
  end

  defp reply(reply, state) do
    {:reply, reply, state}
  end

  defp find_conn(state, key) do
    {:ok, server} = Continuum.find(state.continuum, key, state.state)
    conn = state.connection_map[server]
    {conn, server}
  end

  defp handle_timeout(func) do
    try do
      func.()
    catch
      :exit, {:timeout, _} -> {:error, :timeout}
    end
  end

  defp handle_error(server, state, func) do
    case handle_timeout(func) do
      {:error, error} when error in [:closed, :timeout] ->
	GenServer.cast(state.state, {:failed, server})
	{:error, error}

      whatever ->
	GenServer.cast(state.state, {:recover, server})
	whatever
    end
  end
end
