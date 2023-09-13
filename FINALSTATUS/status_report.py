def create_table_from_clusters(cluster_configs, column_names):
    table = []

    for cluster_info in cluster_configs:
        druid_status = fetch_druid_status(cluster_info)
        if druid_status:
            cluster_data = [
                cluster_info["cluster_name"],
                druid_status.get("version", ""),
                druid_status.get("coordinator", {}).get("status", ""),
                len(druid_status.get("dataSources", [])),
                sum(ds.get("segments", 0) for ds in druid_status.get("dataSources", [])),
                druid_status.get("segments", {}).get("unavailable", 0),
                druid_status.get("server", {}).get("total", {}).get("supervisor", 0),
                druid_status.get("server", {}).get("total", {}).get("supervisor", 0) - \
                druid_status.get("server", {}).get("total", {}).get("runningSupervisor", 0),
                druid_status.get("server", {}).get("total", {}).get("task", 0),
                druid_status.get("server", {}).get("total", {}).get("failedTasks", 0),
                druid_status.get("server", {}).get("total", {}).get("pendingTasks", 0),
                druid_status.get("server", {}).get("middleManager", 0),
                druid_status.get("server", {}).get("historical", 0),
                druid_status.get("server", {}).get("total", {}).get("peon", 0),
                druid_status.get("server", {}).get("total", {}).get("runningPeon", 0),
                druid_status.get("server", {}).get("total", {}).get("extension", 0),
                druid_status.get("server", {}).get("historical", {}).get("storage", {}).get("used", 0),
                druid_status.get("server", {}).get("historical", {}).get("memory", {}).get("used", 0),
                druid_status.get("server", {}).get("middleManager", {}).get("threads", 0),
                druid_status.get("server", {}).get("health", "")
            ]
            table.append(cluster_data)

    return table
