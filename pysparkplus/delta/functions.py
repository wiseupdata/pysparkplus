def get_partitions_from_path(path: str) -> List[str]:
    partitions: List[str] = spark.sql(f"describe detail delta.`{path}`").select("partitionColumns").collect()[0][0]
    print(f"Partitions found: {partitions}")
    return partitions
