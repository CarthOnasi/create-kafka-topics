class Topic:
    def __init__(self, name, partition_number, replication_factor):
        self.name = name
        self.partition_number = partition_number
        self.replication_factor = replication_factor
