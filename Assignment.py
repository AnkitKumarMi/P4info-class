from collections import namedtuple


class P4LockParser:
    Lock = namedtuple('Lock', ['id', 'timestamp', 'lock_type', 'operation_type', 'username', 'client_address',
                               'resource', 'wait_held_time'])

    def __init__(self):
        self.__db_lock = []

    def __get_db_locks(self, log_file):
        with open(log_file, 'r') as f:
            info_blocks = self.__get_info_blocks(f.read())
            for block in info_blocks:
                lock = self.__str2dblock(block)
                self.__db_lock.append(lock)

    def __get_info_blocks(self, log_text):
        blocks = log_text.split('\n\n')
        return [b for b in blocks if b.startswith('lock ')]

    def __str2dblock(self, info_block):
        info_lines = info_block.split('\n')
        id_line = info_lines[0]
        timestamp = info_lines[1].split()[0]
        lock_type, op_type, username = info_lines[2].split()
        client_addr = info_lines[3].split()[2]
        resource = info_lines[4].split()[1]
        wait_held_time = info_lines[-1].split()[1:3]
        wait_held_time = ' '.join(wait_held_time)
        return self.Lock(id_line, timestamp, lock_type, op_type, username, client_addr, resource, wait_held_time)

    def get_max_wait_read(self):
        wait_times = [lock.wait_held_time for lock in self.__db_lock if
                      lock.lock_type == 'read' and 'w' in lock.operation_type]
        return max(wait_times)

    def get_max_wait_write(self):
        wait_times = [lock.wait_held_time for lock in self.__db_lock if
                      lock.lock_type == 'write' and 'w' in lock.operation_type]
        return max(wait_times)

    def get_max_held_read(self):
        held_times = [lock.wait_held_time for lock in self.__db_lock if
                      lock.lock_type == 'read' and 'h' in lock.operation_type]
        return max(held_times)

    def get_max_held_write(self):
        held_times = [lock.wait_held_time for lock in self.__db_lock if
                      lock.lock_type == 'write' and 'h' in lock.operation_type]
        return max(held_times)
