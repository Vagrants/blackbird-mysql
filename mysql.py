# -*- encoding: utf-8 -*-
# pylint: disable=C0111,C0301,R0903,C0326

__VERSION__ = '0.1.0'

import re
import MySQLdb

from blackbird.plugins import base

class ConcreteJob(base.JobBase):
    """
    This class is Called by "Executor".
    Get mysql information and send to specified zabbix server.
    """

    def __init__(self, options, queue=None, logger=None):
        super(ConcreteJob, self).__init__(options, queue, logger)

        self.components = self.options['components'].split(',')

    def build_items(self):
        """
        main loop
        """

        # ping item
        self.ping()

        conn = self.mysql_connect()

        # get version
        if 'version' in self.components:
            self.get_version(conn)

        # get global variables
        if 'global_variables' in self.components:
            self.get_global_variables(conn)

        # get global status
        if 'global_status' in self.components:
            self.get_global_status(conn)

        # get slave status
        if 'slave_status' in self.components:
            self.get_slave_status(conn)

        conn.close()

    def build_discovery_items(self):
        """
        main loop for lld
        """

        conn = self.mysql_connect()

        # Number of tables for each database
        if 'table_count' in self.components:
            self.get_number_of_tables(conn)

        conn.close()


    def mysql_connect(self):

        try:
            conn = MySQLdb.connect(self.options['mysqlhost'],
                                   self.options['mysqluser'],
                                   self.options['mysqlpass'])
        except OperationalError:
            self.logger.error(
                'Can not connect to mysql {user}@{host}'
                ''.format(user=self.options['mysqluser'],
                          host=self.options['mysqlhost'])
            )
            self._enqueue('mysql.alive', 0)
            raise

        self._enqueue('mysql.alive', 1)
        return conn

    def ping(self):
        """
        send ping item
        """

        self._enqueue('blackbird.mysql.ping', 1)
        self._enqueue('blackbird.mysql.version', __VERSION__)

    def _enqueue(self, key, value):

        item = MySQLItem(
            key=key,
            value=value,
            host=self.options['hostname']
        )
        self.queue.put(item, block=False)
        self.logger.debug(
            'Inserted to queue {key}:{value}'
            ''.format(key=key, value=value)
        )

    def _enqueue_lld(self, key, value):

        item = base.DiscoveryItem(
            key=key,
            value=value,
            host=self.options['hostname']
        )
        self.queue.put(item, block=False)
        self.logger.debug(
            'Inserted to lld queue {key}:{value}'
            ''.format(key=key, value=str(value))
        )

    def get_version(self, conn):

        cursor = conn.cursor()
        cursor.execute('SELECT VERSION();')
        mysql_version = cursor.fetchone()[0]
        cursor.close()

        self._enqueue('mysql.version', mysql_version)

    def get_innodb_status(self, conn):

        cursor = conn.cursor()
        cursor.execute('SHOW ENGINE INNODB STATUS;')
        _innodb_status = cursor.fetchone()[2]
        cursor.close()

        _innodb_result = InnoDBParse(_innodb_status)

        for k in _innodb_result.results.keys():
            category = 'innodb_stats.{0}'.format(k)
            self._adjust_queue(category, _innodb_result.results[k])

    def _adjust_queue(self, category, dict):

        for (k, v) in dict.items():
            item_key = ('mysql.{category}[{k}]'
                        '' .format(category=category, k=k)
                       )
            self._enqueue(item_key, v)

    def get_global_variables(self, conn):

        cursor = conn.cursor()
        cursor.execute('SHOW GLOBAL VARIABLES;')
        result = dict([(key.lower(), value) for key, value in cursor.fetchall()])
        cursor.close()

        self._adjust_queue('global_variables', result)

        # get innodb information
        if result['have_innodb'].lower() == 'yes':
            self.get_innodb_status(conn)

    def get_global_status(self, conn):

        cursor = conn.cursor()
        cursor.execute('SHOW GLOBAL STATUS;')
        result = dict([(key.lower(), value) for key, value in cursor.fetchall()])
        cursor.close()

        self._adjust_queue('global_status', result)

    def get_slave_status(self, conn):

        cursor = conn.cursor()
        cursor.execute('SHOW SLAVE STATUS;')

        if cursor.rowcount > 0:
            self._enqueue('mysql.role', 'slave')
            column = [c[0].lower() for c in cursor.description]
            status = dict(zip(column, cursor.fetchone()))
            self._adjust_queue('slave_status', status)
        else:
            self._enqueue('mysql.role', 'master')

        cursor.close()

    def get_number_of_tables(self, conn):

        cursor = conn.cursor()
        cursor.execute('SELECT table_schema, COUNT(table_name) FROM information_schema.TABLES GROUP BY table_schema;')
        result = dict([(key, value) for key, value in cursor.fetchall()])
        cursor.close()

        _ignore_databases = ['information_schema', 'performance_schema', 'test', 'mysql']

        for db in result.keys():
            if db in _ignore_databases:
                continue
            else:
                self._enqueue_lld(db, result[db])


class InnoDBParse:

    results = {}
    data = {}

    def __init__(self, innodb_status):

        parsed = self.parse_innodb_status(innodb_status)

        self.results['semaphores']             = self.parse_semaphores(parsed['SEMAPHORES'])
        self.results['background_thread']      = self.parse_background_thread(parsed['BACKGROUND THREAD'])
        self.results['transactions']           = self.parse_transanctions(parsed['TRANSACTIONS'])
        self.results['file_io']                = self.parse_file_io(parsed['FILE I/O'])
        self.results['ibuf_and_hash_index']    = self.parse_ibuf_hi(parsed['INSERT BUFFER AND ADAPTIVE HASH INDEX'])
        self.results['log']                    = self.parse_log(parsed['LOG'])
        self.results['buffer_pool_and_memory'] = self.parse_bp_and_mem(parsed['BUFFER POOL AND MEMORY'])
        self.results['row_operations']         = self.parse_row_operations(parsed['ROW OPERATIONS'])

    def parse_innodb_status(self, innodb_status):

        dict = {}
        regex = "-+\n([A-Z /]*)\n-+"
        keys = re.compile(regex).findall(innodb_status)
        values = re.sub(regex, "\n\n\n", innodb_status).split("\n\n\n\n")[1:]

        i=0
        for key in keys:
            dict[key] = values[i].lstrip().rstrip()
            i += 1
        return dict

    def parse_semaphores(self, text):
        """
        ----------
        SEMAPHORES
        ----------
        OS WAIT ARRAY INFO: reservation count 94982, signal count 95133
        Mutex spin waits 170471, rounds 399013, OS waits 10745
        RW-shared spins 82436, rounds 2458529, OS waits 81850
        RW-excl spins 499, rounds 74761, OS waits 2354
        Spin rounds per wait: 2.34 mutex, 29.82 RW-shared, 149.82 RW-excl
        """
        dict = {}
        regex = (
                "OS WAIT ARRAY INFO: reservation count (\d+), signal count (\d+)\n"
                "Mutex spin waits (\d+), rounds (\d+), OS waits (\d+)\n"
                "RW-shared spins (\d+), rounds (\d+), OS waits (\d+)\n"
                "RW-excl spins (\d+), rounds (\d+), OS waits (\d+)\n"
                "Spin rounds per wait: (\d+\.\d+) mutex, (\d+\.\d+) RW-shared, (\d+\.\d+) RW-excl"
                )

        try:
            result = re.compile(regex).findall(text)[0]

            dict['os_wait_reservation_count']      = result[0]
            dict['os_wait_signal_count']           = result[1]
            dict['mutex_spin_waits']               = result[2]
            dict['mutex_rounds']                   = result[3]
            dict['mutex_os_waits']                 = result[4]
            dict['rw_shared_spins']                = result[5]
            dict['rw_shared_rounds']               = result[6]
            dict['rw_shared_os_waits']             = result[7]
            dict['rw_excl_spins']                  = result[8]
            dict['rw_excl_rounds']                 = result[9]
            dict['rw_excl_os_waits']               = result[10]
            dict['spin_rounds_per_wait_mutex']     = result[11]
            dict['spin_rounds_per_wait_rw_shared'] = result[12]
            dict['spin_rounds_per_wait_rw_excl']   = result[13]
        except:
            pass

        return dict

    def parse_background_thread(self, text):
        """
        -----------------
        BACKGROUND THREAD
        -----------------
        srv_master_thread loops: 849820 1_second, 849819 sleeps, 84080 10_second, 9293 background, 9293 flush
        srv_master_thread log flush and writes: 851861
        """
        dict = {}
        regex = (
                "srv_master_thread loops: (\d+) 1_second, (\d+) sleeps, (\d+) 10_second, (\d+) background, (\d+) flush\n"
                "srv_master_thread log flush and writes: (\d+)"
                )
        try:
            result = re.compile(regex).findall(text)[0]

            dict['srv_master_thread_loops_1_second']       = result[0]
            dict['srv_master_thread_loops_sleeps']         = result[1]
            dict['srv_master_thread_loops_10_second']      = result[2]
            dict['srv_master_thread_loops_background']     = result[3]
            dict['srv_master_thread_loops_flush']          = result[4]
            dict['srv_master_thread_log_flush_and_writes'] = result[5]

        except:
            pass

        return dict

    def parse_transanctions(self, text):
        """
        ------------
        TRANSACTIONS
        ------------
        Trx id counter 272DCD
        Purge done for trx's n:o < 272DAB undo n:o < 0
        History list length 740
        LIST OF TRANSACTIONS FOR EACH SESSION:
        ---TRANSACTION 0, not started
        (snip)
        ...
        """
        dict = {}
        regex = (
                "Trx id counter ([0-9A-F]+)\n"
                "Purge done for trx's n:o < ([0-9A-F]+) undo n:o < ([0-9A-F]+)\n"
                "History list length (\d+)"
                )
        try:
            result = re.compile(regex).findall(text)[0]

            dict['trx_id_counter']       = result[0]
            dict['purge_done_for_trx']   = result[1]
            dict['purge_done_for_undo']  = result[2]
            dict['history_list_length']  = result[3]

        except:
            pass

        return dict

    def parse_file_io(self, text):
        """
        --------
        FILE I/O
        --------
        I/O thread 0 state: waiting for completed aio requests (insert buffer thread)
        (snip)
        ...
        Pending normal aio reads: 0 [0, 0] , aio writes: 0 [0, 0] ,
         ibuf aio reads: 0, log i/o's: 0, sync i/o's: 0
        Pending flushes (fsync) log: 0; buffer pool: 0
        166 OS file reads, 4895744 OS file writes, 726684 OS fsyncs
        0.00 reads/s, 0 avg bytes/read, 0.50 writes/s, 0.25 fsyncs/s
        """
        dict = {}
        regex = (
                "Pending normal aio reads: (\d+) \[.*\] , aio writes: (\d+) \[.*\] ,\n"
                " ibuf aio reads: (\d+), log i/o's: (\d+), sync i/o's: (\d+)\n"
                "Pending flushes \(fsync\) log: (\d+); buffer pool: (\d+)\n"
                "(\d+) OS file reads, (\d+) OS file writes, (\d+) OS fsyncs\n"
                "(\d+\.\d+) reads/s, (\d+) avg bytes/read, (\d+\.\d+) writes/s, (\d+\.\d+) fsyncs/s"
                )
        try:
            result = re.compile(regex).findall(text)[0]

            dict['pending_normal_aio_reads']      = result[0]
            dict['pending_normal_aio_writes']     = result[1]
            dict['pending_normal_ibuf_aio_reads'] = result[2]
            dict['pending_normal_log_io']         = result[3]
            dict['pending_normal_sync_io']        = result[4]
            dict['pending_flush_log']             = result[4]
            dict['pending_flush_buffer_pool']     = result[5]
            dict['os_file_reads']                 = result[6]
            dict['os_file_writes']                = result[7]
            dict['os_file_fsyncs']                = result[8]
            dict['reads_per_sec']                 = result[9]
            dict['avg_bytes_per_read']            = result[10]
            dict['writes_per_sec']                = result[11]
            dict['fsyncs_per_sec']                = result[12]

        except:
            pass

        return dict

    def parse_ibuf_hi(self, text):
        """
        -------------------------------------
        INSERT BUFFER AND ADAPTIVE HASH INDEX
        -------------------------------------
        Ibuf: size 1, free list len 0, seg size 2, 0 merges
        merged operations:
         insert 0, delete mark 0, delete 0
        discarded operations:
         insert 0, delete mark 0, delete 0
        Hash table size 1940399, node heap has 156 buffer(s)
        4.87 hash searches/s, 9.00 non-hash searches/s
        """
        dict = {}
        regex = (
                "Ibuf: size (\d+), free list len (\d+), seg size (\d+), (\d+) merges\n"
                "merged operations:\n"
                " insert (\d+), delete mark (\d+), delete (\d+)\n"
                "discarded operations:\n"
                " insert (\d+), delete mark (\d+), delete (\d+)\n"
                "Hash table size (\d+), node heap has (\d+) buffer\(s\)\n"
                "(\d+\.\d+) hash searches/s, (\d+\.\d+) non-hash searches/s"
                )
        try:
            result = re.compile(regex).findall(text)[0]

            dict['ibuf_size']                  = result[0]
            dict['ibuf_free_list_len']         = result[1]
            dict['ibuf_seg_size']              = result[2]
            dict['ibuf_merges']                = result[3]
            dict['ibuf_merged_insert']         = result[4]
            dict['ibuf_merged_delete_mark']    = result[5]
            dict['ibuf_merged_delete']         = result[6]
            dict['ibuf_discarded_insert']      = result[7]
            dict['ibuf_discarded_delete_mark'] = result[8]
            dict['ibuf_discarded_delete']      = result[9]
            dict['hash_table_size']            = result[10]
            dict['node_heap_buffers']          = result[11]
            dict['hash_searches_per_sec']      = result[12]
            dict['non_hash_searches_per_sec']  = result[13]

        except:
            pass

        return dict

    def parse_log(self, text):
        """
        ---
        LOG
        ---
        Log sequence number 1122294428
        Log flushed up to   1122294428
        Last checkpoint at  1122285403
        0 pending log writes, 0 pending chkp writes
        710948 log i/o's done, 0.50 log i/o's/second
        """
        dict = {}
        regex = (
                "Log sequence number\s+(\d+)\n"
                "Log flushed up to\s+(\d+)\n"
                "Last checkpoint at\s+(\d+)\n"
                "(\d+) pending log writes, (\d+) pending chkp writes\n"
                "(\d+) log i/o's done, (\d+\.\d+) log i/o's/second"
                )

        try:
            result = re.compile(regex).findall(text)[0]

            dict['log_sequence_number'] = result[0]
            dict['log_flushed_up_to']   = result[1]
            dict['last_checkpoint_at']  = result[2]
            dict['pending_log_writes']  = result[3]
            dict['pending_chkp_writes'] = result[4]
            dict['log_io_done']         = result[5]
            dict['log_io_per_sec']      = result[6]

        except:
            pass

        return dict

    def parse_bp_and_mem(self, text):
        """
        ----------------------
        BUFFER POOL AND MEMORY
        ----------------------
        Total memory allocated 1003405312; in additional pool allocated 0
        Dictionary memory allocated 963282
        Buffer pool size   59840
        Free buffers       39179
        Database pages     14482
        Old database pages 5325
        Modified db pages  36
        Pending reads 0
        Pending writes: LRU 0, flush list 0, single page 0
        Pages made young 1409, not young 0
        0.00 youngs/s, 0.00 non-youngs/s
        Pages read 0, created 14482, written 4181760
        0.00 reads/s, 0.00 creates/s, 0.00 writes/s
        Buffer pool hit rate 1000 / 1000, young-making rate 0 / 1000 not 0 / 1000
        Pages read ahead 0.00/s, evicted without access 0.00/s, Random read ahead 0.00/s
        LRU len: 14482, unzip_LRU len: 12046
        I/O sum[0]:cur[0], unzip sum[0]:cur[0]
        """
        dict = {}
        regex = (
                "Total memory allocated (\d+); in additional pool allocated (\d+)\n"
                "Dictionary memory allocated (\d+)\n"
                "Buffer pool size\s+(\d+)\n"
                "Free buffers\s+(\d+)\n"
                "Database pages\s+(\d+)\n"
                "Old database pages\s+(\d+)\n"
                "Modified db pages\s+(\d+)\n"
                "Pending reads (\d+)\n"
                "Pending writes: LRU (\d+), flush list (\d+), single page (\d+)\n"
                "Pages made young (\d+), not young (\d+)\n"
                "(\d+\.\d+) youngs/s, (\d+\.\d+) non-youngs/s\n"
                "Pages read (\d+), created (\d+), written (\d+)\n"
                "(\d+\.\d+) reads/s, (\d+\.\d+) creates/s, (\d+\.\d+) writes/s\n"
                "Buffer pool hit rate (\d+) / (\d+), young-making rate (\d+) / (\d+) not (\d+) / (\d+)\n"
                "Pages read ahead (\d+\.\d+)/s, evicted without access (\d+\.\d+)/s, Random read ahead (\d+\.\d+)/s\n"
                "LRU len: (\d+), unzip_LRU len: (\d+)\n"
                "I/O sum\[(\d+)\]:cur\[(\d+)\], unzip sum\[(\d+)\]:cur\[(\d+)\]"
                )

        try:
            result = re.compile(regex).findall(text)[0]

            dict['total_memory_allocated']               = result[0]
            dict['additional_pool_allocated']            = result[1]
            dict['dictionary_memory_allocated']          = result[2]
            dict['buffer_pool_size']                     = result[3]
            dict['free_buffers']                         = result[4]
            dict['database_pages']                       = result[5]
            dict['old_database_pages']                   = result[6]
            dict['modified_database_pages']              = result[7]
            dict['pending_reads']                        = result[8]
            dict['pending_writes_lru']                   = result[9]
            dict['pending_writes_flush_list']            = result[10]
            dict['pending_writes_single_page']           = result[11]
            dict['pages_made_young']                     = result[12]
            dict['pages_made_not_young']                 = result[13]
            dict['pages_made_youngs_per_sec']            = result[14]
            dict['pages_made_non_youngs_per_sec']        = result[15]
            dict['pages_read']                           = result[16]
            dict['pages_created']                        = result[17]
            dict['pages_written']                        = result[18]
            dict['pages_reads_per_sec']                  = result[19]
            dict['pages_creates_per_sec']                = result[20]
            dict['pages_writes_per_sec']                 = result[21]
            dict['buffer_pool_hitrate_numerator']        = result[22]
            dict['buffer_pool_hitrate_denominator']      = result[23]
            dict['young_making_rate_numerator']          = result[24]
            dict['young_making_rate_denominator']        = result[25]
            dict['young_making_rate_not_numerator']      = result[26]
            dict['young_making_rate_not_denominator']    = result[27]
            dict['pages_read_ahead_per_sec']             = result[28]
            dict['pages_evicted_without_access_per_sec'] = result[29]
            dict['random_read_ahead_per_sec']            = result[30]
            dict['lru_len']                              = result[31]
            dict['unzip_lru_len']                        = result[32]
            dict['io_sum']                               = result[33]
            dict['io_sum_cur']                           = result[34]
            dict['unzip_sum']                            = result[35]
            dict['unzip_sum_cur']                        = result[36]

        except:
            pass

        return dict

    def parse_row_operations(self, text):
        """
        --------------
        ROW OPERATIONS
        --------------
        0 queries inside InnoDB, 0 queries in queue
        1 read views open inside InnoDB
        Main thread process no. 4718, id 140236006328064, state: sleeping
        Number of rows inserted 3008242, updated 186007, deleted 859973, read 73110170
        3.37 inserts/s, 0.12 updates/s, 0.00 deletes/s, 10.25 reads/s
        """
        dict = {}
        regex = (
                "(\d+) queries inside InnoDB, (\d+) queries in queue\n"
                "(\d+) read views open inside InnoDB\n"
                "Main thread process no. (\d+), id (\d+), state: (\S+)\n"
                "Number of rows inserted (\d+), updated (\d+), deleted (\d+), read (\d+)\n"
                "(\d+\.\d+) inserts/s, (\d+\.\d+) updates/s, (\d+\.\d+) deletes/s, (\d+\.\d+) reads/s"
                )

        try:
            result = re.compile(regex).findall(text)[0]

            dict['queries_inside']          = result[0]
            dict['queries_in_queue']        = result[1]
            dict['views_inside']            = result[2]
            dict['main_thread_process_no']  = result[3]
            dict['main_thread_id']          = result[4]
            dict['main_thread_state']       = result[5]
            dict['number_of_rows_inserted'] = result[6]
            dict['number_of_rows_updated']  = result[7]
            dict['number_of_rows_deleted']  = result[8]
            dict['number_of_rows_read']     = result[9]
            dict['inserted_per_sec']        = result[10]
            dict['updates_per_sec']         = result[11]
            dict['deletes_per_sec']         = result[12]
            dict['reads_per_sec']           = result[13]

        except:
            pass

        return dict

class MySQLItem(base.ItemBase):
    """
    Enqued item.
    """

    def __init__(self, key, value, host):
        super(MySQLItem, self).__init__(key, value, host)

        self._data = {}
        self._generate()

    @property
    def data(self):
        return self._data

    def _generate(self):
        self._data['key'] = self.key
        self._data['value'] = self.value
        self._data['host'] = self.host
        self._data['clock'] = self.clock


class Validator(base.ValidatorBase):
    """
    Validate configuration.
    """

    def __init__(self):
        self.__spec = None

    @property
    def spec(self):
        self.__spec = (
            "[{0}]".format(__name__),
            "mysqlhost=string(default='127.0.0.1')",
            "mysqluser=string(default='bbd')",
            "mysqlpass=string(default='bbd')",
            "components=string(default='version,global_variables,global_status,table_count')",
            "hostname=string(default={0})".format(self.detect_hostname()),
        )
        return self.__spec
