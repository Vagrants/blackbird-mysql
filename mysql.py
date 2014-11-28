# -*- encoding: utf-8 -*-
# pylint: disable=C0111,C0301,R0903,C0326,W0702

__VERSION__ = '0.1.1'

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

        # try to connect and send "mysql.alive"
        conn = self.mysql_connect()

        # get version
        self.get_version(conn)

        # get global variables
        if 'global_variables' in self.components:
            self.get_global_variables(conn)

        # get global status
        if 'global_status' in self.components:
            self.get_global_status(conn)

        # get innodb information
        if 'innodb_status' in self.components:
            self.get_innodb_status(conn)

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
        except MySQLdb.OperationalError, _ex:
            self.logger.error(
                'Can not connect to mysql {user}@{host} [{error}]'
                ''.format(user=self.options['mysqluser'],
                          host=self.options['mysqlhost'],
                          error=_ex)
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

    def _adjust_queue(self, _category, _dict):

        for _key, _val in _dict.items():
            item_key = 'mysql.{c}[{k}]'.format(c=_category, k=_key)
            self._enqueue(item_key, _val)

    def get_global_variables(self, conn):

        cursor = conn.cursor()
        cursor.execute('SHOW GLOBAL VARIABLES;')
        result = dict([(key.lower(), value) for key, value in cursor.fetchall()])
        cursor.close()

        self._adjust_queue('global_variables', result)

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
            column = [c[0].lower() for c in cursor.description]
            status = dict(zip(column, cursor.fetchone()))
            self._adjust_queue('slave_status', status)

        cursor.close()

    def get_number_of_tables(self, conn):

        cursor = conn.cursor()
        cursor.execute('SELECT table_schema, COUNT(table_name) FROM information_schema.TABLES GROUP BY table_schema;')
        result = dict([(key, value) for key, value in cursor.fetchall()])
        cursor.close()

        _ignore_databases = ['information_schema', 'performance_schema', 'test', 'mysql']

        _dict = dict()
        for _db in result.keys():
            if _db in _ignore_databases:
                continue
            else:
                _dict[_db] = result[_db]

        # enqueue lld meta data
        ditem = base.DiscoveryItem(
            key='mysql.lld.table_count',
            value=[
                {'{#DBNAME}': _dbname} for _dbname in _dict.keys()
            ],
            host=self.options['hostname']
        )
        self.queue.put(ditem, block=False)
        self.logger.debug(
            'Inserted to queue LLD data #DBNAME : {db}'
            ''.format(db=_dict.keys())
        )

        # enqueue lld value
        self._adjust_queue('lld.table_count', _dict)


class InnoDBParse(object):

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

        self.results['latest_error'] = {}
        if 'LATEST FOREIGN KEY ERROR' in parsed:
            self.results['latest_error']['latest_foreign_key_error'] = 1
        else:
            self.results['latest_error']['latest_foreign_key_error'] = 0

        if 'LATEST DETECTED DEADLOCK' in parsed:
            self.results['latest_error']['latest_detected_deadlock'] = 1
        else:
            self.results['latest_error']['latest_detected_deadlock'] = 0

    @staticmethod
    def parse_innodb_status(innodb_status):

        _dict = {}
        regex = "-+\n([A-Z /]*)\n-+"
        keys = re.compile(regex).findall(innodb_status)
        values = re.sub(regex, "\n\n\n", innodb_status).split("\n\n\n\n")[1:]

        i=0
        for key in keys:
            _dict[key] = values[i].lstrip().rstrip()
            i += 1
        return _dict

    @staticmethod
    def parse_semaphores(text):
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
        _dict = {}
        regex = (
            r"OS WAIT ARRAY INFO: reservation count (\d+), signal count (\d+)\n"
            r"Mutex spin waits (\d+), rounds (\d+), OS waits (\d+)\n"
            r"RW-shared spins (\d+), rounds (\d+), OS waits (\d+)\n"
            r"RW-excl spins (\d+), rounds (\d+), OS waits (\d+)\n"
            r"Spin rounds per wait: (\d+\.\d+) mutex, (\d+\.\d+) RW-shared, (\d+\.\d+) RW-excl"
        )

        try:
            result = re.compile(regex).findall(text)[0]

            _dict['os_wait_reservation_count']      = result[0]
            _dict['os_wait_signal_count']           = result[1]
            _dict['mutex_spin_waits']               = result[2]
            _dict['mutex_rounds']                   = result[3]
            _dict['mutex_os_waits']                 = result[4]
            _dict['rw_shared_spins']                = result[5]
            _dict['rw_shared_rounds']               = result[6]
            _dict['rw_shared_os_waits']             = result[7]
            _dict['rw_excl_spins']                  = result[8]
            _dict['rw_excl_rounds']                 = result[9]
            _dict['rw_excl_os_waits']               = result[10]
            _dict['spin_rounds_per_wait_mutex']     = result[11]
            _dict['spin_rounds_per_wait_rw_shared'] = result[12]
            _dict['spin_rounds_per_wait_rw_excl']   = result[13]
        except:
            pass

        return _dict

    @staticmethod
    def parse_background_thread(text):
        """
        -----------------
        BACKGROUND THREAD
        -----------------
        srv_master_thread loops: 849820 1_second, 849819 sleeps, 84080 10_second, 9293 background, 9293 flush
        srv_master_thread log flush and writes: 851861
        """
        _dict = {}
        regex = (
            r"srv_master_thread loops: (\d+) 1_second, (\d+) sleeps, (\d+) 10_second, (\d+) background, (\d+) flush\n"
            r"srv_master_thread log flush and writes: (\d+)"
        )

        try:
            result = re.compile(regex).findall(text)[0]

            _dict['srv_master_thread_loops_1_second']       = result[0]
            _dict['srv_master_thread_loops_sleeps']         = result[1]
            _dict['srv_master_thread_loops_10_second']      = result[2]
            _dict['srv_master_thread_loops_background']     = result[3]
            _dict['srv_master_thread_loops_flush']          = result[4]
            _dict['srv_master_thread_log_flush_and_writes'] = result[5]

        except:
            pass

        return _dict

    @staticmethod
    def parse_transanctions(text):
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
        _dict = {}
        regex = (
            r"Trx id counter ([0-9A-F]+)\n"
            r"Purge done for trx's n:o < ([0-9A-F]+) undo n:o < ([0-9A-F]+)\n"
            r"History list length (\d+)"
        )

        try:
            result = re.compile(regex).findall(text)[0]

            _dict['trx_id_counter']       = result[0]
            _dict['purge_done_for_trx']   = result[1]
            _dict['purge_done_for_undo']  = result[2]
            _dict['history_list_length']  = result[3]

        except:
            pass

        return _dict

    @staticmethod
    def parse_file_io(text):
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
        _dict = {}
        regex = (
            r"Pending normal aio reads: (\d+) \[.*\] , aio writes: (\d+) \[.*\] ,\n"
            r" ibuf aio reads: (\d+), log i/o's: (\d+), sync i/o's: (\d+)\n"
            r"Pending flushes \(fsync\) log: (\d+); buffer pool: (\d+)\n"
            r"(\d+) OS file reads, (\d+) OS file writes, (\d+) OS fsyncs\n"
            r"(\d+\.\d+) reads/s, (\d+) avg bytes/read, (\d+\.\d+) writes/s, (\d+\.\d+) fsyncs/s"
        )

        try:
            result = re.compile(regex).findall(text)[0]

            _dict['pending_normal_aio_reads']      = result[0]
            _dict['pending_normal_aio_writes']     = result[1]
            _dict['pending_normal_ibuf_aio_reads'] = result[2]
            _dict['pending_normal_log_io']         = result[3]
            _dict['pending_normal_sync_io']        = result[4]
            _dict['pending_flush_log']             = result[5]
            _dict['pending_flush_buffer_pool']     = result[6]
            _dict['os_file_reads']                 = result[7]
            _dict['os_file_writes']                = result[8]
            _dict['os_file_fsyncs']                = result[9]
            _dict['reads_per_sec']                 = result[10]
            _dict['avg_bytes_per_read']            = result[11]
            _dict['writes_per_sec']                = result[12]
            _dict['fsyncs_per_sec']                = result[13]

        except:
            pass

        return _dict

    @staticmethod
    def parse_ibuf_hi(text):
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
        _dict = {}
        regex = (
            r"Ibuf: size (\d+), free list len (\d+), seg size (\d+), (\d+) merges\n"
            r"merged operations:\n"
            r" insert (\d+), delete mark (\d+), delete (\d+)\n"
            r"discarded operations:\n"
            r" insert (\d+), delete mark (\d+), delete (\d+)\n"
            r"Hash table size (\d+), node heap has (\d+) buffer\(s\)\n"
            r"(\d+\.\d+) hash searches/s, (\d+\.\d+) non-hash searches/s"
        )
        try:
            result = re.compile(regex).findall(text)[0]

            _dict['ibuf_size']                  = result[0]
            _dict['ibuf_free_list_len']         = result[1]
            _dict['ibuf_seg_size']              = result[2]
            _dict['ibuf_merges']                = result[3]
            _dict['ibuf_merged_insert']         = result[4]
            _dict['ibuf_merged_delete_mark']    = result[5]
            _dict['ibuf_merged_delete']         = result[6]
            _dict['ibuf_discarded_insert']      = result[7]
            _dict['ibuf_discarded_delete_mark'] = result[8]
            _dict['ibuf_discarded_delete']      = result[9]
            _dict['hash_table_size']            = result[10]
            _dict['node_heap_buffers']          = result[11]
            _dict['hash_searches_per_sec']      = result[12]
            _dict['non_hash_searches_per_sec']  = result[13]

        except:
            pass

        return _dict

    @staticmethod
    def parse_log(text):
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
        _dict = {}
        regex = (
            r"Log sequence number\s+(\d+)\n"
            r"Log flushed up to\s+(\d+)\n"
            r"Last checkpoint at\s+(\d+)\n"
            r"(\d+) pending log writes, (\d+) pending chkp writes\n"
            r"(\d+) log i/o's done, (\d+\.\d+) log i/o's/second"
        )

        try:
            result = re.compile(regex).findall(text)[0]

            _dict['log_sequence_number'] = result[0]
            _dict['log_flushed_up_to']   = result[1]
            _dict['last_checkpoint_at']  = result[2]
            _dict['pending_log_writes']  = result[3]
            _dict['pending_chkp_writes'] = result[4]
            _dict['log_io_done']         = result[5]
            _dict['log_io_per_sec']      = result[6]

        except:
            pass

        return _dict

    @staticmethod
    def parse_bp_and_mem(text):
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
        _dict = {}
        regex = (
            r"Total memory allocated (\d+); in additional pool allocated (\d+)\n"
            r"Dictionary memory allocated (\d+)\n"
            r"Buffer pool size\s+(\d+)\n"
            r"Free buffers\s+(\d+)\n"
            r"Database pages\s+(\d+)\n"
            r"Old database pages\s+(\d+)\n"
            r"Modified db pages\s+(\d+)\n"
            r"Pending reads (\d+)\n"
            r"Pending writes: LRU (\d+), flush list (\d+), single page (\d+)\n"
            r"Pages made young (\d+), not young (\d+)\n"
            r"(\d+\.\d+) youngs/s, (\d+\.\d+) non-youngs/s\n"
            r"Pages read (\d+), created (\d+), written (\d+)\n"
            r"(\d+\.\d+) reads/s, (\d+\.\d+) creates/s, (\d+\.\d+) writes/s\n"
            r"Buffer pool hit rate (\d+) / (\d+), young-making rate (\d+) / (\d+) not (\d+) / (\d+)\n"
            r"Pages read ahead (\d+\.\d+)/s, evicted without access (\d+\.\d+)/s, Random read ahead (\d+\.\d+)/s\n"
            r"LRU len: (\d+), unzip_LRU len: (\d+)\n"
            r"I/O sum\[(\d+)\]:cur\[(\d+)\], unzip sum\[(\d+)\]:cur\[(\d+)\]"
        )

        try:
            result = re.compile(regex).findall(text)[0]

            _dict['total_memory_allocated']               = result[0]
            _dict['additional_pool_allocated']            = result[1]
            _dict['dictionary_memory_allocated']          = result[2]
            _dict['buffer_pool_size']                     = result[3]
            _dict['free_buffers']                         = result[4]
            _dict['database_pages']                       = result[5]
            _dict['old_database_pages']                   = result[6]
            _dict['modified_database_pages']              = result[7]
            _dict['pending_reads']                        = result[8]
            _dict['pending_writes_lru']                   = result[9]
            _dict['pending_writes_flush_list']            = result[10]
            _dict['pending_writes_single_page']           = result[11]
            _dict['pages_made_young']                     = result[12]
            _dict['pages_made_not_young']                 = result[13]
            _dict['pages_made_youngs_per_sec']            = result[14]
            _dict['pages_made_non_youngs_per_sec']        = result[15]
            _dict['pages_read']                           = result[16]
            _dict['pages_created']                        = result[17]
            _dict['pages_written']                        = result[18]
            _dict['pages_reads_per_sec']                  = result[19]
            _dict['pages_creates_per_sec']                = result[20]
            _dict['pages_writes_per_sec']                 = result[21]
            _dict['buffer_pool_hitrate_numerator']        = result[22]
            _dict['buffer_pool_hitrate_denominator']      = result[23]
            _dict['young_making_rate_numerator']          = result[24]
            _dict['young_making_rate_denominator']        = result[25]
            _dict['young_making_rate_not_numerator']      = result[26]
            _dict['young_making_rate_not_denominator']    = result[27]
            _dict['pages_read_ahead_per_sec']             = result[28]
            _dict['pages_evicted_without_access_per_sec'] = result[29]
            _dict['random_read_ahead_per_sec']            = result[30]
            _dict['lru_len']                              = result[31]
            _dict['unzip_lru_len']                        = result[32]
            _dict['io_sum']                               = result[33]
            _dict['io_sum_cur']                           = result[34]
            _dict['unzip_sum']                            = result[35]
            _dict['unzip_sum_cur']                        = result[36]

        except:
            pass

        return _dict

    @staticmethod
    def parse_row_operations(text):
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
        _dict = {}
        regex = (
            r"(\d+) queries inside InnoDB, (\d+) queries in queue\n"
            r"(\d+) read views open inside InnoDB\n"
            r"Main thread process no. (\d+), id (\d+), state: (\S+)\n"
            r"Number of rows inserted (\d+), updated (\d+), deleted (\d+), read (\d+)\n"
            r"(\d+\.\d+) inserts/s, (\d+\.\d+) updates/s, (\d+\.\d+) deletes/s, (\d+\.\d+) reads/s"
        )

        try:
            result = re.compile(regex).findall(text)[0]

            _dict['queries_inside']          = result[0]
            _dict['queries_in_queue']        = result[1]
            _dict['views_inside']            = result[2]
            _dict['main_thread_process_no']  = result[3]
            _dict['main_thread_id']          = result[4]
            _dict['main_thread_state']       = result[5]
            _dict['number_of_rows_inserted'] = result[6]
            _dict['number_of_rows_updated']  = result[7]
            _dict['number_of_rows_deleted']  = result[8]
            _dict['number_of_rows_read']     = result[9]
            _dict['inserted_per_sec']        = result[10]
            _dict['updates_per_sec']         = result[11]
            _dict['deletes_per_sec']         = result[12]
            _dict['reads_per_sec']           = result[13]

        except:
            pass

        return _dict

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
