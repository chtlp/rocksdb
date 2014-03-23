import sys, argparse
from fabric.api import local

def get_parser():
    parser = argparse.ArgumentParser("rocksdb cache benchmark")

    parser.add_argument("--rocksdb_cache_test", type=int, default=int(1e8))
    parser.add_argument("--trace_file", default="trace.tsv")
    parser.add_argument("--num_segments", type=int, default=4)
    parser.add_argument("--queue_length", type=int, default=1000)

    parser.add_argument("--db", default="data/fa/tmp/rocksdb_cache_test")
    parser.add_argument("--verify_checksum", type=int, default=1)
    # maximum number of files to keep open at the same time.
    parser.add_argument("--open_files", type=int, default=500000)
    # don't report stats from rocksdb
    parser.add_argument("--stats_interval", type=int, default=-1)
    parser.add_argument("--block_size", type=int, default=4096)
    parser.add_argument("--cache_size", type=int, default=1048576*16)
    parser.add_argument("--sync", type=int, default=0)
    # TODO If true, do not wait until data is synced to disk
    parser.add_argument("--disable_data_sync", type=int, default=0)
    parser.add_argument("--target_file_size_base", type=int, default=67108864)
    # TODO The number of in-memory memtables. Each memtable is of size
    parser.add_argument("--write_buffer_size", type=int, default=134217728)
    parser.add_argument("--write_buffer_number", type=int, default=3)
    # TODO tweak this!
    parser.add_argument("--max_background_compactions", type=int, default=20)
    # universal compaction
    parser.add_argument("--compaction_style", type=int, default=1)
    # space amplification for universal compaction
    parser.add_argument("--universal_space_amplification", type=int, default=100)

    return parser

if __name__ == '__main___':
    parser = get_parser()
    print sys.argv
    sys.exit(0)
    args = parser.parse_args(sys.argv[2:])

    cmd = '''
./db_bench --benchmarks=cache --disable_seek_compaction=1 --mmap_read=0
--statistics=1 --histogram=1 --stats_interval={stats_interval}
--block_size={block_size} --cache_size={cache_size} --bloom_bits=10
--cache_numshardbits=4
--open_files={open_files} --verify_checksum={verify_checksum} --db={db}
--sync={sync} --disable_wal=1
 --disable_data_sync={disable_data_sync}
--write_buffer_size={write_buffer_size}
--target_file_size_base={target_file_size_base}
--max_write_buffer_number={write_buffer_number}
--max_background_compactions=${max_background_compactions} --use_existing_db=0
-compression_type none
--compaction_style 1 --universal_max_size_amplification_percent 2000
'''.strip()
    cmd = ' '.join(cmd.split('\n'))
    local(cmd)
