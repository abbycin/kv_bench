#!/usr/bin/env bash

pushd .
cd ../rocksdb
cmake --preset release 1>/dev/null 2>/dev/null
cmake --build --preset release 1>/dev/null 2>/dev/null

function samples() {
        kv_sz=(16 16 100 1024 1024 1024 16 10240)
        # set -x
        cnt=10000
        for ((i = 1; i <= $(nproc); i *= 2))
        do
                for ((j = 0; j < ${#kv_sz[@]}; j += 2))
                do
                        ./build/release/rocksdb_bench --path /home/abby/rocksdb_tmp --threads $i --iterations $cnt --mode insert --key-size ${kv_sz[j]} --value-size ${kv_sz[j+1]}
                        if test $? -ne 0
                        then
                                echo "insert threads $i ksz ${kv_sz[j]} vsz ${kv_sz[j+1]} fail"
                                exit 1
                        fi
                        ./build/release/rocksdb_bench --path /home/abby/rocksdb_tmp --threads $i --iterations $cnt --mode get --key-size ${kv_sz[j]} --value-size ${kv_sz[j+1]}
                        if test $? -ne 0
                        then
                                echo "get threads $i ksz ${kv_sz[j]} vsz ${kv_sz[j+1]} fail"
                                exit 1
                        fi
                        ./build/release/rocksdb_bench --path /home/abby/rocksdb_tmp --threads $i --iterations $cnt --mode mixed --key-size ${kv_sz[j]} --value-size ${kv_sz[j+1]} --insert-ratio 30
                        if test $? -ne 0
                        then
                                echo "mixed threads $i ksz ${kv_sz[j]} vsz ${kv_sz[j+1]} fail"
                                exit 1
                        fi
                done
        done
}

echo mode,threads,key_size,value_size,insert_ratio,ops,elapsed > ../scripts/rocksdb.csv
samples 1>> ../scripts/rocksdb.csv
popd
./bin/python plot.py rocksdb.csv
