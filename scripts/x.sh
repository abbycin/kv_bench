#!/usr/bin/env bash

pushd .
cd ..
cargo build --release 1>/dev/null 2> /dev/null

function samples() {
        kv_sz=(16 16 100 1024 1024 1024)
        # set -x

        for ((i = 1; i <= $(nproc); i *= 2))
        do
                for ((j = 0; j < ${#kv_sz[@]}; j += 2))
                do
                        ./target/release/kv_bench --path /home/abby/mace_bench --threads $i --iterations 100000 --mode insert --key-size ${kv_sz[j]} --value-size ${kv_sz[j+1]}
                        if test $? -ne 0
                        then
                                echo "insert threads $i ksz ${kv_sz[j]} vsz ${kv_sz[j+1]} fail"
                                exit 1
                        fi
                        ./target/release/kv_bench --path /home/abby/mace_bench --threads $i --iterations 100000 --mode get --key-size ${kv_sz[j]} --value-size ${kv_sz[j+1]}
                        if test $? -ne 0
                        then
                                echo "insert threads $i ksz ${kv_sz[j]} vsz ${kv_sz[j+1]} fail"
                                exit 1
                        fi
                        ./target/release/kv_bench --path /home/abby/mace_bench --threads $i --iterations 100000 --mode mixed --key-size ${kv_sz[j]} --value-size ${kv_sz[j+1]} --insert-ratio 30
                        if test $? -ne 0
                        then
                                echo "insert threads $i ksz ${kv_sz[j]} vsz ${kv_sz[j+1]} fail"
                                exit 1
                        fi
                done
        done
}

echo mode,threads,key_size,value_size,insert_ratio,ops > scripts/x.csv
samples 2>> scripts/x.csv
popd
./bin/python plot.py
