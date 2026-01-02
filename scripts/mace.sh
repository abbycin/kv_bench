#!/usr/bin/env bash

if [ "$#" -ne 1 ]
then
    printf "\033[m$0 path\033[0m\n"
    exit 1
fi

pushd .
cd ..
cargo build --release 1>/dev/null 2> /dev/null

function samples() {
        export RUST_BACKTRACE=full
        kv_sz=(16 16 100 1024 1024 1024 16 10240)
        mode=(insert get mixed scan)
        # set -x
        db_root=$1

        cnt=100000
        for ((i = 1; i <= $(nproc); i *= 2))
        do
                for ((j = 0; j < ${#kv_sz[@]}; j += 2))
                do
                      for ((k = 0; k < ${#mode[@]}; k += 1))
                      do 
                            if [ "${mode[k]}" == "insert" ]
                            then
                                  ./target/release/kv_bench --path $db_root --threads $i --iterations $cnt --mode ${mode[k]} --key-size ${kv_sz[j]} --value-size ${kv_sz[j+1]} --random
                                  if test $? -ne 0
                                  then
                                          echo "${mode[k]} threads $i ksz ${kv_sz[j]} vsz ${kv_sz[j+1]} random fail"
                                          exit 1
                                  fi
                            fi
                            ./target/release/kv_bench --path $db_root --threads $i --iterations $cnt --mode ${mode[k]} --key-size ${kv_sz[j]} --value-size ${kv_sz[j+1]}
                            if test $? -ne 0
                            then
                                    echo "${mode[k]} threads $i ksz ${kv_sz[j]} vsz ${kv_sz[j+1]} fail"
                                    exit 1
                            fi
                      done
                done
        done
}

echo mode,threads,key_size,value_size,insert_ratio,ops,elasped > scripts/mace.csv
samples $1 2>> scripts/mace.csv
popd
./bin/python plot.py mace.csv
