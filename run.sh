#!/bin/bash
mkdir -p ../data
export LOCAL_INGESTION_ENABLED=true
export GGML_CUDA_ENABLE_UNIFIED_MEMORY=1
# Model is loaded into system ram, do not load models larger than ram capacity! Swap not used!
# export PGPT_PROFILES=llama70b
make ingest ../data -- --watch
sleep 5
echo BBQ Sauce!
make run