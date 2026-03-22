#!/bin/bash
# create raw zone folders
# create bronze/silver/gold zones
# create log directory

STORAGE_ACCOUNT="mktintelligencedl"
CONTAINER="lakehouse"

az storage fs directory create -n raw/alpha_vantage/daily_prices \
    --file-system $CONTAINER --account-name $STORAGE_ACCOUNT --auth-mode login

az storage fs directory create -n bronze/daily_prices \
    --file-system $CONTAINER --account-name $STORAGE_ACCOUNT --auth-mode login

az storage fs directory create -n silver/stg_daily_prices \
    --file-system $CONTAINER --account-name $STORAGE_ACCOUNT --auth-mode login

az storage fs directory create -n silver/stg_tickers \
    --file-system $CONTAINER --account-name $STORAGE_ACCOUNT --auth-mode login

az storage fs directory create -n gold/fct_daily_prices \
    --file-system $CONTAINER --account-name $STORAGE_ACCOUNT --auth-mode login

az storage fs directory create -n gold/dim_ticker \
    --file-system $CONTAINER --account-name $STORAGE_ACCOUNT --auth-mode login

az storage fs directory create -n logs/pipeline_runs \
    --file-system $CONTAINER --account-name $STORAGE_ACCOUNT --auth-mode login