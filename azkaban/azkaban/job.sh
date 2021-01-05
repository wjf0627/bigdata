#!/usr/bin/env bash

id=$1

if [[ $id -eq 1 ]]; then
  sleep 60
  exit 255
fi

sleep 180

echo "id ===>>> $id"

if [[ $? -ne 0 ]]; then
  exit 255
fi
