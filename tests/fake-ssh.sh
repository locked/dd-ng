#!/usr/bin/env bash
# Fake ssh for local testing: `fake-ssh [opts...] host cmd args...`
# Understands `-o KEY=VAL` pairs and other single-value short opts.
args=("$@")
i=0
while [ $i -lt ${#args[@]} ]; do
  a="${args[$i]}"
  case "$a" in
    -o|-p|-i|-l|-F|-E|-c|-m|-b|-e|-R|-L|-D|-Q|-B|-J|-O|-S|-w)
      i=$((i+2)); continue;;
    -*) i=$((i+1)); continue;;
    *) break;;
  esac
done
# args[i] is host, drop it.
i=$((i+1))
exec "${args[@]:$i}"
