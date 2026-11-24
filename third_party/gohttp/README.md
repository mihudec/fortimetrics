# gohttp snapshot

This directory contains the HTTP client from the local `mihudec/gohttp` checkout used during Fortimetrics development. It includes working-tree changes beyond commit `2b378a4`. Fortimetrics builds against this snapshot so a clean checkout needs no access to the separate private repository.

To update, copy the client source and module files from the intended gohttp revision, then run `make check`.
