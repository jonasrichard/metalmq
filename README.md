## Build and run

```bash
cargo install rabbiteer
RUST_LOG=info cargo run
echo "{\"panda\":true}" | rabbiteer -u admin -p admin -v prod publish -e myexchange -c application/json
```
