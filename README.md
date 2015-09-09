# java-stratum
Stratum / Electrum protocol and shell in Java

## CLI

    mvn package
    ./bin/statrumsh
    
    connect
    subscribe_address 1C6JdWXzAS8yCNfZNtK5uwSYQyZ5JL9LbC
    history 1C6JdWXzAS8yCNfZNtK5uwSYQyZ5JL9LbC

## Manual Testing

    openssl s_client -connect ecdsa.net:50002

    {"id": 0, "method": "server.version", "params": []}
    {"id": 1, "method": "blockchain.address.get_history", "params": ["1ByzGZo36hJQ8D74PME1btU8wnv1K7DXW3"]}
    {"id": 1, "method": "blockchain.address.get_balance", "params": ["1ByzGZo36hJQ8D74PME1btU8wnv1K7DXW3"]}

## Test address

    1C6JdWXzAS8yCNfZNtK5uwSYQyZ5JL9LbC
