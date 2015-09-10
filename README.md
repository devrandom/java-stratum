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
    {"id": 1, "method": "blockchain.address.get_history", "params": ["1C6JdWXzAS8yCNfZNtK5uwSYQyZ5JL9LbC"]}
    {"id": 1, "method": "blockchain.address.listunspent", "params": ["1C6JdWXzAS8yCNfZNtK5uwSYQyZ5JL9LbC"]}
    {"id": 1, "method": "blockchain.address.get_balance", "params": ["1C6JdWXzAS8yCNfZNtK5uwSYQyZ5JL9LbC"]}

## Test address

    1C6JdWXzAS8yCNfZNtK5uwSYQyZ5JL9LbC
