# app/consumers/__init__.py
# Marks the consumers folder as a package.
# Each consumer runs as its own independent process
# (separate Docker container, different Kafka topic/group).
# They are not imported by each other or by the pipeline —
# they each run standalone via their own entry point.
#
# app-consumer-hot:         python -m app.consumers.hot
# app-consumer-cold:        python -m app.consumers.cold
# app-consumer-deadletter:  python -m app.consumers.deadletter