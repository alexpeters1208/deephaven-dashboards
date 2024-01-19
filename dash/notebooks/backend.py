
#### START DEEPHAVEN ####

from deephaven_server import Server
s = Server(port=10000, jvm_args=["-Xmx8g", "-Dprocess.info.system-info.enabled=false"])
s.start()

from deephaven.execution_context import get_exec_ctx
CTX = get_exec_ctx()

#### IMPORTS ####

# data imports
from deephaven import read_csv
from deephaven.replay import TableReplayer

# analysis imports

# plotting imports

# dashboard imports

#### INGEST DATA ####

historical = read_csv("https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/crypto_sept7.csv")
#historical2 = read_csv("https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/crypto_sept8.csv")

#replayer = TableReplayer("2021-09-08T04:00:00Z", "2021-09-09T05:00:00Z")
#streaming = replayer.add_table(historical2.sort("dateTime"), "dateTime").sort_descending("dateTime")
#replayer.start()

def get_tables():
    return CTX, historical