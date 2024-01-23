
#### START DEEPHAVEN ####

from deephaven_server import Server
s = Server(port=10000, jvm_args=["-Xmx8g", "-Dprocess.info.system-info.enabled=false",  "-Dauthentication.psk=12345"])
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

data = read_csv(
    "https://media.githubusercontent.com/media/deephaven/examples/main/CryptoCurrencyHistory/CSV/CryptoTrades_20210922.csv",
    num_rows=250000
)

historical = data.where("minuteOfDay(Timestamp, 'ET') < 60*12 + 30")
historical2 = data.where("minuteOfDay(Timestamp, 'ET') >= 60*12 + 30")

replayer = TableReplayer("2021-09-22T12:30:00.000 ET", "2021-09-22T13:01:48.054 ET")
streaming = replayer.add_table(historical2.sort("Timestamp"), "Timestamp").sort_descending("Timestamp")
replayer.start()

def get_tables():
    return CTX, historical, streaming