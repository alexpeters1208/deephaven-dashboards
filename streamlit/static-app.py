import streamlit as st
from streamlit_deephaven import start_server, display_dh

# Start the Deephaven server. You must start the server before running any Deephaven operations.
s = start_server(port=10000, jvm_args=["-Xmx4g",
                                       "-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler",
                                       "-Dprocess.info.system-info.enabled=false"])

st.subheader("Streamlit Deephaven")

# Create a simple table.
from deephaven import empty_table
from deephaven.plot.figure import Figure

t = empty_table(10).update(["x=i", "y=x * x"])

plot = Figure().plot_xy(series_name="Series", t=t, x="x", y="y").show()

# Display the table.
display_dh(t)
display_dh(plot)