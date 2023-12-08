#!/bin/bash

# Create and activate virtual environment
python -m venv dh-shiny-venv
source dh-shiny-venv/bin/activate

# Install streamlit and dh streamlit plugin
pip install shiny shinyswatch deephaven-ipywidgets jupyter

# Clone the web-plugin-packager repository
git clone https://github.com/deephaven/web-plugin-packager.git
cd web-plugin-packager
git checkout latest

# Pack JavaScript plugins
./pack-plugins.sh @deephaven/js-plugin-plotly @deephaven/js-plugin-matplotlib

# Install Python packages
pip install --no-cache-dir deephaven-plugin-plotly deephaven-plugin-matplotlib

# Remove the cloned GitHub repository
cd ..
rm -rf web-plugin-packager
