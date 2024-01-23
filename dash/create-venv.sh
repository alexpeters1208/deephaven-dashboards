#!/bin/bash

# Create and activate virtual environment
python -m venv dh-dash-venv
source dh-dash-venv/bin/activate

# Install dash and relevant deephaven packages
pip install dash dash-bootstrap-components plotly-express deephaven-server deephaven-ipywidgets

# Clone the web-plugin-packager repository
git clone https://github.com/deephaven/web-plugin-packager.git
cd web-plugin-packager
git checkout latest

# Pack JavaScript plugins
./pack-plugins.sh @deephaven/js-plugin-plotly

# Install Python packages
pip install --no-cache-dir deephaven-plugin-plotly

# Remove the cloned GitHub repository
cd ..
rm -rf web-plugin-packager