#!/bin/bash
cd "${0%/*}"
python3 -m streamlit run streamlit_app.py --server.headless true