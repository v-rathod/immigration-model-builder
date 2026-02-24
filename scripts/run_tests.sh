#!/bin/bash
# Convenience script to run tests

set -e

echo "Installing dependencies..."
pip install -r requirements.txt

echo ""
echo "Running tests..."
pytest -q
