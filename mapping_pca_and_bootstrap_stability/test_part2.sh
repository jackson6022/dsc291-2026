#!/bin/bash
# Test script for Part 2: Tail Analysis

set -e  # Exit on error

echo "=========================================="
echo "Testing Part 2: Tail Analysis"
echo "=========================================="

# Change to script directory
cd "$(dirname "$0")"

# Check if pca_model.pkl exists
if [ ! -f "pca_model.pkl" ]; then
    echo "ERROR: pca_model.pkl not found!"
    exit 1
fi

echo "Found pca_model.pkl ✓"

# Run Part 2
echo -e "\nRunning tail analysis..."
../venv/bin/python pca_part2_tail_analysis.py --model pca_model.pkl --output-dir .

# Check outputs
echo -e "\nChecking outputs..."

if [ -f "coefficient_distribution.png" ]; then
    echo "✓ coefficient_distribution.png generated"
else
    echo "✗ coefficient_distribution.png NOT found"
    exit 1
fi

if [ -f "tail_analysis_report.json" ]; then
    echo "✓ tail_analysis_report.json generated"
    echo -e "\nReport contents:"
    cat tail_analysis_report.json | head -20
else
    echo "✗ tail_analysis_report.json NOT found"
    exit 1
fi

echo -e "\n=========================================="
echo "Part 2 test completed successfully! ✓"
echo "=========================================="
