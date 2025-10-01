# Informatica to PySpark Conversion Toolkit

This repository provides tools to convert Informatica PowerCenter XML exports to PySpark ETL code, enabling migration from Informatica to Spark-based data processing.

## ✨ Features

- 🔍 **Parse** Informatica PowerCenter XML files
- 🔄 **Convert** to executable PySpark ETL code
- 📊 **Generate** realistic sample data for testing
- ✅ **Test** converted code with sample data
- 📦 **UV Project** with PySpark pre-configured

## 📦 Installation

```bash
# Install dependencies
uv sync

# Verify PySpark installation
uv run python -c "import pyspark; print('PySpark installed!')"
```

**Requirements:**
- Python 3.9+
- Java 11+ (for running PySpark)
- UV package manager

See **[SETUP.md](SETUP.md)** for detailed installation instructions.

## 🚀 Quick Start

```bash
# 1. Parse Informatica XML and extract metadata
uv run python infa_to_pyspark_parser.py "Informatica PC Samples/wf_m_PatternForChurnData.xml"

# 2. Generate PySpark code
uv run python generate_pyspark_code.py "Informatica PC Samples/wf_m_PatternForChurnData.xml"

# 3. Generate sample test data
uv run python generate_sample_data.py generated/metadata/wf_m_PatternForChurnData_metadata.json 50

# 4. Run ETL and generate outputs (no Java required!)
uv run python test_without_java.py

# Output files created in generated/outputs/
```

## 📚 Documentation

- **[SETUP.md](SETUP.md)** - Installation and prerequisites
- **[Quick Start Guide](QUICK_START.md)** - Get started in minutes
- **[Conversion Guide](README_CONVERSION.md)** - Detailed conversion documentation
- **[Sample Data Guide](SAMPLE_DATA_GUIDE.md)** - Generate test data
- **[Directory Structure](DIRECTORY_STRUCTURE.md)** - File organization

## 🛠️ Tools Included

### `infa_to_pyspark_parser.py`
Parses Informatica PowerCenter XML files and extracts metadata including sources, targets, transformations, and data flows.

### `generate_pyspark_code.py`
Generates executable PySpark ETL scripts from parsed Informatica metadata with proper transformations and data handling.

### `generate_sample_data.py`
Creates realistic CSV sample data for testing your converted PySpark code without production data access.

### `run_etl.py`
Executes generated PySpark scripts with automatic path updates to use sample data.

### `test_etl_simple.py`
Simple test script that validates the conversion by running basic PySpark operations on sample data.

## 📁 Sample Files

The `Informatica PC Samples/` directory contains example Informatica XML files:
- `wf_m_PatternForChurnData.xml` - Customer churn analysis
- `wf_s_m_complex5.XML` - Complex multi-source ETL
- `wf_m_mapplet_multiple_unconnected.XML` - Unconnected lookups
- And more...

## 🎯 Project Structure

```
informatica-to-pyspark/
├── Informatica PC Samples/     # Sample XML files
├── generated/                   # Auto-generated (gitignored)
│   ├── metadata/               # Parsed JSON metadata
│   ├── pyspark/                # Generated PySpark scripts
│   └── sample_data/            # Generated CSV files
├── infa_to_pyspark_parser.py   # Parser
├── generate_pyspark_code.py    # Code generator
├── generate_sample_data.py     # Data generator
├── run_etl.py                  # ETL runner
├── test_etl_simple.py          # Simple test
├── pyproject.toml              # UV project config
└── README.md                   # This file
```

## 🎯 Next Steps

1. **[SETUP.md](SETUP.md)** - Install Java and dependencies
2. **[Quick Start Guide](QUICK_START.md)** - Run your first conversion
3. **[Conversion Guide](README_CONVERSION.md)** - Understand transformation mappings
4. **[Sample Data Guide](SAMPLE_DATA_GUIDE.md)** - Generate test data

## 📝 License

MIT License - Feel free to use and modify!
