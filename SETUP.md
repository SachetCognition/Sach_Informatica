# Setup Guide

## Prerequisites

### 1. Python 3.9+
Already installed if you're using UV.

### 2. Java 11 or 17 (Required for PySpark)

PySpark requires Java to run. Install using one of these methods:

**macOS (Homebrew):**
```bash
brew install openjdk@17
```

**Ubuntu/Debian:**
```bash
sudo apt-get update
sudo apt-get install openjdk-17-jdk
```

**Verify Java installation:**
```bash
java -version
```

You should see output like:
```
openjdk version "17.0.x" ...
```

### 3. Set JAVA_HOME (if needed)

**macOS (add to ~/.zshrc or ~/.bash_profile):**
```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
```

**Linux (add to ~/.bashrc):**
```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
```

## Installation

### 1. Clone the Repository
```bash
git clone <your-repo-url>
cd private-informatica-demo
```

### 2. Install Dependencies with UV
```bash
# UV will automatically create a virtual environment and install dependencies
uv sync
```

This installs:
- PySpark 3.5+
- All project dependencies

### 3. Verify Installation
```bash
uv run python -c "import pyspark; print(f'PySpark {pyspark.__version__} installed!')"
```

## Quick Start

### 1. Parse Informatica XML
```bash
uv run python infa_to_pyspark_parser.py "Informatica PC Samples/wf_m_PatternForChurnData.xml"
```

### 2. Generate PySpark Code
```bash
uv run python generate_pyspark_code.py "Informatica PC Samples/wf_m_PatternForChurnData.xml"
```

### 3. Generate Sample Data
```bash
uv run python generate_sample_data.py generated/metadata/wf_m_PatternForChurnData_metadata.json 100
```

### 4. Run the ETL
```bash
uv run python run_etl.py generated/pyspark/wf_m_PatternForChurnData_etl.py
```

## Project Structure

```
private-informatica-demo/
├── Informatica PC Samples/     # Sample Informatica XML files
├── generated/                   # Auto-generated files (gitignored)
│   ├── metadata/               # Parsed metadata JSON
│   ├── pyspark/                # Generated PySpark scripts
│   └── sample_data/            # Generated CSV files
├── infa_to_pyspark_parser.py   # Parser script
├── generate_pyspark_code.py    # Code generator
├── generate_sample_data.py     # Sample data generator
├── run_etl.py                  # ETL runner
├── pyproject.toml              # Project configuration
└── README_CONVERSION.md        # Documentation
```

## Troubleshooting

### Issue: "Unable to locate a Java Runtime"
**Solution:** Install Java (see Prerequisites above)

### Issue: "JAVA_HOME is not set"
**Solution:** Set JAVA_HOME environment variable (see Prerequisites above)

### Issue: "Module not found"
**Solution:** Make sure you're using `uv run` to execute scripts

### Issue: "No sample data found"
**Solution:** Generate sample data first:
```bash
uv run python generate_sample_data.py generated/metadata/<file>_metadata.json
```

## Development

### Install Dev Dependencies
```bash
uv sync --extra dev
```

### Run Tests (when available)
```bash
uv run pytest
```

### Format Code
```bash
uv run black *.py
```

## Environment Variables

Optional environment variables:

- `JAVA_HOME` - Path to Java installation
- `SPARK_HOME` - Path to Spark installation (optional, UV manages this)
- `PYSPARK_PYTHON` - Python executable for PySpark workers

## Next Steps

1. ✅ Install Java
2. ✅ Run `uv sync`
3. ✅ Parse an Informatica XML file
4. ✅ Generate PySpark code
5. ✅ Generate sample data
6. ✅ Run and test the ETL

For more details, see:
- `README_CONVERSION.md` - Conversion guide
- `QUICK_START.md` - Quick reference
- `SAMPLE_DATA_GUIDE.md` - Sample data generation
- `DIRECTORY_STRUCTURE.md` - Project organization
