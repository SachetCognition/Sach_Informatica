#!/bin/bash
# Helper script to run PySpark scripts with JAVA_HOME set correctly

# Set JAVA_HOME for macOS
if [[ "$OSTYPE" == "darwin"* ]]; then
    export JAVA_HOME=$(/usr/libexec/java_home -v 17 2>/dev/null)
    if [ -z "$JAVA_HOME" ]; then
        echo "❌ Java 17 not found. Installing..."
        brew install openjdk@17
        sudo ln -sfn /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-17.jdk
        export JAVA_HOME=$(/usr/libexec/java_home -v 17)
    fi
fi

# Verify Java is available
if ! command -v java &> /dev/null; then
    echo "❌ Java not found. Please install Java 11+ first."
    echo "   See SETUP.md for installation instructions."
    exit 1
fi

echo "✅ Using Java: $(java -version 2>&1 | head -n 1)"
echo "   JAVA_HOME: $JAVA_HOME"
echo ""

# Run the command passed as arguments
exec "$@"
