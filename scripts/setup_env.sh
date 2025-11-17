#!/bin/bash
# Script to set up environment variables for this project
# Sets JAVA_HOME to Java 8 (compatible with Spark 3.5.0)

set -e

echo "========================================="
echo "Setting up environment for Cloud Provider Analytics"
echo "========================================="
echo ""

# Try to find Java 8 first (best compatibility)
JAVA_8_HOME=$(/usr/libexec/java_home -v 1.8 2>/dev/null || echo "")

if [ -n "$JAVA_8_HOME" ]; then
    echo "✓ Found Java 8: $JAVA_8_HOME"
    export JAVA_HOME="$JAVA_8_HOME"
    echo "✓ JAVA_HOME set to Java 8"
elif /usr/libexec/java_home -v 11 &>/dev/null; then
    JAVA_11_HOME=$(/usr/libexec/java_home -v 11)
    echo "✓ Found Java 11: $JAVA_11_HOME"
    export JAVA_HOME="$JAVA_11_HOME"
    echo "✓ JAVA_HOME set to Java 11"
elif /usr/libexec/java_home -v 17 &>/dev/null; then
    JAVA_17_HOME=$(/usr/libexec/java_home -v 17)
    echo "✓ Found Java 17: $JAVA_17_HOME"
    export JAVA_HOME="$JAVA_17_HOME"
    echo "✓ JAVA_HOME set to Java 17"
    # Still need HADOOP_OPTS for Java 17+
    export HADOOP_OPTS="--add-opens=java.base/javax.security.auth=ALL-UNNAMED --add-opens=java.base/java.security=ALL-UNNAMED"
    echo "✓ HADOOP_OPTS set for Java 17+ compatibility"
else
    echo "⚠ No compatible Java version (8, 11, or 17) found"
    echo "Current Java:"
    java -version 2>&1 | head -1
    echo ""
    echo "Please install Java 8, 11, or 17:"
    echo "  brew install openjdk@11"
    exit 1
fi

# Set HADOOP_OPTS for Java 17+ (not needed for Java 8/11, but harmless)
if [ -z "$HADOOP_OPTS" ]; then
    export HADOOP_OPTS="--add-opens=java.base/javax.security.auth=ALL-UNNAMED --add-opens=java.base/java.security=ALL-UNNAMED"
fi

# Update PATH to use the selected Java
export PATH="$JAVA_HOME/bin:$PATH"

echo ""
echo "========================================="
echo "Environment Setup Complete"
echo "========================================="
echo "JAVA_HOME: $JAVA_HOME"
echo ""
echo "Java version in use:"
"$JAVA_HOME/bin/java" -version 2>&1 | head -1
echo ""
echo "To use this environment, run:"
echo "  source scripts/setup_env.sh"
echo ""
echo "Or add to your ~/.zshrc for permanent setup:"
echo "  echo 'export JAVA_HOME=\$(/usr/libexec/java_home -v 1.8)' >> ~/.zshrc"

